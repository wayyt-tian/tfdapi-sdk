package tfdclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// This file is the integration point for an EXISTING client that already owns
// its request shape, its error type and its response decoding — which is every
// island client today (tfd-idea 1000+ lines, tfd-core, cloudagents).
//
// The alternative was to make each island rebuild itself on Client.Get/PostJSON.
// That is the long-run shape (S8's "删各自 client"), but it is a rewrite of
// every typed method in three repos, and the thing the gateway actually needs
// for S6 is only that the LADDER has moved: the caller, not the gateway, decides
// whether a 429 is worth re-sending. Send gives exactly that, in one call:
//
//	response, err := retrier.Send(ctx, request)   // retries per policy
//
// and the island keeps everything else.

// NewRetrier builds a Client that is usable only for Send. It has no base URL
// and no token: the caller's own *http.Request carries those, and Send never
// rewrites a request it did not build.
func NewRetrier(opts ...Option) *Client {
	c := &Client{
		http:      &http.Client{Timeout: defaultTimeout},
		ladder:    append([]time.Duration(nil), RetryBackoff...),
		timeout:   defaultTimeout,
		userAgent: "tfdapi-sdk/1",
		jitter: func(base time.Duration) time.Duration {
			return time.Duration(randFloat64() * JitterFraction * float64(base))
		},
		sleep: sleepCtx,
	}
	c.maxRetry = len(c.ladder)
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Send executes req, re-sending it per the retry policy, and returns the FINAL
// response for the caller to read and decode — 2xx or not. An error means no
// response could be obtained at all: a transport failure, or the context ending
// while parked between attempts.
//
// Contract details the caller can rely on:
//
//   - The response body is always readable exactly once. Bodies of attempts
//     that were re-sent are drained and closed here, never handed back.
//   - Retries happen only for a 429 whose body carries a code in
//     RetryableCodes AND whose Retry-After is short enough to absorb inside one
//     request (see APIError.RetryableWithinLadder), and only for an idempotent
//     method unless req has been marked with AllowRetry.
//   - req's method, URL and headers are never rewritten, and each attempt gets
//     its own clone. Its body — if it has one — is buffered on the first call
//     and replaced with an equivalent reader, so the caller may reuse req
//     afterwards but must not read its body concurrently with Send.
//
// Send deliberately does NOT convert a 4xx/5xx into an error. The caller owns
// its error type (the islands build *APIError with body snippets, categories
// and redaction); Send owning that too would mean re-deriving it in three
// places, which is the drift this SDK exists to prevent.
func (c *Client) Send(ctx context.Context, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("tfdclient: Send needs a request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := drainBody(req)
	if err != nil {
		return nil, err
	}
	budget := c.maxRetry
	if RetryDisabled(ctx) {
		// The caller has bought a better retry downstream (see retrymark.go):
		// one attempt, whatever comes back.
		budget = 0
	} else if !isIdempotentMethod(req.Method) && !allowRetry(req.Context()) {
		budget = 0
	}
	for attempt := 0; ; attempt++ {
		next, err := c.sendOnce(ctx, req, body)
		if err != nil {
			return nil, err
		}
		apiErr, raw, retryable := classifyForRetry(next)
		if !retryable || attempt >= budget {
			if raw != nil {
				next.Body = io.NopCloser(bytes.NewReader(raw))
			}
			return next, nil
		}
		// Park and try again. The wait is decided from the server's hint when
		// it gave one, the ladder otherwise, always clamped and always
		// jittered (see retryWait).
		wait, ok := retryWait(c.ladder, attempt, apiErr.RetryAfter, c.jitter)
		_ = next.Body.Close()
		if !ok {
			return nil, fmt.Errorf("tfdclient: no retry rung left for %s %s", req.Method, req.URL.Path)
		}
		if err := c.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
}

func (c *Client) sendOnce(ctx context.Context, req *http.Request, body []byte) (*http.Response, error) {
	clone := req.Clone(ctx)
	// The traffic class rides on the CONTEXT (see WithBackgroundTraffic). Send
	// replays a request the caller built, so it has to carry that declaration
	// onto the wire itself — the self-built path in client.go does the same. An
	// explicit header on the request wins: the caller may know better than the
	// context it was handed.
	if clone.Header.Get(HeaderTrafficClass) == "" {
		if class := BackgroundTrafficFromContext(ctx); class != "" {
			clone.Header.Set(HeaderTrafficClass, class)
		}
	}
	if body != nil {
		clone.Body = io.NopCloser(bytes.NewReader(body))
		clone.ContentLength = int64(len(body))
		clone.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}
	resp, err := c.http.Do(clone)
	if resp == nil && err == nil {
		return nil, fmt.Errorf("tfdclient: %s %s returned no response and no error", req.Method, req.URL)
	}
	if err != nil {
		return nil, fmt.Errorf("tfdclient: execute request: %w", err)
	}
	return resp, nil
}

// classifyForRetry reports whether resp is a retryable rate limit, and returns
// the buffered error body so the caller can still read it when it is not.
// Reading the body here is unavoidable: the code that decides retryability is
// in the body, not in the status.
func classifyForRetry(resp *http.Response) (*APIError, []byte, bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return nil, nil, false
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyReadSize))
	_ = resp.Body.Close()
	if err != nil {
		// An unreadable error body means we cannot see the code, which means we
		// cannot justify a retry. Hand the response back with an empty body
		// rather than a closed one, so the caller's own error path still works.
		return nil, []byte{}, false
	}
	apiErr := newAPIError(resp.StatusCode, resp.Status, resp.Header, raw)
	return apiErr, raw, apiErr.RetryableWithinLadder()
}

// drainBody buffers req.Body so it can be replayed, and reports nil when there
// was nothing to buffer.
func drainBody(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	raw, err := io.ReadAll(req.Body)
	closeErr := req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("tfdclient: buffer request body: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("tfdclient: close request body: %w", closeErr)
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, nil
}
