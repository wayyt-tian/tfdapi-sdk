// Package tfdclient is the one HTTP client every caller of tfdapi should use.
//
// WHY IT EXISTS. The gateway used to absorb 千川's rate-limit rejects itself
// (a three-rung ladder of its own, plus a 5s per-endpoint freeze), which meant
// one caller's 40100 failed every OTHER caller of that interface locally. The
// retry was moved to the caller (QIANCHUAN-RETRY-SDK-AND-QPS-PLAN.md §1): the
// gateway now answers a stable, uniform 429 and this package decides whether
// the same logical request is worth re-sending.
//
// WHAT MAKES THAT SAFE is §1.2's unified contract: one code (RATE_LIMITED) and
// a Retry-After that is seconds, not a minute. Without both, "retry every 429"
// would either take a minute per rung or ignore the server's own advice.
//
// WHAT IT NEVER DOES. It does not retry POST/PATCH/PUT by default (the upstream
// may already have applied the write — the same argument the gateway's own
// wireRateLimitRetryBudget makes), and it does not retry 5xx or any other 4xx.
package tfdclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTimeout       = 30 * time.Second
	maxErrorBodyReadSize = 1 << 20
	maxSuccessBodySize   = 64 << 20

	// HeaderTrafficClass is the request header tfdapi reads to tell a
	// user-facing read from a background sweep. Byte-identical to the 投放岛's
	// internal contract (its traffic_class.go compares with ==).
	HeaderTrafficClass = "X-TFD-Traffic"
	// TrafficClassBackground is the value a background sweep declares.
	TrafficClassBackground = "background"
)

// Client is a retrying tfdapi HTTP client. It is safe for concurrent use.
type Client struct {
	baseURL *url.URL
	token   string
	http    *http.Client

	ladder    []time.Duration
	maxRetry  int
	jitter    func(time.Duration) time.Duration
	sleep     func(context.Context, time.Duration) error
	timeout   time.Duration
	userAgent string
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient installs the underlying transport (proxies, tracing, TLS
// settings). The timeout on it is NOT the SDK's budget — see WithTimeout.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.http = hc } }

// WithRetryBudget replaces the ladder. An EMPTY or nil ladder disables retries
// entirely (the background-sweep case: fail fast and let the next tick try).
func WithRetryBudget(ladder []time.Duration) Option {
	return func(c *Client) {
		if len(ladder) == 0 {
			c.ladder = nil
			c.maxRetry = 0
			return
		}
		c.ladder = append([]time.Duration(nil), ladder...)
		c.maxRetry = len(ladder)
	}
}

// WithTimeout bounds ONE attempt's wall clock for the requests this package
// builds itself (Get/PostJSON/Do). It is deliberately not folded into the retry
// budget: a slow success must not be retried just because the ladder's total
// budget is larger.
//
// Send does NOT read it. A Send caller already owns its *http.Request and its
// transport (that is the whole point of Send), so the per-attempt bound there
// is whatever its own *http.Client.Timeout says; accepting a second, invisible
// bound would mean the caller could not tell which one fired.
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithUserAgent overrides the default user agent.
func WithUserAgent(ua string) Option { return func(c *Client) { c.userAgent = ua } }

// WithJitter overrides the jitter source. Tests use it for determinism.
func WithJitter(fn func(time.Duration) time.Duration) Option {
	return func(c *Client) { c.jitter = fn }
}

// WithSleeper overrides how the client waits between attempts. Tests use it to
// avoid real sleeps.
func WithSleeper(fn func(context.Context, time.Duration) error) Option {
	return func(c *Client) { c.sleep = fn }
}

// New builds a client for one tfdapi base URL and API key.
func New(baseURL, apiKey string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("tfdclient: parse base url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("tfdclient: base url %q must be absolute", baseURL)
	}
	c := &Client{
		baseURL:   u,
		token:     apiKey,
		http:      &http.Client{Timeout: defaultTimeout},
		ladder:    append([]time.Duration(nil), RetryBackoff...),
		timeout:   defaultTimeout,
		userAgent: "tfdapi-sdk/1",
		jitter: func(base time.Duration) time.Duration {
			return time.Duration(rand.Float64() * JitterFraction * float64(base))
		},
		sleep: sleepCtx,
	}
	c.maxRetry = len(c.ladder)
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Response is a successful (2xx) tfdapi response.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Get issues a GET and decodes a JSON body into out when out is non-nil.
func (c *Client) Get(ctx context.Context, path string, query url.Values, out any) (*Response, error) {
	return c.do(ctx, http.MethodGet, path, query, nil, out, false)
}

// PostJSON issues a POST with a JSON body. A POST is NEVER retried unless the
// caller opts in with allowRetry — see WithIdempotencyKey.
func (c *Client) PostJSON(ctx context.Context, path string, body any, out any) (*Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("tfdclient: encode body: %w", err)
	}
	return c.do(ctx, http.MethodPost, path, nil, bytes.NewReader(payload), out, false)
}

// Do is the escape hatch for methods this package has no typed helper for.
// allowRetry must be true only for an idempotent request (the caller is
// asserting it is safe to apply twice).
func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body []byte, out any, allowRetry bool) (*Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	return c.do(ctx, method, path, query, reader, out, allowRetry)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body io.Reader, out any, allowRetry bool) (*Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Replayable body: the retry loop needs a fresh reader per attempt.
	var bodyBytes []byte
	if body != nil {
		b, err := io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("tfdclient: buffer request body: %w", err)
		}
		bodyBytes = b
	}
	idempotent := allowRetry || isIdempotentMethod(method)
	budget := c.maxRetry
	if !idempotent {
		budget = 0
	}

	for attempt := 0; ; attempt++ {
		resp, err := c.attempt(ctx, method, path, query, bodyBytes, out)
		if err == nil {
			return resp, nil
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || !apiErr.RetryableWithinLadder() {
			return nil, err
		}
		if attempt >= budget {
			return nil, err
		}
		wait, ok := retryWait(c.ladder, attempt, apiErr.RetryAfter, c.jitter)
		if !ok {
			return nil, err
		}
		if err := c.sleep(ctx, wait); err != nil {
			// The caller gave up while we were parked. Return THEIR error, not
			// the last 429 — the request was abandoned, not rate-limited.
			return nil, err
		}
	}
}

func isIdempotentMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func (c *Client) attempt(ctx context.Context, method, path string, query url.Values, body []byte, out any) (*Response, error) {
	attemptCtx := ctx
	if c.timeout > 0 {
		var cancel context.CancelFunc
		attemptCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	requestURL := *c.baseURL
	requestURL.Path = strings.TrimRight(requestURL.Path, "/") + path
	if query != nil {
		requestURL.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(attemptCtx, method, requestURL.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("tfdclient: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// Skipped when empty so an undeclared caller sends no header rather than an
	// empty value tfdapi would have to interpret.
	if class := BackgroundTrafficFromContext(ctx); class != "" {
		req.Header.Set(HeaderTrafficClass, class)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tfdclient: execute request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyReadSize))
		if readErr != nil {
			return nil, fmt.Errorf("tfdclient: read error body: %w", readErr)
		}
		return nil, newAPIError(resp.StatusCode, resp.Status, resp.Header, raw)
	}
	// parsed == nil means the caller wants only the status and headers.
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return &Response{StatusCode: resp.StatusCode, Header: resp.Header}, nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxSuccessBodySize))
	if err != nil {
		return nil, fmt.Errorf("tfdclient: read response body: %w", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return nil, fmt.Errorf("tfdclient: decode response: %w", err)
	}
	return &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: raw}, nil
}

// The traffic class rides on the CONTEXT, not the client: one process — and
// usually one *Client — serves both a background sweep and a user's read, so a
// client-level flag would mislabel whichever of the two it was not set for.
type trafficClassKey struct{}

// WithBackgroundTraffic declares that nobody is waiting on this call, so tfdapi
// may put it behind user-facing work. It is a hint, never a permission.
func WithBackgroundTraffic(ctx context.Context) context.Context {
	return context.WithValue(ctx, trafficClassKey{}, TrafficClassBackground)
}

// BackgroundTrafficFromContext reports the declared traffic class.
func BackgroundTrafficFromContext(ctx context.Context) string {
	v, _ := ctx.Value(trafficClassKey{}).(string)
	return v
}

// randFloat64 is the jitter source's random draw, named so both constructors
// (New and NewRetrier) share one definition of "how much extra".
func randFloat64() float64 { return rand.Float64() }
