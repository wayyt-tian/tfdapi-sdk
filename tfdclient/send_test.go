package tfdclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func sendTestRetrier(t *testing.T, opts ...Option) (*Client, *[]time.Duration) {
	t.Helper()
	slept := &[]time.Duration{}
	base := []Option{
		WithJitter(func(time.Duration) time.Duration { return 0 }),
		WithSleeper(func(ctx context.Context, d time.Duration) error {
			*slept = append(*slept, d)
			return ctx.Err()
		}),
	}
	return NewRetrier(append(base, opts...)...), slept
}

func getReq(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}

func TestSendRetriesThenReturnsTheFinalResponse(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"code":"RATE_LIMITED","message":"push back"}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c, slept := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want the retried 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"ok":true`) {
		t.Fatalf("caller must get the final body, got %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("want 2 attempts, got %d", calls.Load())
	}
	if len(*slept) != 1 || (*slept)[0] != time.Second {
		t.Fatalf("want one 1s wait, got %v", *slept)
	}
}

// The exhausted case must still hand the caller a READABLE error body: the
// islands build their *APIError (snippet, code, category) from that body, and a
// closed reader would turn a clean 429 into a decode failure.
func TestSendExhaustedStillReturnsAReadableBody(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"RATE_LIMITED","message":"still pushed back"}`)
	}))
	defer srv.Close()

	c, _ := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send must return the last response, not an error: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("body must stay readable: %v", err)
	}
	if !strings.Contains(string(body), "still pushed back") {
		t.Fatalf("unexpected body %q", body)
	}
	if calls.Load() != 5 {
		t.Fatalf("want 1 initial + 4 rungs, got %d attempts", calls.Load())
	}
}

func TestSendDoesNotMutateTheCallersRequest(t *testing.T) {
	var got []string
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"code":"RATE_LIMITED"}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	req := getReq(t, srv.URL)
	req.Header.Set("Authorization", "Bearer island-key")
	originalBody := req.Body
	c, _ := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), req)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()
	if req.Body != originalBody {
		t.Fatalf("Send must not swap the caller's Body")
	}
	for i, auth := range got {
		if auth != "Bearer island-key" {
			t.Fatalf("attempt %d lost the caller's headers: %q", i, auth)
		}
	}
}

// A body-bearing request must be replayed intact: the islands POST once with a
// JSON body, and a retry that sent an empty body would be worse than no retry.
func TestSendReplaysTheBodyAndOnlyWhenAllowed(t *testing.T) {
	var bodies []string
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"code":"RATE_LIMITED"}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	payload := `{"advertiser_id":"777"}`
	makeReq := func(ctx context.Context) *http.Request {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, bytes.NewBufferString(payload))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		return req
	}
	c, slept := sendTestRetrier(t)

	// Default: a POST is not retried at all.
	resp, err := c.Send(context.Background(), makeReq(context.Background()))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp.Body.Close()
	if calls.Load() != 1 || len(*slept) != 0 {
		t.Fatalf("a POST must not be auto-retried; attempts=%d waits=%v", calls.Load(), *slept)
	}

	// Opted in: retried, with the body replayed byte for byte.
	calls.Store(0)
	bodies = nil
	*slept = nil
	resp, err = c.Send(context.Background(), makeReq(AllowRetry(context.Background())))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp.Body.Close()
	if calls.Load() != 2 {
		t.Fatalf("an AllowRetry POST must be retried; attempts=%d", calls.Load())
	}
	for i, b := range bodies {
		if b != payload {
			t.Fatalf("attempt %d body = %q, want %q", i, b, payload)
		}
	}
}

func TestSendHonoursRetryAfterAndContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "64")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"RATE_LIMITED"}`)
	}))
	defer srv.Close()

	c, slept := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp.Body.Close()
	for i, w := range *slept {
		if w != RetryAfterCap {
			t.Fatalf("wait %d = %v, want the clamp %v", i, w, RetryAfterCap)
		}
	}

	// Cancellation while parked must surface the context error, not the 429.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Send(ctx, getReq(t, srv.URL)); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestSendDoesNotRetryNonRateLimitFailuresOrTransports(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"code":"BOOM"}`)
	}))
	defer srv.Close()
	c, slept := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp.Body.Close()
	if calls.Load() != 1 || len(*slept) != 0 {
		t.Fatalf("5xx must not be retried; attempts=%d waits=%v", calls.Load(), *slept)
	}

	// Transport failure: no response, and no retry either.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	if _, err := c.Send(context.Background(), getReq(t, dead.URL)); err == nil {
		t.Fatalf("want a transport error")
	}
}

func TestSendRejectsNilRequest(t *testing.T) {
	c := NewRetrier()
	if _, err := c.Send(context.Background(), nil); err == nil {
		t.Fatalf("want an error for a nil request")
	}
}

// A request that already bought a bounded retry further down (the gateway's
// per-page ladder) must not ALSO be re-sent by this ladder: the two multiply,
// and the whole-request re-send re-reads every page before the one that
// failed.
func TestSendWithoutRetryPerformsExactlyOneAttempt(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"RATE_LIMITED"}`)
	}))
	defer srv.Close()

	c, _ := sendTestRetrier(t)
	ctx := WithoutRetry(context.Background())
	if !RetryDisabled(ctx) {
		t.Fatal("WithoutRetry must be observable")
	}
	resp, err := c.Send(ctx, getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("want the 429 handed straight back, got %d", resp.StatusCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("want exactly 1 attempt, got %d", calls.Load())
	}
}

// The traffic class is declared on the CONTEXT (WithBackgroundTraffic). Send
// replays a caller-built request, so if it did not carry that declaration onto
// the wire the four islands — which all use Send — would silently go back to
// looking like interactive reads, the exact regression the gateway's traffic
// class exists to prevent.
func TestSendInjectsTheBackgroundTrafficHeader(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get(HeaderTrafficClass))
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	c, _ := sendTestRetrier(t)
	ctx := WithBackgroundTraffic(context.Background())
	resp, err := c.Send(ctx, getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()
	if len(got) != 1 || got[0] != TrafficClassBackground {
		t.Fatalf("want the background marker on the wire, got %q", got)
	}
}

// A header the caller set explicitly wins over the context: the caller may know
// something the context it was handed does not.
func TestSendKeepsAnExplicitTrafficHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(HeaderTrafficClass)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	req := getReq(t, srv.URL)
	req.Header.Set(HeaderTrafficClass, "explicit")
	c, _ := sendTestRetrier(t)
	resp, err := c.Send(WithBackgroundTraffic(context.Background()), req)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()
	if got != "explicit" {
		t.Fatalf("want the explicit header, got %q", got)
	}
}

// A 429 whose Retry-After is longer than the ladder must be handed straight
// back: no sleeps, no extra attempts.
func TestSendDoesNotRetryARetryAfterLongerThanTheLadder(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"RATE_LIMITED"}`)
	}))
	defer srv.Close()

	c, slept := sendTestRetrier(t)
	resp, err := c.Send(context.Background(), getReq(t, srv.URL))
	if err != nil {
		t.Fatalf("Send must hand the 429 back, not error: %v", err)
	}
	defer resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("a 300s hint must not be retried inside one request; attempts=%d", calls.Load())
	}
	if len(*slept) != 0 {
		t.Fatalf("must not sleep; waits=%v", *slept)
	}
}
