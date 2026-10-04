package tfdclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// newTestClient returns a client pointed at srv with real sleeps replaced by a
// recorder, so the ladder's SHAPE is asserted instead of its wall clock.
func newTestClient(t *testing.T, srv *httptest.Server, opts ...Option) (*Client, *[]time.Duration) {
	t.Helper()
	var slept []time.Duration
	base := []Option{
		WithJitter(func(time.Duration) time.Duration { return 0 }),
		WithSleeper(func(ctx context.Context, d time.Duration) error {
			slept = append(slept, d)
			return ctx.Err()
		}),
	}
	c, err := New(srv.URL, "test-key", append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, &slept
}

// counting wraps a handler so tests can assert how many attempts reached the
// server without threading a counter through every handler.
func counting(n *atomic.Int64, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		next(w, r)
	}
}

func rateLimited(status int, code string, retryAfter string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"code":"` + code + `","message":"push back"}`))
	}
}

func TestRetryLadderIsTheDocumentedShape(t *testing.T) {
	if len(RetryBackoff) != 4 {
		t.Fatalf("ladder must have 4 rungs, got %d", len(RetryBackoff))
	}
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second} {
		if RetryBackoff[i] != want {
			t.Fatalf("rung %d: want %v, got %v", i, want, RetryBackoff[i])
		}
	}
	if RetryAfterCap != RetryBackoff[len(RetryBackoff)-1] {
		t.Fatalf("RetryAfterCap must equal the last rung; want %v, got %v", RetryBackoff[len(RetryBackoff)-1], RetryAfterCap)
	}
}

func TestGetRetriesRateLimitedThenSucceeds(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			rateLimited(http.StatusTooManyRequests, CodeRateLimited, "")(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c, slept := newTestClient(t, srv)
	var out map[string]any
	if _, err := c.Get(context.Background(), "/x", nil, &out); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("want 3 attempts, got %d", got)
	}
	if len(*slept) != 2 || (*slept)[0] != time.Second || (*slept)[1] != 2*time.Second {
		t.Fatalf("want waits [1s 2s], got %v", *slept)
	}
	if out["ok"] != true {
		t.Fatalf("body must be decoded: %+v", out)
	}
}

func TestGetExhaustsTheLadderAndReturnsThe429(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(counting(&calls, rateLimited(http.StatusTooManyRequests, CodeRateLimited, "")))
	defer srv.Close()

	c, slept := newTestClient(t, srv)
	var out map[string]any
	_, err := c.Get(context.Background(), "/x", nil, &out)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want *APIError, got %v", err)
	}
	if apiErr.Code != CodeRateLimited || !apiErr.Retryable() {
		t.Fatalf("unexpected error: %+v", apiErr)
	}
	// 1 initial + 4 rungs.
	if got := calls.Load(); got != 5 {
		t.Fatalf("want 5 attempts, got %d", got)
	}
	if len(*slept) != 4 {
		t.Fatalf("want 4 waits, got %v", *slept)
	}
}

// A scheduler-busy 429 with NO Retry-After is retried: the hint is unknown, so
// the ladder is the best guess.
func TestSchedulerBusyWithNoHintIsRetried(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			rateLimited(http.StatusTooManyRequests, CodeSchedulerBusy, "")(w, r)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c, _ := newTestClient(t, srv)
	if _, err := c.Get(context.Background(), "/x", nil, nil); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("QC_SCHEDULER_BUSY must be retried; attempts=%d", calls.Load())
	}
}

// …but the hint tfdapi ACTUALLY sends for scheduler-busy is 30s (MaxP1Wait),
// and a hint past the ladder is a "come back later": re-sending five times at a
// queue that just said "busy" is how the caller makes our own congestion worse.
// The old version of this test pinned the opposite behaviour by omitting the
// header production always sends.
func TestSchedulerBusyWithItsRealHintIsNotRetried(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		rateLimited(http.StatusTooManyRequests, CodeSchedulerBusy, "30")(w, r)
	}))
	defer srv.Close()
	c, slept := newTestClient(t, srv)
	if _, err := c.Get(context.Background(), "/x", nil, nil); err == nil {
		t.Fatal("want the 429 back")
	}
	if calls.Load() != 1 {
		t.Fatalf("a 30s hint must not be absorbed by a 4s-capped ladder; attempts=%d", calls.Load())
	}
	if len(*slept) != 0 {
		t.Fatalf("must not sleep; waits=%v", *slept)
	}
}

func TestNonRetryableErrorsAreNotRetried(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"500":                       {http.StatusInternalServerError, `{"code":"BOOM","message":"x"}`},
		"400":                       {http.StatusBadRequest, `{"code":"BAD","message":"x"}`},
		"429 with unknown code":     {http.StatusTooManyRequests, `{"code":"SOMETHING_ELSE","message":"x"}`},
		"409 retry governance":      {http.StatusConflict, `{"code":"RETRY_GOVERNANCE_BACKOFF","message":"x"}`},
		"429 without a code at all": {http.StatusTooManyRequests, `<html>gateway</html>`},
		"429 retryable code on 200": {http.StatusOK, `{"code":"RATE_LIMITED"}`},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if tc.status != http.StatusOK {
					w.WriteHeader(tc.status)
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, slept := newTestClient(t, srv)
			_, err := c.Get(context.Background(), "/x", nil, nil)
			if tc.status == http.StatusOK {
				if err != nil {
					t.Fatalf("200 must not be an error: %v", err)
				}
			} else if err == nil {
				t.Fatalf("want an error")
			}
			if calls.Load() != 1 {
				t.Fatalf("must not retry; attempts=%d", calls.Load())
			}
			if len(*slept) != 0 {
				t.Fatalf("must not sleep; waits=%v", *slept)
			}
		})
	}
}

func TestPostIsNeverRetriedByDefault(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		rateLimited(http.StatusTooManyRequests, CodeRateLimited, "")(w, r)
	}))
	defer srv.Close()
	c, slept := newTestClient(t, srv)
	if _, err := c.PostJSON(context.Background(), "/x", map[string]string{"a": "b"}, nil); err == nil {
		t.Fatalf("want an error")
	}
	if calls.Load() != 1 {
		t.Fatalf("POST must not be auto-retried; attempts=%d", calls.Load())
	}
	if len(*slept) != 0 {
		t.Fatalf("POST must not sleep; waits=%v", *slept)
	}
}

func TestDoCanOptAnIdempotentWriteIntoRetrying(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			rateLimited(http.StatusTooManyRequests, CodeRateLimited, "")(w, r)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c, _ := newTestClient(t, srv)
	if _, err := c.Do(context.Background(), http.MethodPost, "/x", nil, []byte(`{}`), nil, true); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("an explicitly idempotent write must be retried; attempts=%d", calls.Load())
	}
}

func TestRetryAfterIsHonouredAndClamped(t *testing.T) {
	for name, tc := range map[string]struct {
		header string
		want   []time.Duration
	}{
		"honoured when short":   {header: "2", want: []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second}},
		"at the cap is retried": {header: "4", want: []time.Duration{RetryAfterCap, RetryAfterCap, RetryAfterCap, RetryAfterCap}},
		// A hint past the ladder is a "come back later", not a "hold the line":
		// 40110 forwards its ~300s freeze, and clamping that to 4s would mean
		// five attempts against an interface we were told to leave alone.
		"over the cap is a later": {header: "60", want: nil},
		"nonsense is no hint":     {header: "soon", want: []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second}},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			srv := httptest.NewServer(counting(&calls, rateLimited(http.StatusTooManyRequests, CodeRateLimited, tc.header)))
			defer srv.Close()
			c, slept := newTestClient(t, srv)
			_, _ = c.Get(context.Background(), "/x", nil, nil)
			if len(*slept) != len(tc.want) {
				t.Fatalf("waits: want %v, got %v", tc.want, *slept)
			}
			for i := range tc.want {
				if (*slept)[i] != tc.want[i] {
					t.Fatalf("wait %d: want %v, got %v", i, tc.want[i], (*slept)[i])
				}
			}
			if want := int64(len(tc.want) + 1); calls.Load() != want {
				t.Fatalf("attempts: want %d, got %d", want, calls.Load())
			}
		})
	}
}

func TestRetryBudgetZeroDisablesRetrying(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(counting(&calls, rateLimited(http.StatusTooManyRequests, CodeRateLimited, "")))
	defer srv.Close()
	c, slept := newTestClient(t, srv, WithRetryBudget(nil))
	if _, err := c.Get(context.Background(), "/x", nil, nil); err == nil {
		t.Fatalf("want an error")
	}
	if calls.Load() != 1 || len(*slept) != 0 {
		t.Fatalf("budget 0 must fail fast; attempts=%d waits=%v", calls.Load(), *slept)
	}
}

func TestContextCancellationDuringBackoffReturnsTheContextError(t *testing.T) {
	srv := httptest.NewServer(rateLimited(http.StatusTooManyRequests, CodeRateLimited, ""))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(srv.URL, "k", WithJitter(func(time.Duration) time.Duration { return 0 }),
		WithSleeper(func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, gotErr := c.Get(ctx, "/x", nil, nil)
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("want context.Canceled (the caller gave up, not the 429), got %v", gotErr)
	}
}

func TestHeadersAreAttachedAndTrafficClassIsContextScoped(t *testing.T) {
	type seen struct {
		auth    string
		traffic string
		ua      string
	}
	got := make(chan seen, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{auth: r.Header.Get("Authorization"), traffic: r.Header.Get(HeaderTrafficClass), ua: r.Header.Get("User-Agent")}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL, "test-key")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.Get(context.Background(), "/x", nil, nil); err != nil {
		t.Fatalf("Get: %v", err)
	}
	first := <-got
	if first.auth != "Bearer test-key" || first.traffic != "" || first.ua == "" {
		t.Fatalf("unexpected headers: %+v", first)
	}
	if _, err := c.Get(WithBackgroundTraffic(context.Background()), "/x", nil, nil); err != nil {
		t.Fatalf("Get: %v", err)
	}
	second := <-got
	if second.traffic != TrafficClassBackground {
		t.Fatalf("background declaration must reach the wire: %+v", second)
	}
}

func TestQueryParamsAndDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("advertiser_id") != "777" {
			t.Errorf("query not forwarded: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"value":{"nested":5}}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL, "k")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var out struct {
		Value struct{ Nested int } `json:"value"`
	}
	if _, err := c.Get(context.Background(), "/x", url.Values{"advertiser_id": {"777"}}, &out); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out.Value.Nested != 5 {
		t.Fatalf("decode: %+v", out)
	}
}

func TestParseRetryAfterBothForms(t *testing.T) {
	if d := parseRetryAfter("3"); d != 3*time.Second {
		t.Fatalf("delta seconds: got %v", d)
	}
	future := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(future); d <= 0 || d > 5*time.Second {
		t.Fatalf("http-date: got %v", d)
	}
	for _, bad := range []string{"", "0", "-1", "soon", "Thu, 01 Jan 1970 00:00:00 GMT"} {
		if d := parseRetryAfter(bad); d != 0 {
			t.Fatalf("parseRetryAfter(%q): want 0 (no hint), got %v", bad, d)
		}
	}
}

func TestAPIErrorParsesEnvelopeAndSnippet(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "7")
	apiErr := newAPIError(http.StatusTooManyRequests, "429 Too Many Requests", h, []byte(`{"code":"RATE_LIMITED","message":"busy","request_id":"rq-1"}`))
	if apiErr.Code != CodeRateLimited || apiErr.Message != "busy" || apiErr.RequestID != "rq-1" || apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("unexpected: %+v", apiErr)
	}
	if !apiErr.Retryable() {
		t.Fatalf("RATE_LIMITED on 429 must be retryable")
	}
	wrapped := newAPIError(http.StatusBadGateway, "502", nil, []byte(`{"error":"upstream died"}`))
	if wrapped.Message != "upstream died" || wrapped.Retryable() {
		t.Fatalf("unexpected: %+v", wrapped)
	}
	long := make([]byte, maxBodySnippet*2)
	for i := range long {
		long[i] = 'x'
	}
	snippet := newAPIError(http.StatusInternalServerError, "500", nil, long)
	if len(snippet.Message) <= maxBodySnippet {
		t.Fatalf("long bodies must be truncated, got %d chars", len(snippet.Message))
	}
}

func TestJitterIsAddedOnTopOfTheRung(t *testing.T) {
	srv := httptest.NewServer(rateLimited(http.StatusTooManyRequests, CodeRateLimited, ""))
	defer srv.Close()
	var slept []time.Duration
	c, err := New(srv.URL, "k",
		WithJitter(func(base time.Duration) time.Duration { return base / 4 }),
		WithSleeper(func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, _ = c.Get(context.Background(), "/x", nil, nil)
	want := []time.Duration{1250 * time.Millisecond, 2500 * time.Millisecond, 3750 * time.Millisecond, 5 * time.Second}
	for i := range want {
		if slept[i] != want[i] {
			t.Fatalf("wait %d: want %v, got %v", i, want[i], slept[i])
		}
	}
}

func TestJitterDecorrelatesConcurrentCallers(t *testing.T) {
	// A fixed ladder would make every caller wake at the same instant; the
	// default jitter must spread them. Statistical, but with 64 samples the
	// chance of a false pass is negligible.
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 64; i++ {
		d := randJitterSample(time.Second)
		if d < 0 || d > time.Second/4 {
			t.Fatalf("jitter out of range: %v", d)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 16 {
		t.Fatalf("jitter must decorrelate; only %d distinct waits in 64 samples", len(seen))
	}
}

func randJitterSample(base time.Duration) time.Duration {
	c, err := New("http://example.invalid", "")
	if err != nil {
		panic(err)
	}
	return c.jitter(base)
}

func TestNewRejectsRelativeBaseURL(t *testing.T) {
	if _, err := New("/api", "k"); err == nil {
		t.Fatalf("a relative base URL must be rejected at construction, not at first use")
	}
}

func TestRetryAfterHeaderRoundTripWithStatusCode(t *testing.T) {
	srv := httptest.NewServer(rateLimited(http.StatusTooManyRequests, CodeRateLimited, "4"))
	defer srv.Close()
	c, _ := newTestClient(t, srv)
	_, err := c.Get(context.Background(), "/x", nil, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want *APIError, got %v", err)
	}
	if apiErr.StatusCode != http.StatusTooManyRequests || apiErr.RetryAfter != 4*time.Second {
		t.Fatalf("unexpected: %+v", apiErr)
	}
	if _, err := json.Marshal(apiErr); err != nil {
		// APIError has no JSON tags on purpose; this only asserts it does not
		// panic or misbehave if something serializes it.
		t.Fatalf("marshal: %v", err)
	}
}

func TestRetryableCodesAreTheGatewayContract(t *testing.T) {
	want := map[string]bool{CodeRateLimited: true, CodeSchedulerBusy: true, CodeRetryGovernanceBackoff: true}
	if len(RetryableCodes) != len(want) {
		t.Fatalf("retryable set drifted: %v", RetryableCodes)
	}
	for code := range want {
		if _, ok := RetryableCodes[code]; !ok {
			t.Fatalf("missing %s", code)
		}
	}
	if strconv.Itoa(0) == "" {
		t.Fatal("unreachable")
	}
}
