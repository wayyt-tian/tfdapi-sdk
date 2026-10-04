package tfdclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Codes the gateway mints on a caller-visible 429. Byte-identical to
// qianchuan.RateLimitedErrorCode and qianchuan.SchedulerBusyErrorCode in
// tfdapi — they are a cross-repo contract, so they are written out here rather
// than imported (this module must not depend on tfdapi's internals).
const (
	// CodeRateLimited means 千川 pushed back: the platform-total 40100 (random,
	// retry is the official advice), our developer quota 40110, or the local
	// endpoint freeze those two can arm.
	CodeRateLimited = "RATE_LIMITED"
	// CodeSchedulerBusy means tfdapi's OWN admission queue was full of more
	// urgent work and gave up on this read. Same "come back later" contract,
	// different owner — it is our capacity, not 千川's. tfdapi attaches a
	// Retry-After of up to MaxP1Wait (30s), so RetryableWithinLadder declines to
	// absorb it: re-sending five times against a queue that just said "busy"
	// makes our own congestion worse, not better.
	CodeSchedulerBusy = "QC_SCHEDULER_BUSY"
	// CodeRetryGovernanceBackoff means our own retry governor is holding this
	// operation back. Retryable, but on the governor's schedule.
	CodeRetryGovernanceBackoff = "RETRY_GOVERNANCE_BACKOFF"
)

// RetryableCodes is the complete set this SDK will re-send. Everything else —
// every other 4xx, every 5xx — is returned to the caller untouched.
//
// WHY 5xx IS NOT HERE. A 5xx is as likely to be a genuine bug as a hiccup, and
// re-sending a request that failed on a nil map or a bad SQL statement four
// times turns one error into five and hides the first. The gateway maps its
// retryable conditions to 429 explicitly; if a 5xx ever means "retry", that is
// a gateway bug to fix there, not a client-side guess.
var RetryableCodes = map[string]struct{}{
	CodeRateLimited:            {},
	CodeSchedulerBusy:          {},
	CodeRetryGovernanceBackoff: {},
}

// APIError is a non-2xx tfdapi response.
type APIError struct {
	StatusCode int
	Status     string
	// Code is the machine-readable body code when the response carried the
	// {"code","message"} envelope, empty otherwise.
	Code string
	// Message is the human-readable body message, or a bounded snippet of the
	// raw body when there was no envelope.
	Message string
	// RequestID is tfdapi's request id when present, for cross-referencing
	// gateway logs.
	RequestID string
	// RetryAfter is the parsed Retry-After header, zero when absent.
	RetryAfter time.Duration
	// Body is the raw (bounded) response body.
	Body []byte
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	code := e.Code
	if code == "" {
		code = "-"
	}
	return fmt.Sprintf("tfdapi %s (status=%d code=%s): %s", e.Status, e.StatusCode, code, e.Message)
}

// Retryable reports whether this error's CODE is one the SDK is willing to
// re-send at all. Callers use it to decide whether to surface "retry later" or
// to show the failure as final — but see RetryableWithinLadder: an error can be
// retryable in principle and still be too expensive to retry inside one
// request.
func (e *APIError) Retryable() bool {
	if e == nil || e.StatusCode != http.StatusTooManyRequests {
		return false
	}
	_, ok := RetryableCodes[e.Code]
	return ok
}

// RetryableWithinLadder is the decision the retry loop actually makes: the code
// is retryable AND the server's own Retry-After is short enough that waiting it
// out inside one request is honest.
//
// WHY THE SECOND CONDITION EXISTS. A 429 that says "come back in 300 seconds"
// is a 40110 developer-quota freeze (scheduler_errors.go forwards the remaining
// freeze as Retry-After) or, at 30 seconds, tfdapi's own admission queue telling
// us to back off. Clamping either to RetryAfterCap and re-sending four times
// does not bring the capacity back sooner — it just spends the ladder hammering
// an interface that has just told us to leave it alone, which is exactly the
// behaviour the 40110 penalty is meant to punish. A hint longer than the ladder
// itself is a "later", not a "hold on".
func (e *APIError) RetryableWithinLadder() bool {
	return e.Retryable() && e.RetryAfter <= RetryAfterCap
}

const maxBodySnippet = 512

// newAPIError parses a non-2xx response into an APIError.
//
// The Retry-After parse accepts the two forms HTTP allows (delta-seconds and
// HTTP-date) because a wrong "no hint" reading silently downgrades the wait to
// the ladder — a real bug, not a cosmetic one.
func newAPIError(statusCode int, status string, header http.Header, body []byte) *APIError {
	apiErr := &APIError{
		StatusCode: statusCode,
		Status:     status,
		Body:       body,
	}
	if header != nil {
		apiErr.RetryAfter = parseRetryAfter(header.Get("Retry-After"))
	}
	var envelope struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		apiErr.Code = envelope.Code
		apiErr.Message = envelope.Message
		apiErr.RequestID = envelope.RequestID
	}
	if apiErr.Message == "" {
		// Older surfaces answer {"error": "..."} — accept that shape too.
		var wrapped struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Error != "" {
			apiErr.Message = wrapped.Error
		}
	}
	if apiErr.Message == "" {
		apiErr.Message = bodySnippet(body)
	}
	return apiErr
}

func bodySnippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > maxBodySnippet {
		s = s[:maxBodySnippet] + "…"
	}
	if s == "" {
		s = "<empty body>"
	}
	return s
}

// parseRetryAfter understands both forms HTTP permits. A malformed value is
// "no hint" (zero), never "retry immediately".
func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(raw); err == nil {
		d := time.Until(t)
		if d <= 0 {
			return 0
		}
		return d
	}
	return 0
}
