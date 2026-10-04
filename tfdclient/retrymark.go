package tfdclient

import "context"

// retryDisabledCtxKey marks a request whose re-send is owned by something
// BELOW the SDK — today, exactly one thing: tfdapi's per-page ladder
// (?page_retry_seconds on a plan-materials read), which absorbs a rate limit
// on the ONE page that took it and keeps the pages already walked.
//
// WHY THIS EXISTS. The two ladders answer different questions and are good at
// different things. The page ladder wins whenever a read walks several pages,
// because re-sending the whole request re-reads every page before the one that
// failed — more calls to the quota the retry was trying to survive. The SDK
// ladder wins when there is nothing to keep: one logical read, one response.
// Letting both run MULTIPLIES them (up to 5 SDK attempts, each spending the
// page ladder's rungs), which is why the caller must pick one — and why the
// proof of that choice travels with the request instead of being assumed.
//
// It is deliberately NOT a general "turn retries off" for callers that find
// the ladder inconvenient: nothing sets it except a request that has already
// bought a bounded, pagination-aware retry of its own.
type retryDisabledCtxKey struct{}

// WithoutRetry marks ctx so Send performs exactly one attempt and hands the
// response back whatever it is. Apply it only where the gateway is already
// retrying the same logical read better than a whole-request re-send could.
func WithoutRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryDisabledCtxKey{}, true)
}

// RetryDisabled reports whether ctx was marked by WithoutRetry.
func RetryDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(retryDisabledCtxKey{}).(bool)
	return v
}

// The retry permission for a request that is NOT a GET.
//
// It rides on the REQUEST's context rather than on the Client because one
// client serves many operations, and because the assertion is per-operation:
// "this particular write is safe to apply twice". A client-level flag would
// say it about every write the client ever makes.
//
// The gateway makes the same argument in the other direction
// (wireRateLimitRetryBudget's method guard): a POST that 千川 may already have
// applied must not be re-sent just because a caller tagged something. Here the
// caller is explicitly choosing to say so.
type allowRetryKey struct{}

// AllowRetry marks a request's context as safe to re-send after a rate limit.
// Use it only for an operation that is idempotent at the upstream — an update
// that sets a value, a delete by id, an upsert with a caller-supplied key.
//
// Attach it to the request the caller passes to Send:
//
//	req = req.WithContext(tfdclient.AllowRetry(req.Context()))
func AllowRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, allowRetryKey{}, true)
}

func allowRetry(ctx context.Context) bool {
	v, _ := ctx.Value(allowRetryKey{}).(bool)
	return v
}
