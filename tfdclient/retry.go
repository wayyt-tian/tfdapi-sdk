package tfdclient

import "time"

// RetryBackoff is THE retry ladder: one definition, one place.
//
// 1/2/3/4 seconds, i.e. four rungs and therefore at most five attempts.
// QIANCHUAN-RETRY-SDK-AND-QPS-PLAN.md §1.5 has the shape and the reasoning;
// the two properties that matter are:
//
//   - it is BOUNDED and SHORT. The whole ladder costs 10s of sleeping. A user
//     waiting on a button can absorb that; they cannot absorb the gateway's old
//     Retry-After: 60, which is why the gateway's fallback was cut to seconds
//     (qianchuan.RateLimitedRetryAfterFallbackSeconds) in the same change.
//   - the LAST rung is 4s, not "however long the server says". A server hint
//     longer than RetryAfterCap is clamped, because past a few seconds the
//     honest answer is "come back later", not "hold this connection open".
//
// Exported so every caller imports the same slice rather than growing a
// second, drifting copy — the C4 rule from TFDAPI-QIANCHUAN-GATEWAY.md: the
// ladder may live in one place only.
var RetryBackoff = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	3 * time.Second,
	4 * time.Second,
}

// RetryAfterCap clamps a server-provided Retry-After. It equals the ladder's
// last rung on purpose: a hint longer than the longest rung we would ever wait
// on our own is a hint we do not follow inside one request.
//
// Since 2026-10-02 a hint at or below this cap is the ONLY case the retry loop
// runs at all (APIError.RetryableWithinLadder): an interface that says "wait
// 30s" or "wait 300s" is declined outright rather than clamped, so the clamp in
// retryWait is now defence-in-depth for a header that changes mid-ladder.
const RetryAfterCap = 4 * time.Second

// JitterFraction is the maximum extra wait, as a fraction of the nominal one,
// added to decorrelate callers.
//
// WITHOUT IT THE LADDER IS WORSE THAN NO LADDER. Every caller rejected in the
// same instant — which is what a platform-wide 40100 does, since it is thrown
// at whoever happens to be sending — would sleep the same 1s, wake together,
// be rejected together, and sleep the same 2s. The system then alternates
// between a thundering herd and an empty interface: the classic metastable
// failure of a fixed backoff (QIANCHUAN-RETRY-SDK-AND-QPS-PLAN.md §3.4).
const JitterFraction = 0.25

// ladderWait returns the nominal wait before attempt number retriesMade+1, and
// whether there is any rung left.
func ladderWait(ladder []time.Duration, retriesMade int) (time.Duration, bool) {
	if retriesMade < 0 || retriesMade >= len(ladder) {
		return 0, false
	}
	return ladder[retriesMade], true
}

// retryWait resolves the actual sleep before the next attempt.
//
// Precedence:
//  1. the server's Retry-After (clamped to RetryAfterCap), because it is the
//     one number that comes from the side that knows when capacity returns;
//  2. otherwise the ladder's rung.
//
// Both get jitter added on top. Jitter is applied AFTER the clamp so a clamped
// hint stays a hint; the extra ≤25% is inside the caller's stated budget.
func retryWait(ladder []time.Duration, retriesMade int, retryAfter time.Duration, jitter func(time.Duration) time.Duration) (time.Duration, bool) {
	base, ok := ladderWait(ladder, retriesMade)
	if retryAfter > 0 {
		if retryAfter > RetryAfterCap {
			retryAfter = RetryAfterCap
		}
		base, ok = retryAfter, true
	}
	if !ok {
		return 0, false
	}
	if jitter != nil && base > 0 {
		base += jitter(base)
	}
	return base, true
}
