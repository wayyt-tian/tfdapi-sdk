# tfdapi-sdk

The caller-side SDK for the tfdapi gateway. It exists so every island
(`tfd-idea`, `tfd-core`, `cloudagents`, `tfd-pilot`) shares **one** retry ladder
for the gateway's uniform `429`, instead of each growing its own.

- Package: `tfdclient` — `github.com/wayyt-tian/tfdapi-sdk/tfdclient`.
- The ladder (`RetryBackoff`) is **1/2/3/4 seconds**: four rungs, at most five
  attempts per logical request, one definition in one place.
- The gateway does **not** retry; deciding whether a `429` is worth re-sending
  is the caller's job. Design + rationale: `QIANCHUAN-RETRY-SDK-AND-QPS-PLAN.md`
  in the tfdapi repo (S5/S6/S8).

## Use it

```go
require github.com/wayyt-tian/tfdapi-sdk v0.1.0
```

Two shapes, pick the one that fits:

- `tfdclient.New(...)` — the full client (base URL + token) if the island is
  happy to build its requests through it.
- `tfdclient.NewRetrier(...)` + `Send(ctx, req)` — for an island client that
  already owns its request shape/decoding and only needs the retry ladder moved
  to the caller.

### Fetching

This module is **public**. Consumers need no `GOPRIVATE`, no token, and no
special git config: `go get github.com/wayyt-tian/tfdapi-sdk@vX.Y.Z` resolves
through the configured module proxy like any other dependency. It carries the
retry ladder and an HTTP client only — no credentials, no business logic, no
tfdapi internals.

## Versioning

- Plain `vX.Y.Z` tags on `main`.
- Changing the ladder, the retry predicate, or anything that alters when a
  request is re-sent is a **behavioural break for every caller**: bump the minor
  (major once ≥1.0) and update the islands in the same change set.
- `v0.x` means the contract may still move.

## CI

`go build` / `go vet` / `go test -race` / `golangci-lint v2.12.2`
(config: `.golangci.yml`) on every PR and on `main`.
