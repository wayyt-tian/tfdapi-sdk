// tfdapi-sdk is the caller-side SDK for the tfdapi gateway, as its own module
// so every island (tfd-idea / tfd-core / cloudagents / tfd-pilot) can depend on
// it without depending on tfdapi's internals.
//
// This module is PRIVATE: consumers need `GOPRIVATE=github.com/wayyt-tian/*`
// (and git credentials for github.com) to fetch it. See README.md.
module github.com/wayyt-tian/tfdapi-sdk

go 1.26.0
