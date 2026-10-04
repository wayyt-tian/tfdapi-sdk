// tfdapi-sdk is the caller-side SDK for the tfdapi gateway, as its own module
// so every island (tfd-idea / tfd-core / cloudagents / tfd-pilot) can depend on
// it without depending on tfdapi's internals.
//
// This module is PUBLIC: consumers fetch it through the normal Go module
// machinery with no credentials and no GOPRIVATE. See README.md.
module github.com/wayyt-tian/tfdapi-sdk

go 1.26.0
