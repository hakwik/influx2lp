# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test ./... -v

# Run a single test
go test ./... -run TestWriteLP/url_encoding

# Run tests for one package only
go test github.com/hakwik/influx2lp
go test github.com/hakwik/influx2lp/writer
```

There is no build step or linter configured — `go vet ./...` is the standard static check.

## Architecture

The repo is split into two packages to allow the line protocol formatter to be used with TinyGo (which does not support `net/http`, `net/url`, or `os`):

**Root package `influx2lp`** (`influxwrite.go`) — TinyGo-compatible. Contains only `LPMetric` and its `String()` method. Imports: `fmt`, `sort`, `strconv`, `strings`. The formatter sorts both tag keys and field keys alphabetically for deterministic output, and escapes special characters per the InfluxDB LP spec using `strings.NewReplacer` vars (`measurementEsc`, `tagEsc`, `fieldStrEsc`).

**Sub-package `influx2lp/writer`** (`writer/writer.go`) — standard Go only. Contains `Config`, `NewConfig`, `WriteLP`, and `WriteLPString`. `WriteLP` delegates to `WriteLPString` after calling `metric.String()`. Both functions require a `context.Context` and a `*http.Client` from the caller; the package does not create or pool clients. URL query parameters are encoded with `url.Values` to handle special characters in org/bucket names.

The split means a TinyGo program imports only the root package and passes `m.String()` to its own transport. A standard Go program imports both packages.

## Key constraints

- `LPMetric.Tags` is `map[string]string` — InfluxDB tags are always strings.
- `LPMetric.Fields` is `map[string]interface{}` — supported concrete types are all signed/unsigned integer widths, `float32`, `float64`, `string`, and `bool`. Unknown types fall through to `fmt.Fprintf(b, "%v", v)`.
- `LPMetric.Timestamp` is Unix nanoseconds (`int64`), matching InfluxDB's default write precision.
- `WriteLP` validates that `Bucket` and `Org` are non-empty before making any network call.
- `Config` has no `Timeout` field — callers control timeout via the `http.Client` they pass in.
