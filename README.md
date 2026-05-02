# influx2lp

A Go library for formatting and writing metrics to InfluxDB v2 using the [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).

The library is split into two packages to support TinyGo targets:

| Package | Import path | TinyGo compatible |
|---|---|---|
| `influx2lp` | `github.com/hakwik/influx2lp` | Yes |
| `influx2lp/writer` | `github.com/hakwik/influx2lp/writer` | No |

The root package handles metric construction and line protocol formatting using only TinyGo-safe imports (`fmt`, `sort`, `strconv`, `strings`). The `writer` sub-package adds InfluxDB HTTP transport and depends on `net/http`, `net/url`, and `os`, which are not available on bare-metal TinyGo targets.

## Installation

```
go get github.com/hakwik/influx2lp
```

## Building a metric

`LPMetric` and its `String()` formatter live in the root package — the only import needed on TinyGo.

```go
import "github.com/hakwik/influx2lp"

m := influx2lp.LPMetric{
    Measurement: "cpu",
    Tags: map[string]string{
        "host":   "web-01",
        "region": "eu-west",
    },
    Fields: map[string]interface{}{
        "usage_user":   72.4,
        "usage_system": 5.1,
        "core_count":   int64(8),
        "throttled":    false,
    },
    Timestamp: time.Now().UnixNano(),
}
```

**Tags** must be `string` values — this matches the InfluxDB line protocol requirement.

**Fields** support the following Go types:

| Go type | LP encoding | Example |
|---|---|---|
| `int`, `int8`, `int16`, `int32`, `int64` | integer (`i`) | `42i` |
| `uint`, `uint8`, `uint16`, `uint32`, `uint64` | integer (`i`) | `42i` |
| `float32`, `float64` | float | `3.14` |
| `string` | quoted string | `"hello"` |
| `bool` | boolean | `true` / `false` |

**Timestamp** is Unix nanoseconds (`int64`), matching InfluxDB's default precision.

### Formatting to line protocol

`LPMetric` implements `fmt.Stringer`. Tag and field keys are sorted alphabetically, making the output deterministic.

```go
fmt.Println(m.String())
// cpu,host=web-01,region=eu-west core_count=8i,throttled=false,usage_system=5.1,usage_user=72.4 1714000000000000000
```

Special characters are escaped automatically per the [LP specification](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#special-characters):

| Context | Characters escaped |
|---|---|
| Measurement name | `,` and ` ` |
| Tag keys and values | `,`, `=`, and ` ` |
| Field keys | `,`, `=`, and ` ` |
| String field values | `"` and `\` |

## Writing to InfluxDB

Import the `writer` sub-package in addition to the root package. This is standard Go only — not compatible with TinyGo.

```go
import (
    "context"
    "net/http"
    "time"

    "github.com/hakwik/influx2lp"
    "github.com/hakwik/influx2lp/writer"
)
```

### Configuring the client

`writer.NewConfig` returns a `*Config` with the write path pre-set and the user-agent derived from the local hostname.

```go
c := writer.NewConfig()
c.Host   = "http://localhost:8086"
c.Org    = "my-org"
c.Bucket = "my-bucket"
c.Token  = "my-token"
```

| Field | Default | Description |
|---|---|---|
| `Host` | `""` | InfluxDB base URL (no trailing `/`) |
| `Path` | `"/api/v2/write"` | Write endpoint path |
| `Org` | `""` | InfluxDB organisation |
| `Bucket` | `""` | Target bucket |
| `Token` | `""` | API token |
| `UserAgent` | `"influx2lp-<hostname>"` | `User-Agent` header value |

### Sending a metric

```go
cli := &http.Client{Timeout: 5 * time.Second}
ctx := context.Background()

status, body, err := writer.WriteLP(ctx, cli, *c, m)
if err != nil {
    log.Printf("write failed (status %d): %s: %v", status, body, err)
}
```

`WriteLP` returns the HTTP status code, the raw response body (non-empty only on error), and an error.

### Sending a pre-formatted line protocol string

If you already have a line protocol string (e.g. forwarded from another source), use `WriteLPString` directly:

```go
lp := `cpu,host=web-01 usage_user=72.4,core_count=8i 1714000000000000000`
status, body, err := writer.WriteLPString(ctx, cli, *c, lp)
```

## Error handling

| Condition | Behaviour |
|---|---|
| `Bucket` or `Org` empty | `WriteLP` returns an error immediately, no request sent |
| Non-204 HTTP response | Returns the status code, response body, and an error |
| Network / context error | Returns the underlying `http` error |

## TinyGo usage

On TinyGo targets, import only the root package and deliver the formatted string over whatever transport is available (UART, SPI, a custom TCP stack, etc.):

```go
import "github.com/hakwik/influx2lp"

m := influx2lp.LPMetric{ /* ... */ }
sendOverUART(m.String())
```

## License

MIT
