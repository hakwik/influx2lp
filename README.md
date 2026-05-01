# influx2lp

A minimal Go library for formatting and writing metrics to InfluxDB v2 using the [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).

## Installation

```
go get github.com/hakwik/influx2lp
```

## Usage

### Configuring the client

`NewConfig` returns a `*Config` with sensible defaults: the write path is set to `/api/v2/write` and the user-agent is derived from the local hostname.

```go
c := influx2lp.NewConfig()
c.Host   = "http://localhost:8086"
c.Org    = "my-org"
c.Bucket = "my-bucket"
c.Token  = "my-token"
```

| Field       | Default                  | Description                        |
|-------------|--------------------------|------------------------------------|
| `Host`      | `""`                     | InfluxDB base URL (no trailing `/`) |
| `Path`      | `"/api/v2/write"`        | Write endpoint path                |
| `Org`       | `""`                     | InfluxDB organisation              |
| `Bucket`    | `""`                     | Target bucket                      |
| `Token`     | `""`                     | API token                          |
| `UserAgent` | `"influx2lp-<hostname>"` | `User-Agent` header value          |

### Building a metric

```go
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

| Go type                                        | LP encoding    | Example        |
|------------------------------------------------|----------------|----------------|
| `int`, `int8`, `int16`, `int32`, `int64`       | integer (`i`)  | `42i`          |
| `uint`, `uint8`, `uint16`, `uint32`, `uint64`  | integer (`i`)  | `42i`          |
| `float32`, `float64`                           | float          | `3.14`         |
| `string`                                       | quoted string  | `"hello"`      |
| `bool`                                         | boolean        | `true`/`false` |

**Timestamp** is Unix nanoseconds (`int64`), matching InfluxDB's default precision.

### Writing to InfluxDB

```go
cli := &http.Client{Timeout: 5 * time.Second}
ctx := context.Background()

status, body, err := influx2lp.WriteLP(ctx, cli, *c, m)
if err != nil {
    log.Printf("write failed (status %d): %s: %v", status, body, err)
}
```

`WriteLP` returns the HTTP status code, the raw response body (non-empty only on error), and an error.

### Writing a pre-formatted line protocol string

If you already have a line protocol string (e.g. forwarded from another source), use `WriteLPString` directly:

```go
lp := `cpu,host=web-01 usage_user=72.4,core_count=8i 1714000000000000000`
status, body, err := influx2lp.WriteLPString(ctx, cli, *c, lp)
```

### Formatting without writing

`LPMetric` implements `fmt.Stringer`, so you can obtain the line protocol string without sending it anywhere:

```go
fmt.Println(m.String())
// cpu,host=web-01,region=eu-west core_count=8i,throttled=false,usage_system=5.1,usage_user=72.4 1714000000000000000
```

Tag and field keys are always sorted alphabetically, making the output deterministic.

### Escaping

Special characters are escaped automatically per the [InfluxDB line protocol specification](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#special-characters):

| Context              | Characters escaped  |
|----------------------|---------------------|
| Measurement name     | `,` and ` `         |
| Tag keys and values  | `,`, `=`, and ` `   |
| Field keys           | `,`, `=`, and ` `   |
| String field values  | `"` and `\`         |

Org and bucket values in the HTTP query string are percent-encoded automatically.

## Error handling

| Condition               | Behaviour                                                |
|-------------------------|----------------------------------------------------------|
| `Bucket` or `Org` empty | `WriteLP` returns an error immediately, no request sent  |
| Non-204 HTTP response   | Returns the status code, response body, and an error     |
| Network / context error | Returns the underlying `http` error                      |

## License

MIT
