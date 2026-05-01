package influx2lp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Config struct {
	Bucket    string `yaml:"bucket"`
	Host      string `yaml:"host"`
	Path      string `yaml:"path"`
	Org       string `yaml:"org"`
	Token     string `yaml:"token"`
	UserAgent string `yaml:"user_agent"`
}

type LPMetric struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   int64
}

func NewConfig() *Config {
	c := &Config{Path: "/api/v2/write"}
	host, err := os.Hostname()
	if err != nil {
		c.UserAgent = "influx2lp-unknown-host"
		return c
	}
	c.UserAgent = "influx2lp-" + host
	return c
}

// LP escaping per https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/
var (
	measurementEsc = strings.NewReplacer(",", `\,`, " ", `\ `)
	tagEsc         = strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `)
	fieldStrEsc    = strings.NewReplacer(`\`, `\\`, `"`, `\"`)
)

// String formats the metric in InfluxDB line protocol.
func (m LPMetric) String() string {
	var b strings.Builder
	b.Grow(len(m.Measurement) + len(m.Tags)*24 + len(m.Fields)*24 + 22)

	b.WriteString(measurementEsc.Replace(m.Measurement))

	if len(m.Tags) > 0 {
		tagKeys := make([]string, 0, len(m.Tags))
		for k := range m.Tags {
			tagKeys = append(tagKeys, k)
		}
		sort.Strings(tagKeys)
		for _, k := range tagKeys {
			b.WriteByte(',')
			b.WriteString(tagEsc.Replace(k))
			b.WriteByte('=')
			b.WriteString(tagEsc.Replace(m.Tags[k]))
		}
	}

	if len(m.Fields) > 0 {
		fieldKeys := make([]string, 0, len(m.Fields))
		for k := range m.Fields {
			fieldKeys = append(fieldKeys, k)
		}
		sort.Strings(fieldKeys)
		for i, k := range fieldKeys {
			if i == 0 {
				b.WriteByte(' ')
			} else {
				b.WriteByte(',')
			}
			b.WriteString(tagEsc.Replace(k))
			b.WriteByte('=')
			appendFieldValue(&b, m.Fields[k])
		}
	}

	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(m.Timestamp, 10))
	return b.String()
}

func appendFieldValue(b *strings.Builder, value interface{}) {
	switch v := value.(type) {
	case int:
		b.WriteString(strconv.FormatInt(int64(v), 10))
		b.WriteByte('i')
	case int8:
		b.WriteString(strconv.FormatInt(int64(v), 10))
		b.WriteByte('i')
	case int16:
		b.WriteString(strconv.FormatInt(int64(v), 10))
		b.WriteByte('i')
	case int32:
		b.WriteString(strconv.FormatInt(int64(v), 10))
		b.WriteByte('i')
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
		b.WriteByte('i')
	case uint:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
		b.WriteByte('i')
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
		b.WriteByte('i')
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
		b.WriteByte('i')
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
		b.WriteByte('i')
	case uint64:
		b.WriteString(strconv.FormatUint(v, 10))
		b.WriteByte('i')
	case float32:
		b.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case string:
		b.WriteByte('"')
		b.WriteString(fieldStrEsc.Replace(v))
		b.WriteByte('"')
	case bool:
		if v {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		fmt.Fprintf(b, "%v", v)
	}
}

// WriteLP formats and writes an LPMetric to InfluxDB.
func WriteLP(ctx context.Context, cli *http.Client, c Config, metric LPMetric) (int, string, error) {
	if c.Bucket == "" {
		return 0, "", fmt.Errorf("no bucket configured")
	}
	if c.Org == "" {
		return 0, "", fmt.Errorf("no org configured")
	}
	return WriteLPString(ctx, cli, c, metric.String())
}

// WriteLPString writes an already-formatted line protocol string to InfluxDB.
func WriteLPString(ctx context.Context, cli *http.Client, c Config, stringMetric string) (int, string, error) {
	u, err := url.Parse(c.Host + c.Path)
	if err != nil {
		return 0, "", fmt.Errorf("invalid host/path: %w", err)
	}
	q := u.Query()
	q.Set("org", c.Org)
	q.Set("bucket", c.Bucket)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(stringMetric))
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("Authorization", "Token "+c.Token)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}

	resp, err := cli.Do(req)
	if err != nil {
		return 0, "failed to write", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), fmt.Errorf("expected status 204, got status %d (uri=%q)", resp.StatusCode, u.String())
	}
	return resp.StatusCode, "", nil
}
