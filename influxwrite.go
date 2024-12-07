package influx2lp

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

type Config struct {
	Bucket    string `yaml:"bucket"`
	Host      string `yaml:"host"`
	Path      string `yaml:"path"`
	Org       string `yaml:"org"`
	Token     string `yaml:"token"`
	UserAgent string `yaml:"user_agent"`
	Timeout   time.Duration
}

type LPMetric struct {
	Measurement string
	Tags        map[string]interface{}
	Fields      map[string]interface{}
	Timestamp   int64
}

func NewConfig() *Config {
	var c Config
	c.Timeout = 3 * time.Second
	c.Path = "/api/v2/write"
	host, err := os.Hostname()
	if err != nil {
		c.UserAgent = "influx2lp-unknown-host"
		return &c
	}
	c.UserAgent = "influx2lp-" + host
	return &c
}

// Formats an LPMetric in line protocol format for writing to InfluxDB (or printing)
func (m LPMetric) String() string {
	metric := m.Measurement
	if len(m.Tags) > 0 {
		for k, v := range m.Tags {
			metric = fmt.Sprintf("%s,%s=%v", metric, k, v)
		}
	}
	if len(m.Fields) > 0 {
		keys := make([]string, 0, len(m.Fields))
		for k := range m.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		i := 0
		for _, k := range keys {
			field := k
			value := m.Fields[k]
			format := ""
			// be careful with how we print values - append "i" to ints and do not use scientific notation for floats
			switch value.(type) {
			case int, int16, int32, int64:
				format = "%di"
			case float32, float64:
				format = "%f"
			case string:
				format = "%q"
			default:
				format = "%v"
			}
			if i == 0 {
				metric = fmt.Sprintf("%s %s="+format, metric, field, value)
			} else {
				metric = fmt.Sprintf("%s,%s="+format, metric, field, value)
			}
			i++
		}
	}
	return fmt.Sprintf("%s %d", metric, m.Timestamp)
}

// WriteLP formats and writes an LPMetric to InfluxDB
func WriteLP(cli http.Client, c Config, metric LPMetric) (int, string, error) {
	if c.Bucket == "" {
		return 0, "", fmt.Errorf("no bucket configured")
	}
	if c.Org == "" {
		return 0, "", fmt.Errorf("no org configured")
	}
	m := metric.String()
	return WriteLPString(cli, c, m)
}

// WriteLPString writes an already formatted line protocol metric string to InfluxDB
func WriteLPString(cli http.Client, c Config, stringMetric string) (int, string, error) {
	d := bytes.NewReader([]byte(stringMetric))

	uri := fmt.Sprintf("%s%s?&org=%s&bucket=%s", c.Host, c.Path, c.Org, c.Bucket)

	req, err := http.NewRequest("POST", uri, d)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Token %s", c.Token))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")

	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}

	resp, err := cli.Do(req)
	if err != nil {
		return 0, "failed to write", err
	}
	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), fmt.Errorf("expected status 204, got status %d (uri=%q)", resp.StatusCode, uri)
	}
	return resp.StatusCode, "", nil
}
