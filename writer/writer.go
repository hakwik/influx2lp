// Package writer sends influx2lp metrics to InfluxDB v2 over HTTP.
// This package is not compatible with TinyGo; use the parent influx2lp
// package directly when targeting microcontrollers or WASM.
package writer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/hakwik/influx2lp"
)

type Config struct {
	Bucket    string `yaml:"bucket"`
	Host      string `yaml:"host"`
	Path      string `yaml:"path"`
	Org       string `yaml:"org"`
	Token     string `yaml:"token"`
	UserAgent string `yaml:"user_agent"`
}

// NewConfig returns a Config with the write path pre-set and the user-agent
// derived from the local hostname.
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

// WriteLP formats metric as line protocol and writes it to InfluxDB.
func WriteLP(ctx context.Context, cli *http.Client, c Config, metric influx2lp.LPMetric) (int, string, error) {
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
