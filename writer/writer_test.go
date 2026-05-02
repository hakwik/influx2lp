package writer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hakwik/influx2lp"
)

func TestWriteLP(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer svr.Close()

	cli := &http.Client{Timeout: 1 * time.Second}
	ctx := context.Background()
	m := influx2lp.LPMetric{
		Measurement: "test-measurement",
		Tags:        map[string]string{"mytag1": "myvalue1", "mytag2": "myvalue2"},
		Fields:      map[string]interface{}{"field1": 1.23, "field2": 4, "field3": "abcABC"},
		Timestamp:   time.Now().UnixNano(),
	}

	t.Run("fail on missing org", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Bucket = "testbucket"
		_, _, err := WriteLP(ctx, cli, *c, m)
		if err == nil || err.Error() != "no org configured" {
			t.Errorf("expected 'no org configured' error, got %v", err)
		}
	})

	t.Run("fail on missing bucket", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Org = "testorg"
		_, _, err := WriteLP(ctx, cli, *c, m)
		if err == nil || err.Error() != "no bucket configured" {
			t.Errorf("expected 'no bucket configured' error, got %v", err)
		}
	})

	t.Run("verify good config", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Bucket = "testbucket"
		c.Org = "testorg"

		status, s, err := WriteLP(ctx, cli, *c, m)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if status != 204 {
			t.Errorf("expected status 204, got %d", status)
		}
		if s != "" {
			t.Errorf("expected empty response body, got %q", s)
		}
	})

	t.Run("url encoding", func(t *testing.T) {
		var gotQuery string
		paramSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotQuery = r.URL.RawQuery
			w.WriteHeader(204)
		}))
		defer paramSvr.Close()

		c := NewConfig()
		c.Host = paramSvr.URL
		c.Bucket = "my bucket"
		c.Org = "my&org"
		WriteLP(ctx, cli, *c, m)
		if gotQuery != "bucket=my+bucket&org=my%26org" {
			t.Errorf("unexpected query string: %q", gotQuery)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		c := NewConfig()
		c.Host = svr.URL
		c.Bucket = "testbucket"
		c.Org = "testorg"
		_, _, err := WriteLP(cctx, cli, *c, m)
		if err == nil {
			t.Error("expected error from cancelled context, got nil")
		}
	})
}
