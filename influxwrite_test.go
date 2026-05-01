package influx2lp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestString(t *testing.T) {
	now := time.Now().UnixNano()
	m := LPMetric{
		Measurement: "test-measurement",
		Tags:        map[string]string{"mytag1": "myvalue1", "mytag2": "myvalue2"},
		Fields:      map[string]interface{}{"field1": 1.23, "field2": 4, "field3": "abcABC"},
		Timestamp:   now,
	}

	t.Run("check result", func(t *testing.T) {
		want := fmt.Sprintf("test-measurement,mytag1=myvalue1,mytag2=myvalue2 field1=1.23,field2=4i,field3=\"abcABC\" %d", now)
		got := m.String()
		if want != got {
			t.Errorf("want %q\n got %q", want, got)
		}
	})

	t.Run("check tags", func(t *testing.T) {
		if len(m.Tags) != 2 {
			t.Errorf("wanted 2 tags, got %d", len(m.Tags))
		}
	})

	t.Run("check fields", func(t *testing.T) {
		if len(m.Fields) != 3 {
			t.Errorf("wanted 3 fields, got %d", len(m.Fields))
		}
	})

	t.Run("lp escaping", func(t *testing.T) {
		e := LPMetric{
			Measurement: "my,measurement",
			Tags:        map[string]string{"ta g": "val=ue"},
			Fields:      map[string]interface{}{"f": `say "hi" \o/`},
			Timestamp:   0,
		}
		want := `my\,measurement,ta\ g=val\=ue f="say \"hi\" \\o/" 0`
		got := e.String()
		if want != got {
			t.Errorf("want %q\n got %q", want, got)
		}
	})

	t.Run("bool fields", func(t *testing.T) {
		b := LPMetric{
			Measurement: "m",
			Fields:      map[string]interface{}{"ok": true, "fail": false},
			Timestamp:   0,
		}
		want := "m fail=false,ok=true 0"
		got := b.String()
		if want != got {
			t.Errorf("want %q\n got %q", want, got)
		}
	})

	t.Run("uint fields", func(t *testing.T) {
		u := LPMetric{
			Measurement: "m",
			Fields:      map[string]interface{}{"u": uint64(42)},
			Timestamp:   0,
		}
		want := "m u=42i 0"
		got := u.String()
		if want != got {
			t.Errorf("want %q\n got %q", want, got)
		}
	})
}

func TestWriteLP(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer svr.Close()

	cli := &http.Client{Timeout: 1 * time.Second}
	ctx := context.Background()
	now := time.Now().UnixNano()
	m := LPMetric{
		Measurement: "test-measurement",
		Tags:        map[string]string{"mytag1": "myvalue1", "mytag2": "myvalue2"},
		Fields:      map[string]interface{}{"field1": 1.23, "field2": 4, "field3": "abcABC"},
		Timestamp:   now,
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
