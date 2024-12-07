package influx2lp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestString(t *testing.T) {
	now := time.Now().UnixNano()
	m := LPMetric{
		Measurement: "test-measurement",
		Tags:        map[string]interface{}{"mytag1": "myvalue1", "mytag2": "myvalue2"},
		Fields:      map[string]interface{}{"field1": 1.23, "field2": 4, "field3": "abcABC"},
		Timestamp:   now,
	}

	t.Run("check result", func(t *testing.T) {
		want := fmt.Sprintf("test-measurement,mytag1=myvalue1,mytag2=myvalue2 field1=1.230000,field2=4i,field3=\"abcABC\" %d", now)
		got := m.String()

		if !reflect.DeepEqual(want, got) {
			t.Errorf("Wanted %q but got %q", want, got)
		}
	})

	t.Run("check tags", func(t *testing.T) {
		wantNumTags := 2

		if len(m.Tags) != wantNumTags {
			t.Errorf("wanted %d tags, got %d tags", wantNumTags, len(m.Tags))
		}
	})

	t.Run("check fields", func(t *testing.T) {
		wantNumFields := 3

		if len(m.Fields) != wantNumFields {
			t.Errorf("wanted %d fields, got %d fields", wantNumFields, len(m.Tags))
		}
	})
}

func TestWriteLP(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	cli := http.Client{Timeout: 1 * time.Second}
	now := time.Now().UnixNano()
	m := LPMetric{
		Measurement: "test-measurement",
		Tags:        map[string]interface{}{"mytag1": "myvalue1", "mytag2": "myvalue2"},
		Fields:      map[string]interface{}{"field1": 1.23, "field2": 4, "field3": "abcABC"},
		Timestamp:   now,
	}

	t.Run("fail on missing org", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Bucket = "testbucket"
		_, _, err := WriteLP(cli, *c, m)
		if err.Error() != "no org configured" {
			t.Errorf("wanted error, got no error: %q", err.Error())
		}
	})

	t.Run("fail on missing bucket", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Org = "testorg"
		_, _, err := WriteLP(cli, *c, m)
		if err.Error() != "no bucket configured" {
			t.Errorf("wanted error, got no error: %q", err.Error())
		}
	})

	t.Run("verify good config", func(t *testing.T) {
		c := NewConfig()
		c.Host = svr.URL
		c.Bucket = "testbucket"
		c.Org = "testorg"

		status, s, err := WriteLP(cli, *c, m)
		if err != nil {
			t.Errorf("expected no error, got error %q", err)
		}
		if status != 204 {
			t.Errorf("expected status 204, got %d", status)
		}
		if s != "" {
			t.Errorf("expected no response, got %q", s)
		}
	})

}
