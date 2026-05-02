package influx2lp

import (
	"fmt"
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
