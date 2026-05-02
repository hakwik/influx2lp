package influx2lp

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// LPMetric holds the data for a single InfluxDB line protocol measurement.
// Tags must be strings; Fields support int*, uint*, float*, string, and bool.
type LPMetric struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   int64 // Unix nanoseconds
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
