package verify

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"time"
)

// rowHash: FNV-64a over length-prefixed canonical field encodings. Length
// prefixes prevent "ab"+"c" == "a"+"bc" ambiguities.
func rowHash(oids []uint32, vals []any) uint64 {
	h := fnv.New64a()
	var lenBuf [8]byte
	for i, v := range vals {
		s := canonical(oids[i], v)
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
		h.Write(lenBuf[:])
		h.Write([]byte(s))
	}
	return h.Sum64()
}

// canonical renders a decoded value identically whether it came from Postgres
// (via decode.ByOID) or from ClickHouse (via a driver scan).
func canonical(oid uint32, v any) string {
	switch x := v.(type) {
	case nil:
		return "\x00N"
	case bool:
		if x {
			return "t"
		}
		return "f"
	case uint8: // bool columns are UInt8 in the sink
		if x == 0 {
			return "f"
		}
		return "t"
	case time.Time:
		if oid == 1082 { // date: calendar day only, no zone arithmetic
			return x.Format("2006-01-02")
		}
		// Postgres has microsecond precision, the sink DateTime64(3): compare
		// at millisecond precision on both sides.
		return x.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case string:
		return x
	case []byte:
		return string(x)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	}
	return fmt.Sprintf("%v", v)
}

// chNormalize dereferences pointers (Nullable scans) and leaves concrete
// types for canonical().
func chNormalize(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		return chNormalize(rv.Elem().Interface())
	}
	return v
}
