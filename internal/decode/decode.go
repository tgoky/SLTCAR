// Package decode converts Postgres text-format column values (the format
// pgoutput uses by default, and what "::text" casts produce) into Go values.
// One decoder serves the WAL stream, the snapshot and the verifier, so all
// three paths agree by construction.
package decode

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/YOURNAME/slotcar/internal/metrics"
)

// Postgres prints timezone offsets as +00, +05:30 or +05:30:15 depending on
// the zone, so every form must be tried (longest first).
var pgTimeLayouts = []string{
	"2006-01-02 15:04:05.999999999-07:00:00",
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

func parsePGTime(s string) (time.Time, error) {
	var lastErr error
	for _, l := range pgTimeLayouts {
		t, err := time.Parse(l, s)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("unparsable timestamp %q: %w", s, lastErr)
}

// ByOID decodes a text-format value given its Postgres type OID.
// Unknown OIDs degrade to string (counted) rather than failing the stream.
// Dates/timestamps of 'infinity' / '-infinity' have no ClickHouse equivalent
// and become NULL.
func ByOID(oid uint32, b []byte) (any, error) {
	s := string(b)
	switch oid {
	case 16: // bool
		return s == "t", nil
	case 20, 21, 23: // int8, int2, int4
		return strconv.ParseInt(s, 10, 64)
	case 700, 701, 1700: // float4, float8, numeric (precision caveat in README)
		return strconv.ParseFloat(s, 64)
	case 18, 19, 25, 1043, 2950, 114, 3802, 17: // char,name,text,varchar,uuid,json,jsonb,bytea
		return s, nil
	case 1082, 1114, 1184: // date, timestamp, timestamptz
		if s == "infinity" || s == "-infinity" {
			return nil, nil
		}
		if strings.HasSuffix(s, " BC") {
			return nil, fmt.Errorf("BC timestamp %q not supported", s)
		}
		return parsePGTime(s)
	default:
		metrics.UnknownOIDTypes.Inc()
		return s, nil
	}
}
