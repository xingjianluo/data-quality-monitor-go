package model

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// AsInt converts a decoded value to int64. JSON numbers arrive as json.Number
// (when decoded with UseNumber); CSV values arrive as strings and are parsed.
// Float values are accepted only when they are integral.
func AsInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		if x == math.Trunc(x) && !math.IsInf(x, 0) {
			return int64(x), true
		}
		return 0, false
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n, true
		}
		if f, err := x.Float64(); err == nil && f == math.Trunc(f) && !math.IsInf(f, 0) {
			return int64(f), true
		}
		return 0, false
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
			return n, true
		}
		return 0, false
	}
	return 0, false
}

// AsFloat converts a decoded value to float64.
func AsFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return x, true
	case json.Number:
		if f, err := x.Float64(); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return f, true
		}
		return 0, false
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return f, true
		}
		return 0, false
	}
	return 0, false
}

// AsBool converts a decoded value to bool. Strings "true"/"false" (after
// trimming) are accepted; other strings are not.
func AsBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true":
			return true, true
		case "false":
			return false, true
		}
	}
	return false, false
}

// AsTime converts a decoded value to time.Time using the given layout. Time
// values already converted by the loader pass through unchanged.
func AsTime(v any, format string) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		return x, !x.IsZero()
	case string:
		if format == "" {
			format = time.RFC3339
		}
		if t, err := time.Parse(format, strings.TrimSpace(x)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// AsString returns the canonical string form of a decoded scalar.
func AsString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case bool:
		return strconv.FormatBool(x), true
	case time.Time:
		return x.Format(time.RFC3339), true
	}
	return "", false
}
