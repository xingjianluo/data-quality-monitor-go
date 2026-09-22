package plan

import (
	"fmt"
	"time"
)

// parseWatermark 解析 RFC3339 水位线。
func parseWatermark(s string) (time.Time, error) {
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid watermark %q: %w", s, err)
	}
	return ts, nil
}
