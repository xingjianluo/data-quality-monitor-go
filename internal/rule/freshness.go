package rule

import (
	"context"
	"fmt"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

type freshnessEval struct{ spec model.RuleSpec }

func (e freshnessEval) Evaluate(ctx context.Context, ec EvalContext) (model.RuleResult, error) {
	spec := e.spec
	ds, res, ok := resolveDataset(spec, ec)
	if !ok {
		return res, nil
	}
	colName := spec.Columns[0]
	col, declared := ds.Schema.ColumnByName(colName)
	if !declared {
		return errResult(spec, model.ReasonFieldMissing,
			fmt.Sprintf("column %q is not declared in dataset %q", colName, ds.Name)), nil
	}
	if res, blocked := structuralGate(spec, ds, []string{colName}); blocked {
		return res, nil
	}
	maxAge, err := time.ParseDuration(spec.MaxAge)
	if err != nil {
		return errResult(spec, model.ReasonInvalidRule,
			fmt.Sprintf("invalid max_age %q: %v", spec.MaxAge, err)), nil
	}

	threshold := ec.Watermark.Add(-maxAge)
	stale := 0
	var oldest time.Time
	total := len(ds.Records)
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return model.RuleResult{}, err
		}
		if err := maybeSleep(ctx, ec, spec.ID); err != nil {
			return model.RuleResult{}, err
		}
		text, nonNull, _ := typedCell(col, rec[colName])
		if !nonNull {
			continue
		}
		ts, perr := time.Parse(time.RFC3339, text)
		if perr != nil {
			// 结构门已保证类型匹配，理论上不可达；归一化为内部错误而非透出。
			return errResult(spec, model.ReasonInternal, "unparseable timestamp after type check"), nil
		}
		if oldest.IsZero() || ts.Before(oldest) {
			oldest = ts
		}
		if ts.Before(threshold) {
			stale++
		}
	}
	if stale > 0 {
		return failResult(spec, total, model.Finding{
			Reason:  model.ReasonRuleViolation,
			Column:  colName,
			Count:   stale,
			Message: fmt.Sprintf("%d row(s) older than %s before watermark %s; oldest=%s", stale, maxAge, ec.Watermark.UTC().Format(time.RFC3339), oldest.UTC().Format(time.RFC3339)),
		}), nil
	}
	return passResult(spec, total), nil
}
