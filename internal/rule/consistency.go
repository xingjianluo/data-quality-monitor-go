package rule

import (
	"context"
	"fmt"
	"sort"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

type consistencyEval struct{ spec model.RuleSpec }

func (e consistencyEval) Evaluate(ctx context.Context, ec EvalContext) (model.RuleResult, error) {
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

	allowed := make(map[string]bool, len(spec.Allowed))
	for _, v := range spec.Allowed {
		allowed[v] = true
	}
	bad := make(map[string]int)
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
			continue // 空值由完整性规则负责
		}
		if !allowed[text] {
			bad[text]++
		}
	}
	if len(bad) > 0 {
		values := make([]string, 0, len(bad))
		count := 0
		for v, n := range bad {
			values = append(values, v)
			count += n
		}
		sort.Strings(values)
		limit := 5
		if len(values) < limit {
			limit = len(values)
		}
		return failResult(spec, total, model.Finding{
			Reason:  model.ReasonRuleViolation,
			Column:  colName,
			Count:   count,
			Message: fmt.Sprintf("%d value(s) outside allowed set, examples: %v", count, values[:limit]),
		}), nil
	}
	return passResult(spec, total), nil
}
