package rule

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

type uniquenessEval struct{ spec model.RuleSpec }

func (e uniquenessEval) Evaluate(ctx context.Context, ec EvalContext) (model.RuleResult, error) {
	spec := e.spec
	ds, res, ok := resolveDataset(spec, ec)
	if !ok {
		return res, nil
	}
	cols := make([]model.Column, 0, len(spec.Columns))
	for _, name := range spec.Columns {
		col, declared := ds.Schema.ColumnByName(name)
		if !declared {
			return errResult(spec, model.ReasonFieldMissing,
				fmt.Sprintf("column %q is not declared in dataset %q", name, ds.Name)), nil
		}
		cols = append(cols, col)
	}
	if res, blocked := structuralGate(spec, ds, spec.Columns); blocked {
		return res, nil
	}

	seen := make(map[string]int)
	dups := make(map[string]int) // 重复键 -> 额外出现次数
	total := len(ds.Records)
	for i, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return model.RuleResult{}, err
		}
		if err := maybeSleep(ctx, ec, spec.ID); err != nil {
			return model.RuleResult{}, err
		}
		// 任一键字段为 null/缺失时不参与唯一性比较（由完整性规则负责）。
		parts := make([]string, len(cols))
		usable := true
		for j, col := range cols {
			text, nonNull, _ := typedCell(col, rec[col.Name])
			if !nonNull {
				usable = false
				break
			}
			parts[j] = text
		}
		if !usable {
			continue
		}
		key := strings.Join(parts, "\x00")
		if _, exists := seen[key]; exists {
			dups[key]++
		} else {
			seen[key] = i
		}
	}
	if len(dups) > 0 {
		dupRows := 0
		keys := make([]string, 0, len(dups))
		for k, n := range dups {
			keys = append(keys, k)
			dupRows += n
		}
		sort.Strings(keys)
		limit := 3
		if len(keys) < limit {
			limit = len(keys)
		}
		return failResult(spec, total, model.Finding{
			Reason:  model.ReasonRuleViolation,
			Column:  joinCols(spec.Columns),
			Count:   dupRows,
			Message: fmt.Sprintf("%d duplicate row(s) for key [%s], examples: %v", dupRows, joinCols(spec.Columns), keys[:limit]),
		}), nil
	}
	return passResult(spec, total), nil
}
