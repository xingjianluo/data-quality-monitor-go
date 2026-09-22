package rule

import (
	"context"
	"fmt"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

type completenessEval struct{ spec model.RuleSpec }

func (e completenessEval) Evaluate(ctx context.Context, ec EvalContext) (model.RuleResult, error) {
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

	total := len(ds.Records)
	nulls := 0
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return model.RuleResult{}, err
		}
		if err := maybeSleep(ctx, ec, spec.ID); err != nil {
			return model.RuleResult{}, err
		}
		if _, nonNull, _ := typedCell(col, rec[colName]); !nonNull {
			nulls++
		}
	}
	ratio := 1.0
	if total > 0 {
		ratio = float64(total-nulls) / float64(total)
	}
	if total == 0 || ratio < spec.MinNonNull {
		return failResult(spec, total, model.Finding{
			Reason:  model.ReasonRuleViolation,
			Column:  colName,
			Count:   nulls,
			Message: fmt.Sprintf("non-null ratio %.4f on %d row(s) is below required %.4f (%d null/missing)", ratio, total, spec.MinNonNull, nulls),
		}), nil
	}
	return passResult(spec, total), nil
}
