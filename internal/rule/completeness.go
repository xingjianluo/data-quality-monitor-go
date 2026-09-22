package rule

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// completenessChecker verifies that target fields are present and non-null on
// every record. An optional "min_fill_rate" parameter (0..1) tolerates a
// fraction of missing/null values but the rule still fails when the observed
// fill rate is below the threshold, and every offending record is evidenced.
type completenessChecker struct{ baseChecker }

// Params:
//
//	min_fill_rate (float, optional): minimum fraction of records that must be
//	                                 present and non-null. Default 1.0.
func (c *completenessChecker) Check(ctx context.Context, ds *model.Dataset, r model.Rule, evalTime time.Time) model.RuleResult {
	fields, ok := c.requireFields(r, 1)
	if !ok {
		return c.invalid(r, "completeness rule requires at least one field (use \"field\" or \"fields\")")
	}
	minFill := 1.0
	if v, has := paramNumber(r, "min_fill_rate"); has {
		if v < 0 || v > 1 {
			return c.invalid(r, "min_fill_rate must be between 0 and 1")
		}
		minFill = v
	}

	total := len(ds.Records)
	// Keep findings grouped by code so output is stable.
	byCode := map[model.ReasonCode][]model.Evidence{}
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		for _, name := range fields {
			view := resolveField(ds, rec, name)
			switch view.status {
			case fieldMissing:
				byCode[model.ReasonMissingField] = append(byCode[model.ReasonMissingField], model.Evidence{
					Line: rec.Line, Field: name, Detail: view.issue.Detail,
				})
			case fieldNull:
				byCode[model.ReasonNullValue] = append(byCode[model.ReasonNullValue], model.Evidence{
					Line: rec.Line, Field: name, Detail: view.issue.Detail,
				})
			case fieldMistyped:
				// A present-but-mistyped value is still "filled"; report it
				// separately as type_mismatch, never silently counted as good.
				byCode[model.ReasonTypeMismatch] = append(byCode[model.ReasonTypeMismatch], model.Evidence{
					Line: rec.Line, Field: name, Detail: view.issue.Detail,
				})
			}
		}
	}

	filled := 0
	for _, rec := range ds.Records {
		allFilled := true
		for _, name := range fields {
			switch resolveField(ds, rec, name).status {
			case fieldMissing, fieldNull:
				allFilled = false
			}
		}
		if allFilled {
			filled++
		}
	}

	var findings []model.Finding
	for _, code := range []model.ReasonCode{
		model.ReasonMissingField,
		model.ReasonNullValue,
		model.ReasonTypeMismatch,
	} {
		ev := byCode[code]
		if len(ev) == 0 {
			continue
		}
		sort.Slice(ev, func(i, j int) bool {
			if ev[i].Line != ev[j].Line {
				return ev[i].Line < ev[j].Line
			}
			if ev[i].Field != ev[j].Field {
				return ev[i].Field < ev[j].Field
			}
			return ev[i].Detail < ev[j].Detail
		})
		findings = append(findings, model.Finding{
			Code:     code,
			Evidence: dedupeEvidence(ev),
		})
	}

	fillRate := 1.0
	if total > 0 {
		fillRate = float64(filled) / float64(total)
	}
	res := c.newResult(r, findings)
	// Threshold breach is itself an explicit finding with the basis. Its code
	// follows the dominant gap reason so null-only gaps are null_value rather
	// than misreported as missing_field.
	if fillRate+1e-12 < minFill {
		res.Status = model.StatusFailed
		summaryCode := model.ReasonMissingField
		if len(byCode[model.ReasonMissingField]) == 0 {
			summaryCode = model.ReasonNullValue
		}
		res.Findings = append(res.Findings, model.Finding{
			Code: summaryCode,
			Evidence: []model.Evidence{{
				Detail: fmt.Sprintf("fill rate %.4f for fields [%s] is below min_fill_rate %.4f (filled %d/%d records)",
					fillRate, joinFields(fields), minFill, filled, total),
			}},
		})
	}
	if res.Status == model.StatusPassed {
		res.Findings = []model.Finding{}
	}
	return res
}
