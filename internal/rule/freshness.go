package rule

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// freshnessChecker verifies that each record's timestamp field is no older than
// max_age_seconds relative to the batch evaluation time. Evaluation time is
// injected by the engine, so verdicts are deterministic and reproducible.
//
// Params:
//
//	max_age_seconds (number, required): maximum permitted age.
type freshnessChecker struct{ baseChecker }

func (c *freshnessChecker) Check(ctx context.Context, ds *model.Dataset, r model.Rule, evalTime time.Time) model.RuleResult {
	name, ok := c.requireOneField(r)
	if !ok {
		return c.invalid(r, "freshness rule requires exactly one \"field\"")
	}
	fd, exists := findDecl(ds, name)
	if !exists || fd.Type != model.TypeTime {
		return c.invalid(r, "freshness rule requires a declared time field")
	}
	maxAgeSeconds, ok := paramNumber(r, "max_age_seconds")
	if !ok || maxAgeSeconds < 0 {
		return c.invalid(r, "freshness rule requires non-negative numeric param \"max_age_seconds\"")
	}
	maxAge := time.Duration(maxAgeSeconds * float64(time.Second))

	var stale, structural []model.Evidence
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		view := resolveField(ds, rec, name)
		switch view.status {
		case fieldMissing, fieldNull, fieldMistyped:
			if view.issue != nil {
				structural = append(structural, model.Evidence{
					Line: rec.Line, Field: name, Detail: view.issue.Detail,
				})
			}
			continue
		}
		t, _ := view.raw.(time.Time)
		age := evalTime.Sub(t)
		if age > maxAge {
			stale = append(stale, model.Evidence{
				Line: rec.Line, Field: name,
				Detail: fmt.Sprintf("timestamp %s is %s old at evaluation time %s, exceeding max age %s",
					t.Format(fd.Format), age.Truncate(time.Second),
					evalTime.Format(fd.Format), maxAge),
			})
		}
	}
	sort.Slice(structural, func(i, j int) bool { return structural[i].Line < structural[j].Line })
	sort.Slice(stale, func(i, j int) bool { return stale[i].Line < stale[j].Line })

	var findings []model.Finding
	structFindings := splitStructural([]model.Finding{{Code: model.ReasonTypeMismatch, Evidence: structural}})
	findings = append(findings, structFindings...)
	if len(stale) > 0 {
		findings = append(findings, model.Finding{Code: model.ReasonStaleRecord, Evidence: stale})
	}
	return c.newResult(r, findings)
}
