package rule

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// uniquenessChecker verifies that the (composite) key formed by the target
// fields is distinct across records. Missing/null/mistyped key parts are
// reported with their own distinguishable reasons rather than compared.
type uniquenessChecker struct{ baseChecker }

// Params:
//
//	ignore_nulls (bool, optional): skip records whose key is missing/null
//	                               (default false, i.e. nulls participate).
func (c *uniquenessChecker) Check(ctx context.Context, ds *model.Dataset, r model.Rule, evalTime time.Time) model.RuleResult {
	fields, ok := c.requireFields(r, 1)
	if !ok {
		return c.invalid(r, "uniqueness rule requires at least one field (use \"field\" or \"fields\")")
	}
	ignoreNulls := false
	if v, has := r.Params["ignore_nulls"]; has {
		b, ok := v.(bool)
		if !ok {
			return c.invalid(r, "ignore_nulls must be a boolean")
		}
		ignoreNulls = b
	}

	type occurrence struct {
		line int
	}
	keyLines := map[string][]occurrence{}
	var dupEvidence, structEvidence []model.Evidence
	seenStruct := map[string]bool{}

	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		var parts []string
		keyValid := true
		for _, name := range fields {
			view := resolveField(ds, rec, name)
			switch view.status {
			case fieldMissing, fieldNull:
				if view.issue != nil {
					key := evKey(rec.Line, name, view.issue.Detail)
					if !seenStruct[key] {
						seenStruct[key] = true
						structEvidence = append(structEvidence, model.Evidence{
							Line: rec.Line, Field: name, Detail: view.issue.Detail,
						})
					}
				}
				keyValid = false
			case fieldMistyped:
				key := evKey(rec.Line, name, view.issue.Detail)
				if !seenStruct[key] {
					seenStruct[key] = true
					structEvidence = append(structEvidence, model.Evidence{
						Line: rec.Line, Field: name, Detail: view.issue.Detail,
					})
				}
				keyValid = false
			case fieldOK:
				parts = append(parts, view.text)
			}
		}
		if !keyValid {
			// Null/missing keys optionally do not participate in duplication.
			if ignoreNulls && hasOnlyNullOrMissing(ds, rec, fields) {
				continue
			}
			continue
		}
		key := strings.Join(parts, "\x00")
		keyLines[key] = append(keyLines[key], occurrence{rec.Line})
	}

	for key, occ := range keyLines {
		if len(occ) < 2 {
			continue
		}
		lines := make([]int, 0, len(occ))
		for _, o := range occ {
			lines = append(lines, o.line)
		}
		sort.Ints(lines)
		dupEvidence = append(dupEvidence, model.Evidence{
			Detail: duplicateDetail(fields, key, lines),
		})
	}
	sort.Slice(dupEvidence, func(i, j int) bool { return dupEvidence[i].Detail < dupEvidence[j].Detail })
	sort.Slice(structEvidence, func(i, j int) bool {
		if structEvidence[i].Line != structEvidence[j].Line {
			return structEvidence[i].Line < structEvidence[j].Line
		}
		return structEvidence[i].Field < structEvidence[j].Field
	})

	var findings []model.Finding
	if len(structEvidence) > 0 {
		findings = append(findings, model.Finding{
			Code:     model.ReasonTypeMismatch, // may include missing/null below
			Evidence: structEvidence,
		})
	}
	if len(dupEvidence) > 0 {
		findings = append(findings, model.Finding{
			Code:     model.ReasonDuplicate,
			Evidence: dupEvidence,
		})
	}
	// Structural problems need distinct codes; split the mixed bucket.
	findings = splitStructural(findings)
	return c.newResult(r, findings)
}

func evKey(line int, field, detail string) string {
	return strconv.Itoa(line) + "\x00" + field + "\x00" + detail
}

func duplicateDetail(fields []string, key string, lines []int) string {
	var ls []string
	for _, l := range lines {
		ls = append(ls, strconv.Itoa(l))
	}
	values := strings.Split(key, "\x00")
	return "duplicate key [" + joinFields(fields) + "]=(" + strings.Join(values, ", ") +
		") appears on lines " + strings.Join(ls, ",")
}

func hasOnlyNullOrMissing(ds *model.Dataset, rec model.Record, fields []string) bool {
	for _, name := range fields {
		st := resolveField(ds, rec, name).status
		if st == fieldOK || st == fieldMistyped {
			return false
		}
	}
	return true
}

// splitStructural separates a finding bucket that may contain missing_field,
// null_value and type_mismatch evidence into one finding per code.
func splitStructural(in []model.Finding) []model.Finding {
	out := make([]model.Finding, 0, len(in))
	buckets := map[model.ReasonCode][]model.Evidence{}
	var order []model.ReasonCode
	for _, f := range in {
		if f.Code != model.ReasonTypeMismatch {
			out = append(out, f)
			continue
		}
		for _, e := range f.Evidence {
			code := model.ReasonTypeMismatch
			if strings.Contains(e.Detail, "is absent") {
				code = model.ReasonMissingField
			} else if strings.Contains(e.Detail, "is null") {
				code = model.ReasonNullValue
			}
			if _, ok := buckets[code]; !ok {
				order = append(order, code)
			}
			buckets[code] = append(buckets[code], e)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	for _, code := range order {
		out = append(out, model.Finding{Code: code, Evidence: buckets[code]})
	}
	return out
}
