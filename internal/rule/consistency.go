package rule

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// consistencyChecker validates field values against declared value domains:
//
//   - "allowed": value must be member of params.allowed (string set);
//   - "range":   numeric value must satisfy min <= v <= max
//     (params.min / params.max, either side optional);
//   - "regex":   string value must match params.pattern.
//
// Missing/null/mistyped inputs keep their own distinguishable reasons and are
// never evaluated against the domain (nothing passes silently).
type consistencyChecker struct {
	baseChecker
	regexCache map[string]*regexp.Regexp
}

func (c *consistencyChecker) Check(ctx context.Context, ds *model.Dataset, r model.Rule, evalTime time.Time) model.RuleResult {
	name, ok := c.requireOneField(r)
	if !ok {
		return c.invalid(r, "consistency rule requires exactly one \"field\"")
	}
	mode, ok := paramString(r, "mode")
	if !ok {
		return c.invalid(r, "consistency rule requires string param \"mode\" (allowed|range|regex)")
	}
	switch mode {
	case "allowed":
		return c.checkAllowed(ctx, ds, r, name)
	case "range":
		return c.checkRange(ctx, ds, r, name)
	case "regex":
		return c.checkRegex(ctx, ds, r, name)
	default:
		return c.invalid(r, fmt.Sprintf("unsupported consistency mode %q", mode))
	}
}

func (c *consistencyChecker) structuralEvidence(ds *model.Dataset, records []model.Record, name string) ([]model.Finding, int) {
	buckets := map[model.ReasonCode][]model.Evidence{}
	skipped := 0
	seen := map[string]bool{}
	for _, rec := range records {
		view := resolveField(ds, rec, name)
		if view.status == fieldOK {
			continue
		}
		skipped++
		if view.issue == nil {
			continue
		}
		key := strconv.Itoa(rec.Line) + "\x00" + view.issue.Detail
		if seen[key] {
			continue
		}
		seen[key] = true
		buckets[view.issue.Code] = append(buckets[view.issue.Code], model.Evidence{
			Line: rec.Line, Field: name, Detail: view.issue.Detail,
		})
	}
	var out []model.Finding
	codes := []model.ReasonCode{model.ReasonMissingField, model.ReasonNullValue, model.ReasonTypeMismatch}
	for _, code := range codes {
		ev := buckets[code]
		if len(ev) == 0 {
			continue
		}
		sort.Slice(ev, func(i, j int) bool { return ev[i].Line < ev[j].Line })
		out = append(out, model.Finding{Code: code, Evidence: ev})
	}
	return out, skipped
}

func (c *consistencyChecker) checkAllowed(ctx context.Context, ds *model.Dataset, r model.Rule, name string) model.RuleResult {
	raw, ok := r.Params["allowed"]
	if !ok {
		return c.invalid(r, "consistency mode=allowed requires array param \"allowed\"")
	}
	arr, ok := raw.([]any)
	if !ok || len(arr) == 0 {
		return c.invalid(r, "consistency mode=allowed requires a non-empty \"allowed\" array")
	}
	allowed := make(map[string]bool, len(arr))
	var allowedList []string
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return c.invalid(r, "\"allowed\" entries must be strings")
		}
		if !allowed[s] {
			allowed[s] = true
			allowedList = append(allowedList, s)
		}
	}
	sort.Strings(allowedList)

	findings, _ := c.structuralEvidence(ds, ds.Records, name)
	var bad []model.Evidence
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		view := resolveField(ds, rec, name)
		if view.status != fieldOK {
			continue
		}
		s, isString := view.raw.(string)
		if !isString || !allowed[s] {
			bad = append(bad, model.Evidence{
				Line:   rec.Line,
				Field:  name,
				Detail: fmt.Sprintf("value %q is not in allowed set [%s]", view.text, joinQuoted(allowedList)),
			})
		}
	}
	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool { return bad[i].Line < bad[j].Line })
		findings = append(findings, model.Finding{Code: model.ReasonValueOutsideSet, Evidence: bad})
	}
	return c.newResult(r, findings)
}

func (c *consistencyChecker) checkRange(ctx context.Context, ds *model.Dataset, r model.Rule, name string) model.RuleResult {
	minV, hasMin := paramNumber(r, "min")
	maxV, hasMax := paramNumber(r, "max")
	if !hasMin && !hasMax {
		return c.invalid(r, "consistency mode=range requires at least one of \"min\"/\"max\"")
	}
	if hasMin && hasMax && minV > maxV {
		return c.invalid(r, "consistency mode=range: min must be <= max")
	}
	fd, _ := findDecl(ds, name)
	if fd.Type != model.TypeInt && fd.Type != model.TypeFloat {
		return c.invalid(r, "consistency mode=range requires a numeric field")
	}

	findings, _ := c.structuralEvidence(ds, ds.Records, name)
	var bad []model.Evidence
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		view := resolveField(ds, rec, name)
		if view.status != fieldOK {
			continue
		}
		f, _ := model.AsFloat(view.raw)
		if (hasMin && f < minV) || (hasMax && f > maxV) {
			bad = append(bad, model.Evidence{
				Line:  rec.Line,
				Field: name,
				Detail: fmt.Sprintf("value %s is outside range [%s, %s]",
					view.text, numBound(minV, hasMin), numBound(maxV, hasMax)),
			})
		}
	}
	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool { return bad[i].Line < bad[j].Line })
		findings = append(findings, model.Finding{Code: model.ReasonValueRange, Evidence: bad})
	}
	return c.newResult(r, findings)
}

func (c *consistencyChecker) checkRegex(ctx context.Context, ds *model.Dataset, r model.Rule, name string) model.RuleResult {
	pattern, ok := paramString(r, "pattern")
	if !ok {
		return c.invalid(r, "consistency mode=regex requires string param \"pattern\"")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return c.invalid(r, fmt.Sprintf("invalid regex pattern: %v", err))
	}
	fd, _ := findDecl(ds, name)
	if fd.Type != model.TypeString {
		return c.invalid(r, "consistency mode=regex requires a string field")
	}

	findings, _ := c.structuralEvidence(ds, ds.Records, name)
	var bad []model.Evidence
	for _, rec := range ds.Records {
		if err := ctx.Err(); err != nil {
			return c.invalid(r, "evaluation cancelled")
		}
		view := resolveField(ds, rec, name)
		if view.status != fieldOK {
			continue
		}
		s := view.raw.(string)
		if !re.MatchString(s) {
			bad = append(bad, model.Evidence{
				Line:   rec.Line,
				Field:  name,
				Detail: fmt.Sprintf("value %q does not match pattern %q", s, pattern),
			})
		}
	}
	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool { return bad[i].Line < bad[j].Line })
		findings = append(findings, model.Finding{Code: model.ReasonRegexMismatch, Evidence: bad})
	}
	return c.newResult(r, findings)
}

func joinQuoted(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ", "
		}
		out += strconv.Quote(s)
	}
	return out
}

func numBound(v float64, set bool) string {
	if !set {
		return "*"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
