// Package rule implements the four rule families: completeness, uniqueness,
// consistency and freshness.
//
// Every checker returns a normalized model.RuleResult; structural data problems
// (missing fields, nulls, type mismatches) are surfaced as findings with a
// stable ReasonCode rather than causing raw errors or silently passing.
package rule

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Checker evaluates one rule against one dataset. EvalTime is injected so
// freshness verdicts are deterministic and reproducible.
type Checker interface {
	Check(ctx context.Context, ds *model.Dataset, r model.Rule, evalTime time.Time) model.RuleResult
}

// baseChecker carries shared result construction helpers.
type baseChecker struct{}

func (baseChecker) newResult(r model.Rule, findings []model.Finding) model.RuleResult {
	if findings == nil {
		findings = []model.Finding{}
	}
	status := model.StatusPassed
	if len(findings) > 0 {
		status = model.StatusFailed
	}
	return model.RuleResult{
		RuleID:   r.ID,
		Dataset:  r.Dataset,
		Kind:     r.Kind,
		Severity: r.Severity,
		Status:   status,
		Findings: findings,
	}
}

func (baseChecker) invalid(r model.Rule, msg string) model.RuleResult {
	return model.RuleResult{
		RuleID:    r.ID,
		Dataset:   r.Dataset,
		Kind:      r.Kind,
		Severity:  r.Severity,
		Status:    model.StatusErrored,
		ErrorCode: model.ReasonRuleInvalid,
		ErrorMsg:  msg,
		Findings:  []model.Finding{},
	}
}

// requireOneField validates that the rule declares exactly one field.
func (baseChecker) requireOneField(r model.Rule) (string, bool) {
	fields := targetFields(r)
	if len(fields) != 1 || fields[0] == "" {
		return "", false
	}
	return fields[0], true
}

func (baseChecker) requireFields(r model.Rule, min int) ([]string, bool) {
	fields := targetFields(r)
	if len(fields) < min {
		return nil, false
	}
	return fields, true
}

// paramString reads a string parameter.
func paramString(r model.Rule, key string) (string, bool) {
	v, ok := r.Params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok && s != ""
}

// paramNumber reads a numeric parameter (JSON numbers decode to float64).
func paramNumber(r model.Rule, key string) (float64, bool) {
	v, ok := r.Params[key]
	if !ok {
		return 0, false
	}
	f, ok := model.AsFloat(v)
	return f, ok
}

func errInvalid(format string, args ...any) error {
	return &model.RuleError{Code: model.ReasonRuleInvalid, Message: fmt.Sprintf(format, args...)}
}

// NewChecker builds the checker for a rule kind and validates the declaration.
func NewChecker(kind model.RuleKind) (Checker, error) {
	switch kind {
	case model.KindCompleteness:
		return &completenessChecker{baseChecker{}}, nil
	case model.KindUniqueness:
		return &uniquenessChecker{baseChecker{}}, nil
	case model.KindConsistency:
		return &consistencyChecker{baseChecker: baseChecker{}, regexCache: map[string]*regexp.Regexp{}}, nil
	case model.KindFreshness:
		return &freshnessChecker{baseChecker{}}, nil
	default:
		return nil, errInvalid("unknown rule kind %q", kind)
	}
}
