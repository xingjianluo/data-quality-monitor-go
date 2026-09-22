package rule

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func evalAt() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }

func schema() model.Schema {
	return model.Schema{Fields: []model.FieldDecl{
		{Name: "id", Type: model.TypeInt, Required: true},
		{Name: "name", Type: model.TypeString, Required: true},
		{Name: "status", Type: model.TypeString, Required: true},
		{Name: "age", Type: model.TypeInt, Required: false},
		{Name: "ts", Type: model.TypeTime, Format: "2006-01-02", Required: true},
	}}
}

func dsWith(records ...model.Record) *model.Dataset {
	return &model.Dataset{Name: "d", Schema: schema(), Records: records}
}

func ruleFor(kind model.RuleKind, field string, params map[string]any) model.Rule {
	return model.Rule{
		ID: "r-" + string(kind) + "-" + field, Dataset: "d", Kind: kind,
		Severity: model.SeverityError, Field: field, Params: params,
	}
}

// codes collects the distinct finding codes of a result in their returned order.
func codes(rr model.RuleResult) []model.ReasonCode {
	var out []model.ReasonCode
	for _, f := range rr.Findings {
		out = append(out, f.Code)
	}
	return out
}

func hasCode(rr model.RuleResult, want model.ReasonCode) bool {
	for _, c := range codes(rr) {
		if c == want {
			return true
		}
	}
	return false
}

func TestCompletenessDistinguishesMissingNullAndMistype(t *testing.T) {
	ds := dsWith(
		// line 2: all good
		model.Record{Line: 2, Values: map[string]any{"age": int64(30)}, Present: map[string]bool{"age": true}},
		// line 3: optional age explicitly null
		model.Record{Line: 3, Values: map[string]any{"age": nil}, Present: map[string]bool{"age": true},
			FieldIssues: []model.FieldIssue{{Field: "age", Code: model.ReasonNullValue, Detail: "line 3: null"}}},
		// line 4: required id absent; optional age also absent but that is a
		// null (three-valued logic), not a missing_field for age.
		model.Record{Line: 4, Values: map[string]any{}, Present: map[string]bool{},
			FieldIssues: []model.FieldIssue{{Field: "id", Code: model.ReasonMissingField, Detail: "line 4: field \"id\" is absent"}}},
		// line 5: age present but wrong type
		model.Record{Line: 5, Values: map[string]any{"age": "oops"}, Present: map[string]bool{"age": true},
			FieldIssues: []model.FieldIssue{{Field: "age", Code: model.ReasonTypeMismatch, Detail: "line 5: not int"}}},
	)
	chk, _ := NewChecker(model.KindCompleteness)
	rr := chk.Check(context.Background(), ds, ruleFor(model.KindCompleteness, "age", nil), evalAt())
	if rr.Status != model.StatusFailed {
		t.Fatalf("want failed, got %s", rr.Status)
	}
	if !hasCode(rr, model.ReasonNullValue) || !hasCode(rr, model.ReasonTypeMismatch) {
		t.Fatalf("want null_value+type_mismatch, got %v", codes(rr))
	}
	if hasCode(rr, model.ReasonMissingField) {
		t.Fatalf("optional missing age must not be missing_field; got %v", codes(rr))
	}

	rrID := chk.Check(context.Background(), ds, ruleFor(model.KindCompleteness, "id", nil), evalAt())
	if !hasCode(rrID, model.ReasonMissingField) {
		t.Fatalf("want missing_field for absent required id, got %v", codes(rrID))
	}
}

func TestUniquenessDuplicatesAndStructural(t *testing.T) {
	ds := dsWith(
		model.Record{Line: 2, Values: map[string]any{"id": int64(1)}, Present: map[string]bool{"id": true}},
		model.Record{Line: 3, Values: map[string]any{"id": int64(1)}, Present: map[string]bool{"id": true}},
		model.Record{Line: 4, Values: map[string]any{"id": int64(2)}, Present: map[string]bool{"id": true}},
		model.Record{Line: 5, Values: map[string]any{"id": "bad"}, Present: map[string]bool{"id": true},
			FieldIssues: []model.FieldIssue{{Field: "id", Code: model.ReasonTypeMismatch, Detail: "line 5: bad"}}},
	)
	chk, _ := NewChecker(model.KindUniqueness)
	rr := chk.Check(context.Background(), ds, ruleFor(model.KindUniqueness, "id", nil), evalAt())
	if !hasCode(rr, model.ReasonDuplicate) {
		t.Fatalf("want duplicate_value, got %v", codes(rr))
	}
	if !hasCode(rr, model.ReasonTypeMismatch) {
		t.Fatalf("want type_mismatch, got %v", codes(rr))
	}
	// Evidence of the duplicate must cite both lines deterministically.
	for _, f := range rr.Findings {
		if f.Code == model.ReasonDuplicate {
			if len(f.Evidence) != 1 || f.Evidence[0].Detail == "" {
				t.Fatalf("bad duplicate evidence: %+v", f.Evidence)
			}
		}
	}
}

func TestConsistencyModes(t *testing.T) {
	ds := dsWith(
		model.Record{Line: 2, Values: map[string]any{"status": "active", "age": int64(30), "name": "ok"}, Present: map[string]bool{"status": true, "age": true, "name": true}},
		model.Record{Line: 3, Values: map[string]any{"status": "banned", "age": int64(999), "name": "bad1"}, Present: map[string]bool{"status": true, "age": true, "name": true}},
		model.Record{Line: 4, Values: map[string]any{"status": "active", "age": int64(-1), "name": "x"}, Present: map[string]bool{"status": true, "age": true, "name": true}},
	)
	chk, _ := NewChecker(model.KindConsistency)

	rrAllowed := chk.Check(context.Background(), ds,
		ruleFor(model.KindConsistency, "status", map[string]any{"mode": "allowed", "allowed": []any{"active", "pending"}}), evalAt())
	if !hasCode(rrAllowed, model.ReasonValueOutsideSet) || rrAllowed.Status != model.StatusFailed {
		t.Fatalf("allowed: %v %s", codes(rrAllowed), rrAllowed.Status)
	}

	rrRange := chk.Check(context.Background(), ds,
		ruleFor(model.KindConsistency, "age", map[string]any{"mode": "range", "min": 0.0, "max": 150.0}), evalAt())
	if !hasCode(rrRange, model.ReasonValueRange) {
		t.Fatalf("range: %v", codes(rrRange))
	}

	rrRegex := chk.Check(context.Background(), ds,
		ruleFor(model.KindConsistency, "name", map[string]any{"mode": "regex", "pattern": `^ok$`}), evalAt())
	if !hasCode(rrRegex, model.ReasonRegexMismatch) {
		t.Fatalf("regex: %v", codes(rrRegex))
	}

	// Invalid declarations must be classifiable errors, never silent passes.
	for _, tc := range []map[string]any{
		nil,
		{"mode": "unknown"},
		{"mode": "allowed"},
		{"mode": "range", "min": 10.0, "max": 1.0},
		{"mode": "regex", "pattern": "("},
	} {
		rr := chk.Check(context.Background(), ds, ruleFor(model.KindConsistency, "status", tc), evalAt())
		if rr.Status != model.StatusErrored || rr.ErrorCode != model.ReasonRuleInvalid {
			t.Fatalf("invalid params %v: want errored/rule_definition_invalid, got %s/%s", tc, rr.Status, rr.ErrorCode)
		}
	}
}

func TestFreshnessDeterministic(t *testing.T) {
	old, _ := time.Parse("2006-01-02", "2026-08-01")
	fresh, _ := time.Parse("2006-01-02", "2026-09-23")
	ds := dsWith(
		model.Record{Line: 2, Values: map[string]any{"ts": fresh}, Present: map[string]bool{"ts": true}},
		model.Record{Line: 3, Values: map[string]any{"ts": old}, Present: map[string]bool{"ts": true}},
		model.Record{Line: 4, Values: map[string]any{"ts": nil}, Present: map[string]bool{"ts": true},
			FieldIssues: []model.FieldIssue{{Field: "ts", Code: model.ReasonNullValue, Detail: "line 4: field \"ts\" is null"}}},
	)
	chk, _ := NewChecker(model.KindFreshness)
	r := ruleFor(model.KindFreshness, "ts", map[string]any{"max_age_seconds": 86400.0})
	rr1 := chk.Check(context.Background(), ds, r, evalAt())
	rr2 := chk.Check(context.Background(), ds, r, evalAt())
	if !hasCode(rr1, model.ReasonStaleRecord) || !hasCode(rr1, model.ReasonNullValue) {
		t.Fatalf("freshness: %v", codes(rr1))
	}
	if len(rr1.Findings) != len(rr2.Findings) {
		t.Fatal("freshness verdict not reproducible for same eval time")
	}
	// A later evaluation time must be able to flip fresh->stale deterministically.
	later := evalAt().Add(48 * time.Hour)
	rrLater := chk.Check(context.Background(), ds, r, later)
	staleCount := 0
	for _, f := range rrLater.Findings {
		if f.Code == model.ReasonStaleRecord {
			staleCount += len(f.Evidence)
		}
	}
	if staleCount != 2 {
		t.Fatalf("want 2 stale records at later eval time, got %d", staleCount)
	}
}

func TestContextCancellationNormalized(t *testing.T) {
	ds := dsWith(
		model.Record{Line: 2, Values: map[string]any{"id": int64(1)}, Present: map[string]bool{"id": true}},
	)
	chk, _ := NewChecker(model.KindCompleteness)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rr := chk.Check(ctx, ds, ruleFor(model.KindCompleteness, "id", nil), evalAt())
	// Checker reports a definition-style error on ctx; the engine normalizes
	// cancellation to rule_cancelled at the orchestration boundary.
	if rr.Status != model.StatusErrored {
		t.Fatalf("want errored on cancelled context, got %s", rr.Status)
	}
}
