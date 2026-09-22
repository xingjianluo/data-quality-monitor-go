package monitor

import (
	"context"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

func TestAlertsDeterministicAndNormalized(t *testing.T) {
	rec := state.BatchRecord{
		BatchID: "b", Ordinal: 2,
		Results: []model.RuleResult{
			{RuleID: "s", RuleType: model.RuleStructural, Dataset: "d", Status: model.StatusError,
				Findings: []model.Finding{
					{Reason: model.ReasonSchemaDrift, Message: "m1"},
					{Reason: model.ReasonFieldMissing, Column: "c", Message: "m2"},
				}},
			{RuleID: "p", RuleType: model.RuleCompleteness, Dataset: "d", Status: model.StatusPass},
			{RuleID: "f", RuleType: model.RuleFreshness, Dataset: "d", Status: model.StatusFail,
				Findings: []model.Finding{{Reason: model.ReasonRuleViolation, Count: 3, Message: "m3"}}},
		},
	}
	m := New(nil)
	a1 := m.Alerts(rec)
	a2 := m.Alerts(rec)
	if len(a1) != 3 {
		t.Fatalf("want 3 alerts (pass excluded), got %d", len(a1))
	}
	for i := range a1 {
		if a1[i] != a2[i] {
			t.Fatalf("alerts not deterministic at %d", i)
		}
	}
	if a1[0].BatchID != "b" || a1[0].Ordinal != 2 || a1[0].Reason != model.ReasonSchemaDrift {
		t.Fatalf("alert normalization wrong: %+v", a1[0])
	}
	if a1[2].Message != "m3" {
		t.Fatal("alert must carry the normalized message")
	}
	if a1[0].Severity != "error" {
		t.Fatal("default severity should be error")
	}
}

func TestMemorySinkConcurrentSafe(t *testing.T) {
	sink := NewMemorySink()
	ctx := context.Background()
	done := make(chan struct{}, 16)
	for i := 0; i < 16; i++ {
		go func(i int) {
			_ = sink.Send(ctx, []model.Alert{{BatchID: "b", RuleID: "r", Reason: model.ReasonRuleViolation}})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 16; i++ {
		<-done
	}
	if got := len(sink.Snapshot()); got != 16 {
		t.Fatalf("want 16 alerts, got %d", got)
	}
}
