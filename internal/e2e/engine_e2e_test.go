package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// This end-to-end test drives the shipped example configuration: two datasets
// (CSV + JSONL), eight rules of all four kinds, concurrent evaluation under
// -race, deterministic replay and batch-ordering enforcement.
func TestExampleEndToEnd(t *testing.T) {
	cfg, err := config.Load(filepath.Join(repoRoot(t), "examples", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := state.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	eng := engine.New(cfg, datasource.New(""), store, nil, engine.Options{
		MaxConcurrentRules: 8,
		RuleTimeout:        5 * time.Second,
		EvalTime:           time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
	})
	ctx := context.Background()

	b1, err := eng.Run(ctx, "20260923-001", false)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus := map[string]model.RuleStatus{
		"users-id-complete":    model.StatusPassed,
		"users-id-unique":      model.StatusFailed,
		"users-status-domain":  model.StatusFailed,
		"users-age-range":      model.StatusFailed,
		"users-signup-fresh":   model.StatusPassed,
		"events-user-complete": model.StatusFailed,
		"events-key-unique":    model.StatusPassed,
		"events-ts-fresh":      model.StatusFailed,
	}
	wantCode := map[string]model.ReasonCode{
		"users-id-unique":      model.ReasonDuplicate,
		"users-status-domain":  model.ReasonValueOutsideSet,
		"users-age-range":      model.ReasonTypeMismatch,
		"events-user-complete": model.ReasonNullValue,
		"events-ts-fresh":      model.ReasonStaleRecord,
	}
	if len(b1.Results) != len(wantStatus) {
		t.Fatalf("want %d results, got %d", len(wantStatus), len(b1.Results))
	}
	for _, rr := range b1.Results {
		if rr.Status != wantStatus[rr.RuleID] {
			t.Fatalf("rule %s: want %s got %s", rr.RuleID, wantStatus[rr.RuleID], rr.Status)
		}
		if code, ok := wantCode[rr.RuleID]; ok {
			found := false
			for _, f := range rr.Findings {
				if f.Code == code {
					found = true
				}
			}
			if !found {
				t.Fatalf("rule %s: want finding code %s, got %+v", rr.RuleID, code, rr.Findings)
			}
		}
	}

	// Repeat the exact same batch: fingerprint and all verdicts reproduce.
	b2, err := eng.Run(ctx, "20260923-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Status != "replay" || b2.Fingerprint != b1.Fingerprint {
		t.Fatalf("replay not reproducible: status=%s fp1=%s fp2=%s", b2.Status, b1.Fingerprint, b2.Fingerprint)
	}

	// Late arrival of an older batch is rejected, never silently applied.
	if _, err := eng.Run(ctx, "20260922-999", false); err == nil {
		t.Fatal("out-of-order older batch must be rejected")
	}

	// Historical conclusions remain retrievable by batch id.
	st, err := store.Get(ctx, "20260923-001")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != state.StatusCommitted || len(st.Results) != len(wantStatus) {
		t.Fatalf("stored history mismatch: %+v", st)
	}
}
