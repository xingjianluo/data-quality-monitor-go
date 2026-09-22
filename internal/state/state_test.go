package state

import (
	"context"
	"errors"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func sampleResults() []model.RuleResult {
	return []model.RuleResult{
		{RuleID: "r1", Dataset: "d", Kind: model.KindUniqueness, Severity: model.SeverityError, Status: model.StatusFailed, Findings: []model.Finding{{Code: model.ReasonDuplicate}}},
		{RuleID: "r2", Dataset: "d", Kind: model.KindCompleteness, Severity: model.SeverityError, Status: model.StatusPassed, Findings: []model.Finding{}},
	}
}

func newTestStore(t *testing.T) Store {
	t.Helper()
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestBeginCommitReplayIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	st, replay, err := s.Begin(ctx, "b-001", false)
	if err != nil || replay {
		t.Fatalf("first Begin: replay=%v err=%v", replay, err)
	}
	token := st.LeaseToken
	if token == "" {
		t.Fatal("expected a lease token")
	}
	if err := s.Commit(ctx, "b-001", token, "fp-1", sampleResults()); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Repeating the same batch replays without requiring force.
	st2, replay, err := s.Begin(ctx, "b-001", false)
	if err != nil || !replay {
		t.Fatalf("second Begin: replay=%v err=%v", replay, err)
	}
	if st2.Status != StatusCommitted || st2.Fingerprint != "fp-1" {
		t.Fatalf("unexpected replayed state: %+v", st2)
	}
	if len(st2.Results) != 2 || st2.Results[0].RuleID != "r1" {
		t.Fatalf("historical conclusions not preserved: %+v", st2.Results)
	}

	// A committed batch cannot be committed again (lease released).
	if err := s.Commit(ctx, "b-001", token, "fp-x", nil); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease on double commit, got %v", err)
	}
}

func TestStaleRunningRequiresForce(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	st, replay, err := s.Begin(ctx, "b-010", false)
	if err != nil || replay {
		t.Fatalf("Begin: %v replay=%v", err, replay)
	}
	// Simulate a crash: batch remains running with a token nobody holds.

	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("expected ErrStaleBatch, got %v", err)
	}
	// Marking failure releases the lease but still requires force.
	if err := s.Fail(ctx, "b-010", st.LeaseToken, "simulated"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("expected ErrStaleBatch after Fail, got %v", err)
	}
	st2, replay, err := s.Begin(ctx, "b-010", true)
	if err != nil || replay {
		t.Fatalf("forced Begin: %v replay=%v", err, replay)
	}
	if st2.LeaseToken == "" || st2.LeaseToken == st.LeaseToken {
		t.Fatal("force reclaim must issue a fresh lease token")
	}
	if err := s.Commit(ctx, "b-010", st2.LeaseToken, "fp-2", sampleResults()); err != nil {
		t.Fatalf("Commit after reclaim: %v", err)
	}
}

func TestOutOfOrderRollbackRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	st, _, err := s.Begin(ctx, "b-100", false)
	if err != nil {
		t.Fatalf("Begin b-100: %v", err)
	}
	if err := s.Commit(ctx, "b-100", st.LeaseToken, "fp", sampleResults()); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// An older batch arriving late must not move state backwards.
	if _, _, err := s.Begin(ctx, "b-050", false); !errors.Is(err, ErrRollback) {
		t.Fatalf("Begin older batch: expected ErrRollback, got %v", err)
	}
	// Even a force rerun cannot roll back.
	if _, _, err := s.Begin(ctx, "b-050", true); !errors.Is(err, ErrRollback) {
		t.Fatalf("forced Begin older batch: expected ErrRollback, got %v", err)
	}
	// Newer batches are accepted (orderly progression).
	if _, replay, err := s.Begin(ctx, "b-200", false); err != nil || replay {
		t.Fatalf("Begin newer batch: replay=%v err=%v", replay, err)
	}
}

func TestCommitRequiresValidLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, _, err := s.Begin(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "b-001", "wrong-token", "fp", nil); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease, got %v", err)
	}
	if err := s.Commit(ctx, "missing", "x", "fp", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListIsOrderedAndReproducible(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"b-001", "b-002", "b-003"} {
		st, _, err := s.Begin(ctx, id, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Commit(ctx, id, st.LeaseToken, "fp-"+id, sampleResults()); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("want 3 batches, got %d", len(list))
	}
	want := []string{"b-001", "b-002", "b-003"}
	for i, w := range want {
		if list[i].BatchID != w {
			t.Fatalf("position %d: want %s got %s", i, w, list[i].BatchID)
		}
	}
}
