package state

import (
	"context"
	"errors"
	"testing"
	"time"

	manifestpkg "github.com/xingjianluo/data-quality-monitor-go/internal/manifest"
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
	s, err := NewFileStoreWithTTL(t.TempDir(), 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func commitBatch(t *testing.T, s Store, id string) {
	t.Helper()
	ctx := context.Background()
	st, replay, err := s.Begin(ctx, id, false)
	if err != nil || replay {
		t.Fatalf("Begin %s: replay=%v err=%v", id, replay, err)
	}
	if err := s.Commit(ctx, id, st.LeaseToken, "fp-"+id, manifestpkg.Manifest{}, sampleResults()); err != nil {
		t.Fatalf("Commit %s: %v", id, err)
	}
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
	if err := s.Commit(ctx, "b-001", token, "fp-1", manifestpkg.Manifest{}, sampleResults()); err != nil {
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
	if err := s.Commit(ctx, "b-001", token, "fp-x", manifestpkg.Manifest{}, nil); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease on double commit, got %v", err)
	}

	// force must never overwrite committed conclusions.
	if _, _, err := s.Begin(ctx, "b-001", true); !errors.Is(err, ErrImmutable) {
		t.Fatalf("force on committed batch: expected ErrImmutable, got %v", err)
	}
}

func TestStaleRunningRequiresForce(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	st, replay, err := s.Begin(ctx, "b-010", false)
	if err != nil || replay {
		t.Fatalf("Begin: %v replay=%v", err, replay)
	}
	// While the lease is alive (heartbeat fresh) a concurrent trigger sees
	// "already running", never a second lease.
	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("live lease: expected ErrBatchRunning, got %v", err)
	}
	if _, _, err := s.Begin(ctx, "b-010", true); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("force must not steal a live lease: expected ErrBatchRunning, got %v", err)
	}
	// Heartbeat keeps a long-running execution alive.
	time.Sleep(15 * time.Millisecond)
	if err := s.Heartbeat(ctx, "b-010", st.LeaseToken); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("after heartbeat: expected ErrBatchRunning, got %v", err)
	}
	// A heartbeat with a wrong token is rejected.
	if err := s.Heartbeat(ctx, "b-010", "wrong"); !errors.Is(err, ErrLease) {
		t.Fatalf("bad heartbeat: expected ErrLease, got %v", err)
	}
	// After the TTL expires without a heartbeat the run is considered crashed.
	time.Sleep(25 * time.Millisecond)
	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("expired lease: expected ErrStaleBatch, got %v", err)
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
	if err := s.Commit(ctx, "b-010", st2.LeaseToken, "fp-2", manifestpkg.Manifest{}, sampleResults()); err != nil {
		t.Fatalf("Commit after reclaim: %v", err)
	}
}

func TestLateArrivingOlderBatchIsAccepted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	commitBatch(t, s, "b-100")

	// An older, never-processed batch arriving late is evaluated normally:
	// high-water ordering never blocks unseen batches.
	st, replay, err := s.Begin(ctx, "b-050", false)
	if err != nil || replay {
		t.Fatalf("late Begin: replay=%v err=%v", replay, err)
	}
	if err := s.Commit(ctx, "b-050", st.LeaseToken, "fp-late", manifestpkg.Manifest{}, sampleResults()); err != nil {
		t.Fatalf("Commit late batch: %v", err)
	}
	// force on an unseen late batch is also just a fresh start.
	st2, replay, err := s.Begin(ctx, "b-025", true)
	if err != nil || replay {
		t.Fatalf("forced fresh late Begin: replay=%v err=%v", replay, err)
	}
	if err := s.Commit(ctx, "b-025", st2.LeaseToken, "fp-late2", manifestpkg.Manifest{}, sampleResults()); err != nil {
		t.Fatalf("Commit b-025: %v", err)
	}
	// Newer batches are still accepted (orderly progression).
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
	if err := s.Commit(ctx, "b-001", "wrong-token", "fp", manifestpkg.Manifest{}, nil); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease, got %v", err)
	}
	if err := s.Commit(ctx, "missing", "x", "fp", manifestpkg.Manifest{}, nil); !errors.Is(err, ErrNotFound) {
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
		if err := s.Commit(ctx, id, st.LeaseToken, "fp-"+id, manifestpkg.Manifest{}, sampleResults()); err != nil {
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
