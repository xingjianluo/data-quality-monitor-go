package state

import (
	"context"
	"errors"
	"testing"
	"time"

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
	s, err := NewFileStoreWithTTL(t.TempDir(), 30*time.Second)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// newShortTTLStore issues leases that expire almost immediately, simulating
// crash leftovers without long sleeps.
func newShortTTLStore(t *testing.T) Store {
	t.Helper()
	s, err := NewFileStoreWithTTL(t.TempDir(), 15*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileStoreWithTTL: %v", err)
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
	snap := &InputSnapshot{RuleHash: "rules-1"}
	if err := s.Commit(ctx, "b-001", token, "fp-1", sampleResults(), snap); err != nil {
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
	if st2.Inputs == nil || st2.Inputs.RuleHash != "rules-1" {
		t.Fatalf("input snapshot not preserved: %+v", st2.Inputs)
	}
	if len(st2.Results) != 2 || st2.Results[0].RuleID != "r1" {
		t.Fatalf("historical conclusions not preserved: %+v", st2.Results)
	}

	// A committed batch cannot be committed again (no live lease).
	if err := s.Commit(ctx, "b-001", token, "fp-x", nil, nil); !errors.Is(err, ErrAlreadyConcluded) {
		t.Fatalf("expected ErrAlreadyConcluded on double commit, got %v", err)
	}
	// force can neither overwrite nor reclaim committed conclusions.
	if _, _, err := s.Begin(ctx, "b-001", true); !errors.Is(err, ErrAlreadyConcluded) {
		t.Fatalf("force on committed batch: expected ErrAlreadyConcluded, got %v", err)
	}
}

func TestConcurrentBeginReportsRunning(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, _, err := s.Begin(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	// While the lease is live another trigger must see "running", not start a
	// second execution and not get a stale/rollback error.
	if _, _, err := s.Begin(ctx, "b-001", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("expected ErrBatchRunning, got %v", err)
	}
}

func TestHeartbeatKeepsLeaseLive(t *testing.T) {
	s := newShortTTLStore(t)
	ctx := context.Background()
	st, _, err := s.Begin(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if err := s.Heartbeat(ctx, "b-001", st.LeaseToken); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	// Renewed lease: still "running", not reclaimable as stale.
	if _, _, err := s.Begin(ctx, "b-001", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("after heartbeat expected ErrBatchRunning, got %v", err)
	}
	// Wrong token cannot renew.
	if err := s.Heartbeat(ctx, "b-001", "nope"); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease, got %v", err)
	}
}

func TestStaleRunningRequiresForce(t *testing.T) {
	s := newShortTTLStore(t)
	ctx := context.Background()

	st, replay, err := s.Begin(ctx, "b-010", false)
	if err != nil || replay {
		t.Fatalf("Begin: %v replay=%v", err, replay)
	}
	// Simulate a crash: wait for the lease to expire.
	time.Sleep(25 * time.Millisecond)
	if _, _, err := s.Begin(ctx, "b-010", false); !errors.Is(err, ErrStaleBatch) {
		t.Fatalf("expected ErrStaleBatch, got %v", err)
	}
	// Marking failure releases the lease; still requires force.
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
	if err := s.Commit(ctx, "b-010", st2.LeaseToken, "fp-2", sampleResults(), nil); err != nil {
		t.Fatalf("Commit after reclaim: %v", err)
	}
}

func TestLateArrivalOfUnprocessedOlderBatchAccepted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	st, _, err := s.Begin(ctx, "b-100", false)
	if err != nil {
		t.Fatalf("Begin b-100: %v", err)
	}
	if err := s.Commit(ctx, "b-100", st.LeaseToken, "fp", sampleResults(), nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// An older batch that was never processed arrives late: it is accepted and
	// evaluated normally instead of being dropped as a "rollback".
	late, replay, err := s.Begin(ctx, "b-050", false)
	if err != nil {
		t.Fatalf("late unseen batch must be accepted, got %v", err)
	}
	if replay || late.Status != StatusRunning {
		t.Fatalf("late batch: replay=%v status=%s", replay, late.Status)
	}
	if err := s.Commit(ctx, "b-050", late.LeaseToken, "fp-late", sampleResults(), nil); err != nil {
		t.Fatalf("Commit late batch: %v", err)
	}
	// Both histories coexist, neither overwrote the other.
	for _, id := range []string{"b-050", "b-100"} {
		got, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if got.Status != StatusCommitted {
			t.Fatalf("batch %s not committed: %s", id, got.Status)
		}
	}
	// Re-running the late batch id replays it rather than accepting a rewrite.
	if _, replay, err := s.Begin(ctx, "b-050", false); err != nil || !replay {
		t.Fatalf("re-run late batch: replay=%v err=%v", replay, err)
	}
}

func TestCommitRequiresValidLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, _, err := s.Begin(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "b-001", "wrong-token", "fp", nil, nil); !errors.Is(err, ErrLease) {
		t.Fatalf("expected ErrLease, got %v", err)
	}
	if err := s.Commit(ctx, "missing", "x", "fp", nil, nil); !errors.Is(err, ErrNotFound) {
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
		if err := s.Commit(ctx, id, st.LeaseToken, "fp-"+id, sampleResults(), nil); err != nil {
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
