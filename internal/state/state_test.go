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

// TestDifferentBatchesDoNotBlockEachOther opens two independent FileStore
// handles on the same directory, the way two OS processes would. While one
// process holds a live lease on batch A, the other process must be able to run
// a different batch B to completion and to read history, without --force and
// without ErrBusy/ErrBatchRunning.
func TestDifferentBatchesDoNotBlockEachOther(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })

	// A second "process": a separate handle on the same state dir. The old
	// implementation failed here immediately with ErrBusy because of the
	// process-lifetime global flock.
	s2, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatalf("second store handle on same dir must not fail: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	// Process 1 starts batch A and keeps it running (live lease).
	a, replay, err := s1.Begin(ctx, "batch-A", false)
	if err != nil || replay {
		t.Fatalf("begin A: %v replay=%v", err, replay)
	}

	// Process 2 must see A as running ...
	if _, _, err := s2.Begin(ctx, "batch-A", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("A concurrently: expected ErrBatchRunning, got %v", err)
	}
	// ... but an unrelated batch B proceeds fully independently.
	b, replay, err := s2.Begin(ctx, "batch-B", false)
	if err != nil || replay {
		t.Fatalf("unrelated batch B must run while A is live: %v replay=%v", err, replay)
	}
	if err := s2.Commit(ctx, "batch-B", b.LeaseToken, "fp-B", sampleResults(), nil); err != nil {
		t.Fatalf("commit B while A live: %v", err)
	}

	// Read-only queries never block on the running batch.
	got, err := s2.Get(ctx, "batch-B")
	if err != nil || got.Status != StatusCommitted {
		t.Fatalf("Get B: %v status=%v", err, got)
	}
	list, err := s2.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("history while A running: %v len=%d", err, len(list))
	}

	// Process 1 can still finish A afterwards (different lock from B).
	if err := s1.Commit(ctx, "batch-A", a.LeaseToken, "fp-A", sampleResults(), nil); err != nil {
		t.Fatalf("commit A: %v", err)
	}
}

// TestSameBatchLockSerializesAcrossHandles ensures the per-batch lock itself
// is honored across two handles: two simultaneous Begin+Commit sequences on the
// SAME id can never produce two different committed leases.
func TestSameBatchLockSerializesAcrossHandles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s1, _ := NewFileStoreWithTTL(dir, time.Minute)
	s2, _ := NewFileStoreWithTTL(dir, time.Minute)
	t.Cleanup(func() { _ = s1.Close(); _ = s2.Close() })

	st1, replay1, err := s1.Begin(ctx, "x", false)
	if err != nil || replay1 {
		t.Fatalf("s1 begin: %v %v", err, replay1)
	}
	// s2 cannot start while the lease is live.
	if _, _, err := s2.Begin(ctx, "x", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("s2 concurrent begin: %v", err)
	}
	// A stale/wrong token from s2 cannot hijack s1's commit.
	if err := s2.Commit(ctx, "x", "deadbeef", "fp-hijack", nil, nil); !errors.Is(err, ErrLease) {
		t.Fatalf("hijack commit expected ErrLease, got %v", err)
	}
	if err := s1.Commit(ctx, "x", st1.LeaseToken, "fp-real", sampleResults(), nil); err != nil {
		t.Fatalf("s1 commit: %v", err)
	}
	// After commit, s2 replays the one real conclusion.
	got, replay, err := s2.Begin(ctx, "x", false)
	if err != nil || !replay || got.Fingerprint != "fp-real" {
		t.Fatalf("s2 replay: %v replay=%v fp=%s", err, replay, got.Fingerprint)
	}
}
