package state

import (
	"context"
	"errors"
	"sync"
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

// TestSecondStoreInstanceOpensWhileAnotherIsLive simulates two processes
// sharing one state directory: opening a second store must succeed even while
// the first holds a live lease on some batch.
func TestSecondStoreInstanceOpensWhileAnotherIsLive(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatalf("first store: %v", err)
	}
	defer s1.Close()
	s2, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatalf("second store on the same dir must open while the first is live: %v", err)
	}
	defer s2.Close()
	s1.Close()
	s2.Close()
}

// TestUnrelatedBatchesAndReadsProceedWhileOneBatchRuns verifies that a live
// running batch neither blocks other batches nor read-only queries, across
// two store instances (i.e. two processes).
func TestUnrelatedBatchesAndReadsProceedWhileOneBatchRuns(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s1, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// Process 1 starts batch-a and keeps it running.
	stA, _, err := s1.Begin(ctx, "batch-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if stA.LeaseToken == "" {
		t.Fatal("expected live lease on batch-a")
	}

	// Process 2 sees batch-a running...
	if _, _, err := s2.Begin(ctx, "batch-a", false); !errors.Is(err, ErrBatchRunning) {
		t.Fatalf("same batch from another process: want ErrBatchRunning, got %v", err)
	}
	// ...but runs an unrelated batch to completion without interference.
	stB, _, err := s2.Begin(ctx, "batch-b", false)
	if err != nil {
		t.Fatalf("unrelated batch must begin while batch-a runs: %v", err)
	}
	if err := s2.Commit(ctx, "batch-b", stB.LeaseToken, "fp-b", sampleResults(), nil); err != nil {
		t.Fatalf("unrelated batch must commit while batch-a runs: %v", err)
	}
	// ...and read-only queries work from either process at any time.
	if _, err := s2.Get(ctx, "batch-a"); err != nil {
		t.Fatalf("Get of the running batch must not be blocked: %v", err)
	}
	list, err := s2.List(ctx)
	if err != nil {
		t.Fatalf("List must not be blocked by a running batch: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("want 2 batches listed, got %d", len(list))
	}
	// Process 1 can still finish its own batch.
	if err := s1.Commit(ctx, "batch-a", stA.LeaseToken, "fp-a", sampleResults(), nil); err != nil {
		t.Fatalf("commit batch-a: %v", err)
	}
}

// TestConcurrentBeginAcrossInstancesExactlyOneLease races two store instances
// (two processes) on the same fresh batch id: exactly one may win the lease;
// the other must observe ErrBatchRunning. There must never be two owners.
func TestConcurrentBeginAcrossInstancesExactlyOneLease(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s1, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := NewFileStoreWithTTL(dir, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	const attempts = 20
	for round := 0; round < attempts; round++ {
		id := "race-" + string(rune('a'+round))
		type outcome struct {
			st     *BatchState
			replay bool
			err    error
		}
		res := make([]outcome, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, s := range []Store{s1, s2} {
			wg.Add(1)
			go func(i int, s Store) {
				defer wg.Done()
				<-start
				res[i].st, res[i].replay, res[i].err = s.Begin(ctx, id, false)
			}(i, s)
		}
		close(start)
		wg.Wait()

		leases, runnings := 0, 0
		for _, r := range res {
			if r.err == nil {
				if r.replay {
					t.Fatalf("round %d: fresh batch must not replay", round)
				}
				leases++
				continue
			}
			if errors.Is(r.err, ErrBatchRunning) {
				runnings++
				continue
			}
			t.Fatalf("round %d: unexpected error %v", round, r.err)
		}
		if leases != 1 || runnings != 1 {
			t.Fatalf("round %d: want exactly 1 lease + 1 ErrBatchRunning, got %d/%d", round, leases, runnings)
		}
	}
}
