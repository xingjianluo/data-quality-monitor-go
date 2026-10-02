package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// flakySink fails the first failN sends across all batches, then delivers
// everything; it records the (batch, rule) pairs that physically went out so
// the test can prove exactly-once delivery.
type flakySink struct {
	mu    sync.Mutex
	failN int
	calls int
	sent  []alert.Alert
}

func (s *flakySink) Send(ctx context.Context, a alert.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failN {
		return os.ErrInvalid
	}
	s.sent = append(s.sent, a)
	return nil
}

func (s *flakySink) keys() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, a := range s.sent {
		out[a.BatchID+"\x00"+a.RuleID]++
	}
	return out
}

// copyExampleData places the shipped example data into a writable temp dir so
// drift scenarios can modify/delete files without touching the repo.
func copyExampleData(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"users.csv", "events.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(root, "examples", "data", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "data", name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "examples", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func expectedFailures(b *model.BatchResult) int {
	n := 0
	for _, r := range b.Results {
		if r.Status != model.StatusPassed {
			n++
		}
	}
	return n
}

// TestLateArrivalConcurrencyAndAlertRetryCombined drives the three new
// behaviors together under -race:
//
//  1. a newer batch runs first; an older unseen batch arrives late and is
//     evaluated independently,
//  2. concurrent triggers of the late batch all share one execution (single
//     conclusion, one notification per rule),
//  3. a flaky channel fails the first dispatches; restarts (new engines with
//     the same durable journal) backfill missing alerts without duplicating
//     delivered ones.
func TestLateArrivalConcurrencyAndAlertRetryCombined(t *testing.T) {
	dir := copyExampleData(t)
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.NewFileStoreWithTTL(filepath.Join(dir, "state"), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	evalTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	newEngine := func(sink alert.Sink) *engine.Engine {
		al, err := alert.NewAlerter(sink, filepath.Join(dir, "state", "alert-journal.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		return engine.New(cfg, datasource.New(""), store, al, engine.Options{
			MaxConcurrentRules:  8,
			RuleTimeout:         5 * time.Second,
			EvalTime:            evalTime,
			RunningPollInterval: 10 * time.Millisecond,
		})
	}
	ctx := context.Background()

	// Flaky channel: the first 4 physical sends fail.
	sink := &flakySink{failN: 4}
	eng := newEngine(sink)

	// 1. Newer batch first.
	newer, err := eng.Run(ctx, "20260923-010", false)
	if err != nil {
		t.Fatalf("newer batch: %v", err)
	}
	if newer.Status != "committed" {
		t.Fatalf("newer batch status=%s", newer.Status)
	}
	wantNewerAlerts := expectedFailures(newer)
	// Invariant in every report: expected = already + sent now + pending.
	if newer.AlertReport.Expected != wantNewerAlerts ||
		newer.AlertReport.AlreadyDelivered+newer.AlertReport.SentThisRun+newer.AlertReport.Pending != wantNewerAlerts {
		t.Fatalf("newer report does not partition expected alerts: %+v", newer.AlertReport)
	}

	// 2. Late older batch, triggered concurrently many times. Only one
	// execution must commit and notify.
	const triggers = 6
	var wg sync.WaitGroup
	results := make([]*model.BatchResult, triggers)
	errs := make([]error, triggers)
	go func() {
		// Give all goroutines a moment to queue behind the leader.
		time.Sleep(20 * time.Millisecond)
	}()
	start := make(chan struct{})
	for i := 0; i < triggers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = newEngine(sink).Run(ctx, "20260922-999", false)
		}(i)
	}
	close(start)
	wg.Wait()

	var lateFP string
	for i := 0; i < triggers; i++ {
		if errs[i] != nil {
			t.Fatalf("late trigger %d: %v", i, errs[i])
		}
		if results[i] == nil {
			t.Fatalf("late trigger %d nil result", i)
		}
		if i == 0 {
			lateFP = results[i].Fingerprint
			if results[i].Status != "committed" && results[i].Status != "replay" {
				t.Fatalf("unexpected late status=%s", results[i].Status)
			}
		}
		if results[i].Fingerprint != lateFP {
			t.Fatalf("late trigger %d got divergent fingerprint", i)
		}
	}
	st, err := store.Get(ctx, "20260922-999")
	if err != nil || st.Status != state.StatusCommitted {
		t.Fatalf("late batch must be committed exactly once: %v", err)
	}
	// The newer batch history is intact and independent.
	stNewer, err := store.Get(ctx, "20260923-010")
	if err != nil || stNewer.Status != state.StatusCommitted || stNewer.Fingerprint == st.Fingerprint {
		t.Fatalf("newer batch history disturbed by late arrival")
	}

	// 3. Retry the late batch repeatedly with the same flaky (now recovered
	// after 4 failed sends) journal: all alerts converge, none duplicated.
	var last *model.BatchResult
	for attempt := 0; attempt < 3; attempt++ {
		last, err = newEngine(sink).Run(ctx, "20260922-999", false)
		if err != nil {
			t.Fatalf("retry %d: %v", attempt, err)
		}
		if last.Status != "replay" {
			t.Fatalf("retry %d status=%s", attempt, last.Status)
		}
	}
	if last.AlertReport.Pending != 0 {
		t.Fatalf("recovered backlog still pending: %+v", last.AlertReport)
	}

	// Also retry the newer batch to fill its backlog.
	retryNewer, err := newEngine(sink).Run(ctx, "20260923-010", false)
	if err != nil {
		t.Fatal(err)
	}
	if retryNewer.AlertReport.Pending != 0 {
		t.Fatalf("newer batch backlog remains: %+v", retryNewer.AlertReport)
	}

	// Physical delivery accounting: every (batch, failed rule) exactly once.
	keys := sink.keys()
	expected := map[string]bool{}
	for _, b := range []*model.BatchResult{newer, results[0]} {
		for _, r := range b.Results {
			if r.Status != model.StatusPassed {
				expected[b.BatchID+"\x00"+r.RuleID] = true
			}
		}
	}
	if len(keys) != len(expected) {
		t.Fatalf("delivered %d distinct alerts, want %d", len(keys), len(expected))
	}
	for k := range expected {
		if keys[k] != 1 {
			t.Fatalf("alert %q delivered %d times, want exactly once", strings.ReplaceAll(k, "\x00", "/"), keys[k])
		}
	}

	// Final replay on a fresh engine/process reports zero sent and zero pending.
	fresh, err := newEngine(&flakySink{}).Run(ctx, "20260922-999", false)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.AlertReport.SentThisRun != 0 || fresh.AlertReport.Pending != 0 {
		t.Fatalf("clean replay report should be idle: %+v", fresh.AlertReport)
	}
}
