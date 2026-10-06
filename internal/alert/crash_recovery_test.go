package alert

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// idemSink models a notification channel that suppresses redelivery of a
// DeliveryID it has already accepted (like the built-in FileSink).
type idemSink struct {
	mu           sync.Mutex
	delivered    map[string]Alert
	failNextOnce bool // the very next physical send fails, channel gets nothing
}

func newIdemSink() *idemSink {
	return &idemSink{delivered: map[string]Alert{}}
}

func (s *idemSink) Send(ctx context.Context, a Alert) error {
	_, err := s.SendToken(ctx, a)
	return err
}

func (s *idemSink) SendToken(ctx context.Context, a Alert) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.delivered[a.DeliveryID]; ok {
		return false, nil // duplicate suppressed at the channel
	}
	if s.failNextOnce {
		s.failNextOnce = false
		return false, os.ErrPermission // transient failure; channel got nothing
	}
	s.delivered[a.DeliveryID] = a
	return true, nil
}

func (s *idemSink) physicalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.delivered)
}

// firstDeliveryID returns the DeliveryID of the first pending/delivered record
// for a key.
func firstDeliveryID(t *testing.T, path, batchID, ruleID string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e journalEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		if e.BatchID == batchID && e.RuleID == ruleID && e.DeliveryID != "" {
			return e.DeliveryID
		}
	}
	t.Fatalf("no journal record for %s/%s in %s", batchID, ruleID, path)
	return ""
}

// truncateToPending rewrites the journal keeping only the "pending" records,
// simulating a crash after the delivery intent was durable. When a pending
// record was already followed by a delivered confirmation this instead keeps
// just the pending line by dropping every later line.
func truncateToPending(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var kept [][]byte
	for _, line := range splitLines(raw) {
		if len(line) == 0 {
			continue
		}
		var e journalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if e.Status == statusPending {
			kept = append(kept, line)
		}
		// Stop after the first pending line per key is unnecessary here: tests
		// have at most one pending record per key.
	}
	out := append([]byte(nil), joinLines(kept)...)
	return os.WriteFile(path, out, 0o644)
}

func splitLines(raw []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\n' {
			out = append(out, raw[start:i])
			start = i + 1
		}
	}
	if start < len(raw) {
		out = append(out, raw[start:])
	}
	return out
}

func joinLines(lines [][]byte) []byte {
	var out []byte
	for _, l := range lines {
		out = append(out, l...)
		out = append(out, '\n')
	}
	return out
}

// TestCrashAfterDeliveryBeforeJournal simulates the dangerous window: the
// channel accepted the alert, but the process was killed before the journal
// recorded "delivered". The surviving "pending" intent is retried with the
// SAME DeliveryID; the idempotent channel suppresses the duplicate and the
// entry is confirmed. No second physical notification is ever produced.
func TestCrashAfterDeliveryBeforeJournal(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	results := []model.RuleResult{failedResult("r1", model.SeverityError)}

	// First invocation with a healthy channel: send succeeds.
	sink := newIdemSink()
	a1, _ := NewAlerter(sink, journal)
	rep, err := a1.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 {
		t.Fatalf("initial dispatch: rep=%+v err=%v", rep, err)
	}
	deliveryID := firstDeliveryID(t, journal, "b1", "r1")

	// Simulate a crash AFTER delivery but BEFORE the delivered journal line by
	// truncating the journal back to just its "pending" intent.
	if err := truncateToPending(journal); err != nil {
		t.Fatal(err)
	}

	// Restart: a new process/alerter talking to the same idempotent channel.
	a2, _ := NewAlerter(sink, journal)
	rep, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil {
		t.Fatalf("crash recovery dispatch: %v", err)
	}
	if rep.AlreadyDelivered != 0 || rep.SentThisRun != 1 || rep.Pending != 0 {
		t.Fatalf("recovery must confirm the retried intent as sent: %+v", rep)
	}
	// The retry reused the exact pre-crash DeliveryID.
	if got := firstDeliveryID(t, journal, "b1", "r1"); got != deliveryID {
		t.Fatalf("recovery changed DeliveryID %q -> %q (must stay stable)", deliveryID, got)
	}
	// Exactly one physical notification, even though two processes sent it.
	if sink.physicalCount() != 1 {
		t.Fatalf("physical deliveries = %d, want exactly 1", sink.physicalCount())
	}

	// Another replay now reports purely already-delivered and sends nothing.
	a3, _ := NewAlerter(sink, journal)
	rep, err = a3.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 0 || rep.AlreadyDelivered != 1 || rep.Pending != 0 {
		t.Fatalf("clean replay report: %+v err=%v", rep, err)
	}
	if sink.physicalCount() != 1 {
		t.Fatalf("physical deliveries = %d after replay, want 1", sink.physicalCount())
	}
}

// TestCrashBeforeDeliveryBackfills simulates a crash after the pending intent
// was durable but before the channel got anything. Recovery actually sends it.
func TestCrashBeforeDeliveryBackfills(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	results := []model.RuleResult{failedResult("r1", model.SeverityError)}

	sink := newIdemSink()
	a1, _ := NewAlerter(sink, journal)
	rep, _ := a1.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if rep.SentThisRun != 1 {
		t.Fatalf("initial: %+v", rep)
	}
	deliveryID := firstDeliveryID(t, journal, "b1", "r1")

	// Fresh channel that never received anything, same pending journal: the
	// alert genuinely never went out and must be backfilled.
	freshSink := newIdemSink()
	if err := truncateToPending(journal); err != nil {
		t.Fatal(err)
	}
	a2, _ := NewAlerter(freshSink, journal)
	rep, err := a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.Pending != 0 {
		t.Fatalf("backfill: %+v err=%v", rep, err)
	}
	if freshSink.physicalCount() != 1 {
		t.Fatalf("backfill must physically deliver once, got %d", freshSink.physicalCount())
	}
	if got := firstDeliveryID(t, journal, "b1", "r1"); got != deliveryID {
		t.Fatalf("DeliveryID not reused across backfill: %q != %q", got, deliveryID)
	}
}

// TestDeliveryFailureThenRetryBackfills: first attempt fails at the channel
// (pending intent durable), a later invocation succeeds and reports the
// backfill count, with no duplicate on subsequent runs.
func TestDeliveryFailureThenRetryBackfills(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
	}

	sink := newIdemSink()
	sink.failNextOnce = true // r1's first physical send fails; channel got nothing
	a1, _ := NewAlerter(sink, journal)
	rep, err := a1.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected a delivery error")
	}
	// r1 failed (pending), r2 delivered: 1 sent, 1 pending.
	if rep.Expected != 2 || rep.SentThisRun != 1 || rep.Pending != 1 {
		t.Fatalf("partial failure report: %+v", rep)
	}

	// Next invocation backfills the single pending alert.
	a2, _ := NewAlerter(sink, journal)
	rep, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.AlreadyDelivered != 1 || rep.Pending != 0 {
		t.Fatalf("backfill report: %+v err=%v", rep, err)
	}
	if sink.physicalCount() != 2 {
		t.Fatalf("physical deliveries = %d, want 2", sink.physicalCount())
	}
}

// TestConcurrentDispatchSameBatchOnce drives many alerters (as separate
// processes would) dispatching the same batch concurrently: exactly one
// physical delivery per rule, every report consistent.
func TestConcurrentDispatchSameBatchOnce(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
	}
	sink := newIdemSink()

	const n = 8
	var wg sync.WaitGroup
	reps := make([]model.AlertReport, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			al, _ := NewAlerter(sink, journal)
			<-start
			reps[i], errs[i] = al.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("dispatch %d: %v", i, errs[i])
		}
		r := reps[i]
		if r.Expected != 2 || r.Pending != 0 ||
			r.AlreadyDelivered+r.SentThisRun != 2 {
			t.Fatalf("dispatch %d inconsistent report: %+v", i, r)
		}
	}
	if sink.physicalCount() != 2 {
		t.Fatalf("physical deliveries = %d, want exactly 2", sink.physicalCount())
	}
}
