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

type memSink struct {
	mu     sync.Mutex
	alerts []Alert
	failN  int
	calls  int
	// delivered suppresses a repeat of the same DeliveryID, modeling an
	// idempotent sink (like the built-in FileSink). nil means no suppression.
	delivered map[string]bool
}

func (m *memSink) Send(ctx context.Context, a Alert) error {
	_, err := m.SendToken(ctx, a)
	return err
}

func (m *memSink) SendToken(ctx context.Context, a Alert) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.delivered != nil && a.DeliveryID != "" && m.delivered[a.DeliveryID] {
		return false, nil // idempotent retry suppressed as a duplicate
	}
	m.calls++
	if m.calls <= m.failN {
		return false, os.ErrInvalid // simulated transient delivery failure
	}
	m.alerts = append(m.alerts, a)
	if m.delivered != nil && a.DeliveryID != "" {
		m.delivered[a.DeliveryID] = true
	}
	return true, nil
}

func failedResult(id string, severity model.Severity) model.RuleResult {
	return model.RuleResult{
		RuleID: id, Dataset: "d", Kind: model.KindUniqueness, Severity: severity,
		Status: model.StatusFailed, ErrorCode: "",
		Findings: []model.Finding{{Code: model.ReasonDuplicate, Evidence: []model.Evidence{{Line: 2}}}},
	}
}

func countLines(t *testing.T, p string) int {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	return n
}

// countDelivered counts confirmed ("delivered") journal records. Legacy
// records without a status are also confirmed sends.
func countDelivered(t *testing.T, p string) int {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e map[string]any
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		st, _ := e["status"].(string)
		if st == "delivered" || st == "" {
			n++
		}
	}
	return n
}

// countPending counts unconfirmed ("pending") records: a pending line that is
// later followed by a delivered line for the same key is reconciled and is not
// pending anymore.
func countPending(t *testing.T, p string) int {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	unresolved := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e struct {
			BatchID string `json:"batch_id"`
			RuleID  string `json:"rule_id"`
			Status  string `json:"status"`
		}
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		key := e.BatchID + "\x00" + e.RuleID
		switch e.Status {
		case "pending":
			unresolved[key] = true
		case "delivered", "":
			delete(unresolved, key)
		}
	}
	return len(unresolved)
}

func TestDispatchExactlyOnceAcrossRepeats(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal.jsonl")
	sink := &memSink{}
	a, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityWarning),
		{RuleID: "r3", Status: model.StatusPassed, Severity: model.SeverityError, Findings: []model.Finding{}},
	}

	rep, err := a.Dispatch(ctx, "b1", results, model.SeverityWarning)
	if err != nil {
		t.Fatal(err)
	}
	if rep.SentThisRun != 2 || rep.Expected != 2 || rep.Pending != 0 || rep.AlreadyDelivered != 0 {
		t.Fatalf("unexpected first report: %+v", rep)
	}
	// Repeating the batch must not duplicate anything.
	for i := 0; i < 3; i++ {
		rep, err := a.Dispatch(ctx, "b1", results, model.SeverityWarning)
		if err != nil || rep.SentThisRun != 0 || rep.AlreadyDelivered != 2 || rep.Pending != 0 {
			t.Fatalf("repeat %d: rep=%+v err=%v", i, rep, err)
		}
	}
	if len(sink.alerts) != 2 {
		t.Fatalf("sink received %d alerts, want 2", len(sink.alerts))
	}
	if countDelivered(t, journal) != 2 {
		t.Fatalf("journal must confirm exactly 2 deliveries")
	}
	if countPending(t, journal) != 0 {
		t.Fatalf("journal must have no pending intents after success")
	}
}

func TestDispatchResumesAfterPartialFailure(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	sink := &memSink{failN: 1} // first delivery fails -> must not be journaled
	a, _ := NewAlerter(sink, journal)
	results := []model.RuleResult{failedResult("r1", model.SeverityError)}

	rep, err := a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected propagated delivery error")
	}
	if rep.SentThisRun != 0 || rep.Pending != 1 || rep.Expected != 1 {
		t.Fatalf("failed delivery must not be counted sent: rep=%+v", rep)
	}
	// The delivery INTENT is durable even though the send failed, so a crash
	// between intent and send is recoverable; nothing is confirmed delivered.
	if countLines(t, journal) != 1 || countPending(t, journal) != 1 || countDelivered(t, journal) != 0 {
		t.Fatalf("failed send must leave one pending intent, lines=%d", countLines(t, journal))
	}
	// New alerter instance (simulating process restart) resumes and sends once.
	// The channel is idempotent and shares the delivery record across restart.
	sink2 := &memSink{delivered: map[string]bool{}}
	a2, _ := NewAlerter(sink2, journal)
	rep, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.Pending != 0 {
		t.Fatalf("resume: rep=%+v err=%v", rep, err)
	}
	rep, _ = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if rep.SentThisRun != 0 || rep.AlreadyDelivered != 1 {
		t.Fatalf("second resume must not duplicate: rep=%+v", rep)
	}
}

func TestSeverityFiltering(t *testing.T) {
	dir := t.TempDir()
	a, _ := NewAlerter(&memSink{}, filepath.Join(dir, "j.jsonl"))
	results := []model.RuleResult{failedResult("warn", model.SeverityWarning)}
	rep, err := a.Dispatch(context.Background(), "b1", results, model.SeverityError)
	if err != nil || rep.Expected != 0 || rep.SentThisRun != 0 {
		t.Fatalf("warning below error threshold: rep=%+v err=%v", rep, err)
	}
}

func TestFileSinkWritesJSONLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "alerts.jsonl")
	s, err := NewFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), Alert{BatchID: "b", RuleID: "r", Code: model.ReasonDuplicate}); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(p)
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatal("expected one json line")
	}
	var got Alert
	if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.BatchID != "b" || got.Code != model.ReasonDuplicate {
		t.Fatalf("bad payload: %+v", got)
	}
}

// TestDispatchBacklogReportAcrossRestarts covers multiple pending alerts being
// filled one by one across invocations: each report must state exactly how
// many were retried now and how many remain undelivered, with no duplicates.
func TestDispatchBacklogReportAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
		failedResult("r3", model.SeverityError),
	}

	// First channel fails everything: 3 pending, nothing journaled.
	failing := &memSink{failN: 1 << 20}
	a1, _ := NewAlerter(failing, journal)
	rep, err := a1.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected delivery errors")
	}
	if rep.Expected != 3 || rep.SentThisRun != 0 || rep.Pending != 3 {
		t.Fatalf("all-failing report: %+v", rep)
	}

	// Restarted channel fails its first attempt then works: r1 stays pending,
	// r2 and r3 go through in the same invocation. The channel is idempotent.
	channel := &memSink{failN: 1, delivered: map[string]bool{}}
	sink2 := channel
	a2, _ := NewAlerter(sink2, journal)
	rep, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil || rep.SentThisRun != 2 || rep.Pending != 1 || rep.AlreadyDelivered != 0 {
		t.Fatalf("first partial recovery: %+v err=%v", rep, err)
	}

	// Another restart (new process) with a healthy channel fills the rest.
	sink3 := channel
	a3, _ := NewAlerter(sink3, journal)
	rep, err = a3.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.Pending != 0 || rep.AlreadyDelivered != 2 {
		t.Fatalf("final recovery: %+v err=%v", rep, err)
	}

	// Physically delivered exactly once per rule across the shared idempotent
	// channel (sink2 and sink3 are the same channel seen across restarts).
	got := map[string]int{}
	for _, x := range channel.alerts {
		got[x.RuleID]++
	}
	want := map[string]int{"r1": 1, "r2": 1, "r3": 1}
	if len(got) != 3 {
		t.Fatalf("delivered rules = %v, want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Fatalf("rule %s delivered %d times, want %d", k, got[k], n)
		}
	}
	if countDelivered(t, journal) != 3 || countPending(t, journal) != 0 {
		t.Fatalf("journal must confirm exactly 3 deliveries with no pending intents")
	}
}

// TestFileSinkSendTokenIdempotent verifies the built-in sink suppresses a
// repeat of the same DeliveryID (the guarantee that closes the crash window)
// while distinct IDs are written once each.
func TestFileSinkSendTokenIdempotent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "alerts.jsonl")
	s, err := NewFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	a := Alert{BatchID: "b", RuleID: "r", Code: model.ReasonDuplicate, DeliveryID: "tok-1"}

	sent, err := s.SendToken(context.Background(), a)
	if err != nil || !sent {
		t.Fatalf("first SendToken: sent=%v err=%v", sent, err)
	}
	sent, err = s.SendToken(context.Background(), a) // crash-window retry
	if err != nil || sent {
		t.Fatalf("duplicate SendToken must be suppressed: sent=%v err=%v", sent, err)
	}
	other := a
	other.DeliveryID = "tok-2"
	sent, err = s.SendToken(context.Background(), other)
	if err != nil || !sent {
		t.Fatalf("distinct DeliveryID must write: sent=%v err=%v", sent, err)
	}
	if n := countLines(t, p); n != 2 {
		t.Fatalf("physical alert lines=%d, want 2", n)
	}
}
