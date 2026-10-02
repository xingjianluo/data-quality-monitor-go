package alert

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

type memSink struct {
	mu      sync.Mutex
	alerts  []Alert
	failN   int
	calls   int
	lastErr error
}

func (m *memSink) Send(ctx context.Context, a Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls <= m.failN {
		return os.ErrInvalid // simulated transient delivery failure
	}
	m.alerts = append(m.alerts, a)
	return nil
}

// recordingSink is a memSink that also implements DeliveryChecker, simulating
// a sink that durably accepted an alert before the process died.
type recordingSink struct {
	memSink
	delivered map[string]bool
}

func newRecordingSink() *recordingSink {
	return &recordingSink{delivered: map[string]bool{}}
}

func (r *recordingSink) Send(ctx context.Context, a Alert) error {
	if err := r.memSink.Send(ctx, a); err != nil {
		return err
	}
	r.mu.Lock()
	r.delivered[a.Key()] = true
	r.mu.Unlock()
	return nil
}

func (r *recordingSink) Delivered(ctx context.Context, a Alert) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.delivered[a.Key()], nil
}

func failedResult(id string, severity model.Severity) model.RuleResult {
	return model.RuleResult{
		RuleID: id, Dataset: "d", Kind: model.KindUniqueness, Severity: severity,
		Status: model.StatusFailed, ErrorCode: "",
		Findings: []model.Finding{{Code: model.ReasonDuplicate, Evidence: []model.Evidence{{Line: 2}}}},
	}
}

func results3() []model.RuleResult {
	return []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityWarning),
		{RuleID: "r3", Status: model.StatusPassed, Severity: model.SeverityError, Findings: []model.Finding{}},
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

func TestDispatchExactlyOnceAcrossRepeats(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal.jsonl")
	sink := &memSink{}
	a, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	out, err := a.Dispatch(ctx, "b1", results3(), model.SeverityWarning)
	if err != nil {
		t.Fatal(err)
	}
	if out.Due != 2 || out.Sent != 2 || out.Retried != 0 || out.Pending != 0 {
		t.Fatalf("first dispatch outcome wrong: %+v", out)
	}
	// Repeating the batch must not duplicate anything and must report no work.
	for i := 0; i < 3; i++ {
		out, err := a.Dispatch(ctx, "b1", results3(), model.SeverityWarning)
		if err != nil || out.Sent != 0 || out.Retried != 0 || out.Pending != 0 {
			t.Fatalf("repeat %d: out=%+v err=%v", i, out, err)
		}
	}
	if len(sink.alerts) != 2 {
		t.Fatalf("sink received %d alerts, want 2", len(sink.alerts))
	}
	// 2 attempts + 2 delivered entries.
	if countLines(t, journal) != 4 {
		t.Fatalf("journal must hold 4 entries, got %d", countLines(t, journal))
	}
}

func TestDispatchRetriesBacklogAndReportsAccounting(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")

	// Run 1: all three deliveries fail -> 3 pending, nothing sent.
	sink1 := &memSink{failN: 1000}
	a1, _ := NewAlerter(sink1, journal)
	res := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
		failedResult("r3", model.SeverityError),
	}
	out, err := a1.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected propagated delivery error")
	}
	if out.Due != 3 || out.Sent != 0 || out.Pending != 3 {
		t.Fatalf("failed first run: %+v", out)
	}

	// Restart with a healthy sink (simulating retry of the batch). All three go
	// out as retried backlog; none is duplicated.
	sink2 := &memSink{}
	a2, _ := NewAlerter(sink2, journal)
	out, err = a2.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err != nil {
		t.Fatalf("retry dispatch: %v", err)
	}
	if out.Sent != 3 || out.Retried != 3 || out.Pending != 0 {
		t.Fatalf("retry outcome wrong: %+v", out)
	}
	if len(sink2.alerts) != 3 {
		t.Fatalf("sink got %d alerts, want 3", len(sink2.alerts))
	}
	// A third run fills nothing.
	out, _ = a2.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if out.Sent != 0 || out.Retried != 0 || out.Pending != 0 {
		t.Fatalf("post-retry outcome wrong: %+v", out)
	}
}

func TestDispatchPartialFailureThenRetryMixed(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	res := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
	}
	// First sink fails the second delivery (r1 succeeds).
	sink1 := &memSink{failN: 1}
	a1, _ := NewAlerter(sink1, journal)
	out, err := a1.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected delivery error")
	}
	if out.Sent != 1 || out.Pending != 1 || out.Retried != 0 {
		t.Fatalf("partial run: %+v", out)
	}
	// Retry: only r1 goes out, classified as retried; r2 never re-sends.
	sink2 := &memSink{}
	a2, _ := NewAlerter(sink2, journal)
	out, err = a2.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out.Sent != 1 || out.Retried != 1 || out.Pending != 0 {
		t.Fatalf("retry outcome: %+v", out)
	}
	if len(sink2.alerts) != 1 || sink2.alerts[0].RuleID != "r1" {
		t.Fatalf("retry must only deliver r1, got %+v", sink2.alerts)
	}
	// Combined across both sinks: exactly one notification per rule.
	if len(sink1.alerts)+len(sink2.alerts) != 2 {
		t.Fatalf("total deliveries %d, want 2", len(sink1.alerts)+len(sink2.alerts))
	}
}

func TestSinkSideGuardPreventsCrashWindowDuplicate(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	res := []model.RuleResult{failedResult("r1", model.SeverityError)}

	// Sink accepted the alert, but only the attempt marker (not delivered) was
	// journaled before the crash. A sink implementing DeliveryChecker prevents
	// a second external notification.
	sink1 := newRecordingSink()
	a1, _ := NewAlerter(sink1, journal)
	out, _ := a1.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if out.Sent != 1 {
		t.Fatalf("first run: %+v", out)
	}
	// Remove the trailing delivered entry, keep the attempt marker.
	trimJournalToAttempts(t, journal)

	sink2 := newRecordingSink()
	// Pretend the external destination still has the crashed delivery.
	sink2.delivered["b1\x00r1"] = true
	a2, _ := NewAlerter(sink2, journal)
	out, err := a2.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err != nil {
		t.Fatalf("guard dispatch: %v", err)
	}
	if out.Sent != 0 {
		t.Fatalf("crash-window alert must not be re-sent: %+v", out)
	}
	if len(sink2.alerts) != 0 {
		t.Fatalf("external duplicate happened: %+v", sink2.alerts)
	}
}

// trimJournalToAttempts drops delivered entries to simulate a crash after the
// sink accepted the alert but before the delivered entry was persisted.
func trimJournalToAttempts(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var kept []byte
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		var e journalEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		if e.Kind == kindAttempt {
			kept = append(kept, append(sc.Bytes(), '\n')...)
		}
	}
	if err := os.WriteFile(path, kept, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSeverityFiltering(t *testing.T) {
	dir := t.TempDir()
	a, _ := NewAlerter(&memSink{}, filepath.Join(dir, "j.jsonl"))
	results := []model.RuleResult{failedResult("warn", model.SeverityWarning)}
	out, err := a.Dispatch(context.Background(), "b1", results, model.SeverityError)
	if err != nil || out.Due != 0 || out.Sent != 0 {
		t.Fatalf("warning below error threshold: out=%+v err=%v", out, err)
	}
}

func TestFileSinkWritesJSONLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "alerts.jsonl")
	s, err := NewFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	al := Alert{BatchID: "b", RuleID: "r", Code: model.ReasonDuplicate}
	if err := s.Send(context.Background(), al); err != nil {
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
	// Delivered reports presence and absence precisely.
	if ok, err := s.Delivered(context.Background(), al); err != nil || !ok {
		t.Fatalf("Delivered present: ok=%v err=%v", ok, err)
	}
	missing := Alert{BatchID: "b", RuleID: "other"}
	if ok, _ := s.Delivered(context.Background(), missing); ok {
		t.Fatal("Delivered must be false for an unknown key")
	}
}

func TestFileSinkRepairsPartialTrailingLine(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "alerts.jsonl")
	s, err := NewFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	al := Alert{BatchID: "b", RuleID: "r1", Code: model.ReasonDuplicate}
	if err := s.Send(context.Background(), al); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: append a partial second line.
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"batch_id":"b","rule_id":"r2"`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// The next Send repairs the tail, then appends the new alert.
	if err := s.Send(context.Background(), Alert{BatchID: "b", RuleID: "r2", Code: model.ReasonDuplicate}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Delivered(context.Background(), al)
	if err != nil || !got {
		t.Fatalf("r1 must survive repair: got=%v err=%v", got, err)
	}
	got, _ = s.Delivered(context.Background(), Alert{BatchID: "b", RuleID: "r2"})
	if !got {
		t.Fatal("r2 must be present after repair and resend")
	}
}

func TestLegacyJournalEntriesTreatedAsDelivered(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	// An old-style entry without "kind".
	if err := os.WriteFile(journal, []byte(`{"batch_id":"b1","rule_id":"r1"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	a, _ := NewAlerter(sink, journal)
	res := []model.RuleResult{failedResult("r1", model.SeverityError), failedResult("r2", model.SeverityError)}
	out, err := a.Dispatch(context.Background(), "b1", res, model.SeverityWarning)
	if err != nil {
		t.Fatal(err)
	}
	if out.Sent != 1 || out.Retried != 0 {
		t.Fatalf("legacy entry must be honored as delivered, got %+v", out)
	}
	if len(sink.alerts) != 1 || sink.alerts[0].RuleID != "r2" {
		t.Fatalf("only r2 should be delivered, got %+v", sink.alerts)
	}
}
