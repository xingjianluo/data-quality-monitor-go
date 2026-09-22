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

	n, err := a.Dispatch(ctx, "b1", results, model.SeverityWarning)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 alerts, got %d", n)
	}
	// Repeating the batch must not duplicate anything.
	for i := 0; i < 3; i++ {
		n, err := a.Dispatch(ctx, "b1", results, model.SeverityWarning)
		if err != nil || n != 0 {
			t.Fatalf("repeat %d: n=%d err=%v", i, n, err)
		}
	}
	if len(sink.alerts) != 2 {
		t.Fatalf("sink received %d alerts, want 2", len(sink.alerts))
	}
	if countLines(t, journal) != 2 {
		t.Fatalf("journal must hold exactly 2 entries")
	}
}

func TestDispatchResumesAfterPartialFailure(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "j.jsonl")
	sink := &memSink{failN: 1} // first delivery fails -> must not be journaled
	a, _ := NewAlerter(sink, journal)
	results := []model.RuleResult{failedResult("r1", model.SeverityError)}

	n, err := a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected propagated delivery error")
	}
	if n != 0 || countLines(t, journal) != 0 {
		t.Fatalf("failed delivery must not be recorded: n=%d", n)
	}
	// New alerter instance (simulating process restart) resumes and sends once.
	sink2 := &memSink{}
	a2, _ := NewAlerter(sink2, journal)
	n, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || n != 1 {
		t.Fatalf("resume: n=%d err=%v", n, err)
	}
	n, _ = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if n != 0 {
		t.Fatalf("second resume must not duplicate: n=%d", n)
	}
}

func TestSeverityFiltering(t *testing.T) {
	dir := t.TempDir()
	a, _ := NewAlerter(&memSink{}, filepath.Join(dir, "j.jsonl"))
	results := []model.RuleResult{failedResult("warn", model.SeverityWarning)}
	n, err := a.Dispatch(context.Background(), "b1", results, model.SeverityError)
	if err != nil || n != 0 {
		t.Fatalf("warning below error threshold: n=%d err=%v", n, err)
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
