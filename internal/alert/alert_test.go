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

	rep, err := a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil {
		t.Fatal("expected propagated delivery error")
	}
	if rep.SentThisRun != 0 || rep.Pending != 1 || rep.Expected != 1 || countLines(t, journal) != 0 {
		t.Fatalf("failed delivery must not be recorded: rep=%+v", rep)
	}
	// New alerter instance (simulating process restart) resumes and sends once.
	sink2 := &memSink{}
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
	// r2 and r3 go through in the same invocation.
	sink2 := &memSink{failN: 1}
	a2, _ := NewAlerter(sink2, journal)
	rep, err = a2.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err == nil || rep.SentThisRun != 2 || rep.Pending != 1 || rep.AlreadyDelivered != 0 {
		t.Fatalf("first partial recovery: %+v err=%v", rep, err)
	}

	// Another restart (new process) with a healthy channel fills the rest.
	sink3 := &memSink{}
	a3, _ := NewAlerter(sink3, journal)
	rep, err = a3.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.Pending != 0 || rep.AlreadyDelivered != 2 {
		t.Fatalf("final recovery: %+v err=%v", rep, err)
	}

	// Physically delivered exactly once per rule across all channels.
	got := map[string]bool{}
	for _, x := range append(append([]Alert{}, sink2.alerts...), sink3.alerts...) {
		if got[x.RuleID] {
			t.Fatalf("rule %s delivered twice", x.RuleID)
		}
		got[x.RuleID] = true
	}
	want := map[string]bool{"r1": true, "r2": true, "r3": true}
	if len(got) != 3 {
		t.Fatalf("delivered rules = %v, want %v", got, want)
	}
	if countLines(t, journal) != 3 {
		t.Fatalf("journal must hold exactly 3 entries")
	}
}

// TestDeliveredSinkRecordSurvivesLostJournal simulates a crash in the window
// between "sink accepted the alert" and "journal write": the FileSink file
// already holds the delivered line while the journal is empty. After a
// restart (brand-new Alerter) the delivered alert must not be re-sent, must
// be reported as already delivered, and the remaining backlog must go out.
func TestDeliveredSinkRecordSurvivesLostJournal(t *testing.T) {
	dir := t.TempDir()
	sinkPath := filepath.Join(dir, "alerts.jsonl")
	journal := filepath.Join(dir, "journal.jsonl")

	sink, err := NewFileSink(sinkPath)
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-crash: r1 physically went out; the process died before journaling.
	if err := sink.Send(context.Background(), Alert{BatchID: "b1", RuleID: "r1", Code: model.ReasonDuplicate}); err != nil {
		t.Fatal(err)
	}
	if countLines(t, journal) != 0 {
		t.Fatal("journal must be empty to simulate the lost write")
	}

	// Restart with a fresh alerter over the same sink file and journal.
	a, err = NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
	}
	rep, err := a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Expected != 2 || rep.AlreadyDelivered != 1 || rep.SentThisRun != 1 || rep.Pending != 0 {
		t.Fatalf("crash-window recovery report: %+v", rep)
	}
	// r1 physically exists exactly once; r2 was appended by the recovery run.
	if countLines(t, sinkPath) != 2 {
		t.Fatalf("sink file must hold exactly 2 lines (no duplicate r1)")
	}
	delivered, err := sink.Delivered(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !delivered["b1\x00r1"] || !delivered["b1\x00r2"] || len(delivered) != 2 {
		t.Fatalf("delivered set mismatch: %v", delivered)
	}
	// A further replay is completely idle.
	rep, err = a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 0 || rep.AlreadyDelivered != 2 || rep.Pending != 0 {
		t.Fatalf("idle replay: rep=%+v err=%v", rep, err)
	}
	if countLines(t, sinkPath) != 2 {
		t.Fatal("idle replay must not append to the sink file")
	}
}

// TestTornSinkLineIsRedelivered simulates a crash mid-write: the trailing
// partial line is not a durable delivery, so the alert is re-sent once and
// the sink ends with exactly one complete line for that (batch, rule).
func TestTornSinkLineIsRedelivered(t *testing.T) {
	dir := t.TempDir()
	sinkPath := filepath.Join(dir, "alerts.jsonl")
	journal := filepath.Join(dir, "journal.jsonl")

	sink, err := NewFileSink(sinkPath)
	if err != nil {
		t.Fatal(err)
	}
	// Torn trailing write: half a JSON line, no newline.
	if err := os.WriteFile(sinkPath, []byte(`{"batch_id":"b1","rule_id":"r1`), 0o644); err != nil {
		t.Fatal(err)
	}
	delivered, err := sink.Delivered(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 0 {
		t.Fatalf("torn line must not count as delivered: %v", delivered)
	}

	a, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := a.Dispatch(context.Background(), "b1",
		[]model.RuleResult{failedResult("r1", model.SeverityError)}, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 1 || rep.Pending != 0 {
		t.Fatalf("torn line must be redelivered: rep=%+v err=%v", rep, err)
	}
	// Exactly one *valid* line for r1 afterwards (the torn prefix is inert).
	delivered, err = sink.Delivered(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !delivered["b1\x00r1"] || len(delivered) != 1 {
		t.Fatalf("delivered set after redelivery: %v", delivered)
	}
	rep, err = a.Dispatch(context.Background(), "b1",
		[]model.RuleResult{failedResult("r1", model.SeverityError)}, model.SeverityWarning)
	if err != nil || rep.SentThisRun != 0 || rep.AlreadyDelivered != 1 {
		t.Fatalf("no further sends after redelivery: rep=%+v err=%v", rep, err)
	}
}

// TestConcurrentDispatchersDoNotDuplicate runs two independent Alerter
// instances (simulating two processes sharing one journal and one sink file)
// dispatching the same batch concurrently. Every (batch, rule) pair must be
// physically delivered exactly once, and the two reports must partition the
// expected alerts without overlap.
func TestConcurrentDispatchersDoNotDuplicate(t *testing.T) {
	dir := t.TempDir()
	sinkPath := filepath.Join(dir, "alerts.jsonl")
	journal := filepath.Join(dir, "journal.jsonl")

	sink, err := NewFileSink(sinkPath)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := NewAlerter(sink, journal)
	if err != nil {
		t.Fatal(err)
	}

	results := []model.RuleResult{
		failedResult("r1", model.SeverityError),
		failedResult("r2", model.SeverityError),
		failedResult("r3", model.SeverityWarning),
	}
	reps := make([]model.AlertReport, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, a := range []*Alerter{a1, a2} {
		wg.Add(1)
		go func(i int, a *Alerter) {
			defer wg.Done()
			<-start
			reps[i], errs[i] = a.Dispatch(context.Background(), "b1", results, model.SeverityWarning)
		}(i, a)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("dispatcher %d: %v", i, err)
		}
	}
	total := reps[0].SentThisRun + reps[1].SentThisRun
	if total != 3 {
		t.Fatalf("exactly 3 sends across both dispatchers, got %d (%+v / %+v)", total, reps[0], reps[1])
	}
	delivered, err := sink.Delivered(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 3 {
		t.Fatalf("sink must hold exactly 3 distinct deliveries, got %v", delivered)
	}
	if countLines(t, sinkPath) != 3 {
		t.Fatalf("sink file must contain exactly 3 lines (no duplicates)")
	}
}
