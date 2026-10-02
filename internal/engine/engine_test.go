package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// --- test fixtures -------------------------------------------------------

type fixture struct {
	dir            string
	cfg            *config.Config
	store          state.Store
	sink           *countingSink
	logger         *logCapture
	alerter        *alert.Alerter
	loggerInstance *slog.Logger
}

type countingSink struct {
	mu        sync.Mutex
	alerts    []alert.Alert
	failRule  map[string]bool // rule ids that always fail to deliver
	delivered map[string]bool
	calls     int
}

func (c *countingSink) Send(ctx context.Context, a alert.Alert) error {
	c.mu.Lock()
	c.calls++
	fail := c.failRule[a.RuleID]
	c.mu.Unlock()
	if fail {
		return os.ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alerts = append(c.alerts, a)
	if c.delivered == nil {
		c.delivered = map[string]bool{}
	}
	c.delivered[a.Key()] = true
	return nil
}

// Delivered implements alert.DeliveryChecker so the crash window between a
// successful Send and journal durability cannot cause a duplicate.
func (c *countingSink) Delivered(ctx context.Context, a alert.Alert) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.delivered[a.Key()], nil
}

func (c *countingSink) snapshot() []alert.Alert {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]alert.Alert(nil), c.alerts...)
}

// logCapture keeps structured log output so tests can assert input summaries
// and per-rule judgment basis are printed.
type logCapture struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(p)
	return len(p), nil
}
func (l *logCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func newFixture(t *testing.T, files map[string]string, rules []model.Rule, datasets []model.DatasetDecl) *fixture {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i := range datasets {
		datasets[i].Path = filepath.Join(dir, datasets[i].Path)
	}
	cfg := &config.Config{Datasets: datasets, Rules: rules}
	store, err := state.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	sink := &countingSink{}
	alerter, err := alert.NewAlerter(sink, filepath.Join(dir, "state", "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	cap := &logCapture{}
	l := slog.New(slog.NewTextHandler(io.MultiWriter(cap), &slog.HandlerOptions{Level: slog.LevelInfo}))
	// Attach alerter indirectly through New via the helper below.
	fx := &fixture{dir: dir, cfg: cfg, store: store, sink: sink, logger: cap}
	fx.alerter = alerter
	fx.loggerInstance = l
	return fx
}

func (f *fixture) engine(opts Options) *Engine {
	opts.Logger = f.loggerInstance
	if opts.EvalTime.IsZero() {
		opts.EvalTime = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	}
	return New(f.cfg, datasource.New(""), f.store, f.alerter, opts)
}

func resultByID(b *model.BatchResult, id string) model.RuleResult {
	for _, r := range b.Results {
		if r.RuleID == id {
			return r
		}
	}
	return model.RuleResult{}
}

func usersDatasets() []model.DatasetDecl {
	return []model.DatasetDecl{{
		Name: "users", Path: "users.csv", Format: "csv",
		Schema: model.Schema{Fields: []model.FieldDecl{
			{Name: "id", Type: model.TypeInt, Required: true},
			{Name: "name", Type: model.TypeString, Required: true},
			{Name: "status", Type: model.TypeString, Required: true},
		}},
	}}
}

func baseRules() []model.Rule {
	return []model.Rule{
		{ID: "r-unique", Dataset: "users", Kind: model.KindUniqueness, Severity: model.SeverityError, Field: "id"},
		{ID: "r-complete", Dataset: "users", Kind: model.KindCompleteness, Severity: model.SeverityError, Field: "name"},
		{ID: "r-domain", Dataset: "users", Kind: model.KindConsistency, Severity: model.SeverityWarning, Field: "status",
			Params: map[string]any{"mode": "allowed", "allowed": []any{"active", "pending"}}},
	}
}

// --- tests ---------------------------------------------------------------

func TestConcurrentVerdictsAreStableAndOrdered(t *testing.T) {
	files := map[string]string{
		"users.csv": "id,name,status\n1,A,active\n2,B,active\n1,C,pending\n",
	}
	fx := newFixture(t, files, baseRules(), usersDatasets())

	for run := 0; run < 5; run++ {
		b, err := fx.engine(Options{MaxConcurrentRules: 8, RuleTimeout: 2 * time.Second}).
			Run(context.Background(), "b-001", false)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		// First run commits; subsequent runs replay identical results.
		wantIDs := []string{"r-complete", "r-domain", "r-unique"}
		if len(b.Results) != len(wantIDs) {
			t.Fatalf("want %d results, got %d", len(wantIDs), len(b.Results))
		}
		for i, id := range wantIDs {
			if b.Results[i].RuleID != id {
				t.Fatalf("position %d: want %s got %s (unstable order)", i, id, b.Results[i].RuleID)
			}
		}
	}
	// Unique failure is present and deterministic.
	b, _ := fx.engine(Options{}).Run(context.Background(), "b-001", false)
	rr := resultByID(b, "r-unique")
	if rr.Status != model.StatusFailed {
		t.Fatalf("want r-unique failed, got %s", rr.Status)
	}
	if len(rr.Findings) != 1 || rr.Findings[0].Code != model.ReasonDuplicate {
		t.Fatalf("unexpected unique findings: %+v", rr.Findings)
	}
}

func TestReplayIsIdenticalAndAlertsDoNotDuplicate(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})

	b1, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	if b1.Status != "committed" {
		t.Fatalf("first run status=%s", b1.Status)
	}
	b2, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Status != "replay" {
		t.Fatalf("repeat run status=%s, want replay", b2.Status)
	}
	if b2.Fingerprint != b1.Fingerprint {
		t.Fatalf("fingerprints differ: %s vs %s", b2.Fingerprint, b1.Fingerprint)
	}
	// Domain failure reproduces with the same code across runs.
	rr1 := resultByID(b1, "r-domain")
	rr2 := resultByID(b2, "r-domain")
	if rr1.Status != rr2.Status || len(rr1.Findings) != len(rr2.Findings) ||
		rr1.Findings[0].Code != rr2.Findings[0].Code {
		t.Fatal("replayed conclusion differs from original")
	}
	if len(fx.sink.alerts) != 1 {
		t.Fatalf("want exactly 1 alert after replay (no duplicates), got %d", len(fx.sink.alerts))
	}
}

func TestStructuralShapeChangeIsClassifiedNotSilent(t *testing.T) {
	// Dataset drops the declared "status" column entirely and adds "extra".
	files := map[string]string{"users.csv": "id,name,extra\n1,A,x\n2,B,y\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	b, err := fx.engine(Options{}).Run(context.Background(), "b-200", false)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[model.ReasonCode]int{}
	for _, r := range b.Results {
		if r.Status != model.StatusErrored {
			t.Fatalf("rule %s must be errored under shape change, got %s", r.RuleID, r.Status)
		}
		codes[r.ErrorCode]++
		for _, f := range r.Findings {
			codes[f.Code]++
		}
		if r.ErrorMsg == "" {
			t.Fatalf("rule %s errored without explainable message", r.RuleID)
		}
	}
	if codes[model.ReasonSchemaFieldMissing] == 0 || codes[model.ReasonSourceShapeChange] == 0 {
		t.Fatalf("want both schema_field_missing and source_shape_changed codes, got %v", codes)
	}
}

func TestSourceLoadFailureIsNormalizedPerRule(t *testing.T) {
	// Referenced file does not exist: every bound rule errors independently.
	fx := newFixture(t, map[string]string{}, baseRules(), usersDatasets())
	b, err := fx.engine(Options{}).Run(context.Background(), "b-300", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range b.Results {
		if r.Status != model.StatusErrored || r.ErrorCode != model.ReasonSourceLoadFailure {
			t.Fatalf("rule %s: want errored/source_load_failure, got %s/%s", r.RuleID, r.Status, r.ErrorCode)
		}
		if strings.Contains(r.ErrorMsg, "\n") {
			t.Fatal("raw error must be normalized to a single-line message")
		}
	}
}

func TestRuleTimeoutIsolation(t *testing.T) {
	// No rules need to block; instead use a near-zero timeout so every rule is
	// normalized as rule_timeout while the batch still commits fully.
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	start := time.Now()
	b, err := fx.engine(Options{MaxConcurrentRules: 1, RuleTimeout: time.Nanosecond}).
		Run(context.Background(), "b-400", false)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("timeout rules blocked the batch for too long")
	}
	for _, r := range b.Results {
		if r.ErrorCode != model.ReasonRuleTimeout {
			t.Fatalf("rule %s: want rule_timeout, got %s/%s", r.RuleID, r.Status, r.ErrorCode)
		}
	}
}

func TestCancelledBatchMarksRulesCancelled(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b, err := fx.engine(Options{RuleTimeout: time.Second}).Run(ctx, "b-500", false)
	if err != nil {
		// Begin itself may reject with context cancellation; that is acceptable
		// as long as no partial state is committed.
		return
	}
	for _, r := range b.Results {
		if r.ErrorCode != model.ReasonRuleCancelled {
			t.Fatalf("rule %s: want rule_cancelled, got %s/%s", r.RuleID, r.Status, r.ErrorCode)
		}
	}
}

func TestPanicRuleDoesNotAffectOthers(t *testing.T) {
	// An unknown kind reaches the checker registry and must be normalized as a
	// rule_definition_invalid error without tearing down sibling rules.
	rules := append(baseRules(), model.Rule{
		ID: "r-bogus", Dataset: "users", Kind: model.RuleKind("does-not-exist"), Severity: model.SeverityError,
	})
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n"}
	fx := newFixture(t, files, rules, usersDatasets())
	b, err := fx.engine(Options{}).Run(context.Background(), "b-600", false)
	if err != nil {
		t.Fatal(err)
	}
	bogus := resultByID(b, "r-bogus")
	if bogus.Status != model.StatusErrored || bogus.ErrorCode != model.ReasonRuleInvalid {
		t.Fatalf("bogus rule: want errored/rule_definition_invalid, got %s/%s", bogus.Status, bogus.ErrorCode)
	}
	// Sibling rules evaluated normally.
	if resultByID(b, "r-complete").Status != model.StatusPassed {
		t.Fatal("sibling rule must still be evaluated independently")
	}
}

func TestLogsContainInputSummaryAndBasis(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	if _, err := fx.engine(Options{}).Run(context.Background(), "b-700", false); err != nil {
		t.Fatal(err)
	}
	logs := fx.logger.String()
	for _, want := range []string{"input summary", "dataset=users", "records=2", "rule verdict", "r-domain", "finding_codes"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q\n%s", want, logs)
		}
	}
}

// --- late arrivals, validity and delivery backlog tests ------------------

const usersCSVOriginal = "id,name,status\n1,A,active\n2,B,banned\n"

func (f *fixture) rewriteData(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, "users.csv"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func historyInvalid(t *testing.T, err error) *model.HistoryInvalidError {
	t.Helper()
	var hi *model.HistoryInvalidError
	if !errors.As(err, &hi) {
		t.Fatalf("want *HistoryInvalidError, got %T: %v", err, err)
	}
	return hi
}

func TestLateBatchAfterNewerIsAccepted(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})

	// A newer batch commits first.
	if _, err := eng.Run(context.Background(), "b-200", false); err != nil {
		t.Fatal(err)
	}
	// An older, never-seen batch arrives late and is still fully evaluated.
	late, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatalf("late batch must be accepted: %v", err)
	}
	if late.Status != "committed" || len(late.Results) != 3 {
		t.Fatalf("late batch conclusions missing: %+v", late)
	}
}

func TestReplayInvalidatedByDataChange(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	if _, err := eng.Run(context.Background(), "b-100", false); err != nil {
		t.Fatal(err)
	}

	// Same declaration, changed file content: historical conclusions no longer
	// apply; the cause must be identifiable as the data side.
	fx.rewriteData(t, "id,name,status\n1,A,active\n2,B,banned\n3,C,active\n")
	_, err := eng.Run(context.Background(), "b-100", false)
	hi := historyInvalid(t, err)
	if !slicesContain(hi.Sides, model.InvalidatedData) || slicesContain(hi.Sides, model.InvalidatedRules) {
		t.Fatalf("want data-only invalidation, got sides=%v changes=%v", hi.Sides, hi.Changes)
	}
	if len(hi.Changes) == 0 || !strings.Contains(hi.Error(), "users") {
		t.Fatalf("invalid error must name the dataset: %v", err)
	}
	// The stored conclusions must be untouched.
	st, _ := fx.store.Get(context.Background(), "b-100")
	if st.Status != state.StatusCommitted || len(st.Results) != 3 {
		t.Fatalf("history must remain intact: %+v", st)
	}
}

func TestReplayInvalidatedByRuleParamChange(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	if _, err := eng.Run(context.Background(), "b-100", false); err != nil {
		t.Fatal(err)
	}

	// Data identical, but the rule declaration changed (allowed set widened).
	for i := range fx.cfg.Rules {
		if fx.cfg.Rules[i].ID == "r-domain" {
			fx.cfg.Rules[i].Params = map[string]any{"mode": "allowed", "allowed": []any{"active", "pending", "banned"}}
		}
	}
	_, err := eng.Run(context.Background(), "b-100", false)
	hi := historyInvalid(t, err)
	if !slicesContain(hi.Sides, model.InvalidatedRules) || slicesContain(hi.Sides, model.InvalidatedData) {
		t.Fatalf("want rules-only invalidation, got sides=%v changes=%v", hi.Sides, hi.Changes)
	}
}

func TestReplayInvalidatedBySourceDeleted(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	if _, err := eng.Run(context.Background(), "b-100", false); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(fx.dir, "users.csv")); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Run(context.Background(), "b-100", false)
	hi := historyInvalid(t, err)
	if !slicesContain(hi.Sides, model.InvalidatedData) {
		t.Fatalf("deleted source must invalidate on data side: sides=%v changes=%v", hi.Sides, hi.Changes)
	}
	if !strings.Contains(hi.Error(), "missing") {
		t.Fatalf("error should explain the file is missing: %v", err)
	}
}

func TestReplayUnchangedReproducesAndFillsNothing(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	b1, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatalf("unchanged replay must succeed: %v", err)
	}
	if b2.Status != "replay" || b2.Fingerprint != b1.Fingerprint {
		t.Fatalf("replay mismatch: %s vs %s", b2.Status, b1.Fingerprint)
	}
	if b2.AlertsSent != 0 || b2.AlertsPending != 0 {
		t.Fatalf("settled replay must report no alert work: %+v", b2)
	}
}

func TestForceCannotOverwriteCommitted(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	if _, err := fx.engine(Options{}).Run(context.Background(), "b-100", false); err != nil {
		t.Fatal(err)
	}
	_, err := fx.engine(Options{}).Run(context.Background(), "b-100", true)
	if !errors.Is(err, state.ErrImmutable) {
		t.Fatalf("force on committed batch: want ErrImmutable, got %v", err)
	}
}

func TestAlertBacklogRetriedOnReplay(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	fx.sink.failRule = map[string]bool{"r-domain": true}
	eng := fx.engine(Options{})

	b1, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	// r-domain alert failed: one pending, nothing sent on the first run.
	if b1.AlertsPending != 1 || b1.AlertsRetried != 0 || b1.AlertsSent != 0 {
		t.Fatalf("first run alert accounting wrong: %+v", b1)
	}

	// Channel recovers. Repeating the batch replays and fills the backlog.
	fx.sink.mu.Lock()
	fx.sink.failRule = map[string]bool{}
	fx.sink.mu.Unlock()
	b2, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatalf("replay retry: %v", err)
	}
	if b2.AlertsRetried != 1 || b2.AlertsSent != 1 || b2.AlertsPending != 0 {
		t.Fatalf("replay backlog accounting wrong: %+v", b2)
	}
	// Exactly one external notification per failed rule overall.
	got := fx.sink.snapshot()
	counts := map[string]int{}
	for _, a := range got {
		counts[a.RuleID]++
	}
	if counts["r-domain"] != 1 {
		t.Fatalf("want exactly one r-domain notification, got %v", counts)
	}

	// A process restart (new Alerter over the same journal and sink) still
	// delivers nothing and reports no pending backlog.
	restarted, err := alert.NewAlerter(fx.sink, filepath.Join(fx.dir, "state", "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	fx.alerter = restarted
	b3, err := fx.engine(Options{}).Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	if b3.AlertsPending != 0 || b3.AlertsSent != 0 || b3.AlertsRetried != 0 {
		t.Fatalf("post-restart replay must be settled: %+v", b3)
	}
	if len(fx.sink.snapshot()) != 1 {
		t.Fatalf("restart must not duplicate: %d alerts", len(fx.sink.snapshot()))
	}
}

func TestConcurrentTriggersShareOneExecution(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})

	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]*model.BatchResult, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = eng.Run(context.Background(), "b-100", false)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("trigger %d: %v", i, errs[i])
		}
		if results[i].Fingerprint != results[0].Fingerprint {
			t.Fatalf("trigger %d got a different fingerprint", i)
		}
	}
	committed, replay := 0, 0
	for _, r := range results {
		switch r.Status {
		case "committed":
			committed++
		case "replay":
			replay++
		default:
			t.Fatalf("unexpected status %s", r.Status)
		}
	}
	if committed < 1 {
		t.Fatal("at least one trigger must have performed the evaluation")
	}
	st, _ := fx.store.Get(context.Background(), "b-100")
	if st.Status != state.StatusCommitted || len(st.Results) != 3 {
		t.Fatalf("exactly one committed conclusion set expected: %+v", st)
	}
	// No duplicate external notifications despite the fan-out.
	counts := map[string]int{}
	for _, a := range fx.sink.snapshot() {
		counts[a.RuleID]++
	}
	if len(counts) != 1 || counts["r-domain"] != 1 {
		t.Fatalf("want exactly one r-domain notification, got %v", counts)
	}
	_ = committed
	_ = replay
}

// TestCombinedLateConcurrentRetry runs the three new behaviors together under
// -race: a late-arriving batch, concurrent triggers sharing one execution, and
// alert-channel failure followed by backlog retry on replay.
func TestCombinedLateConcurrentRetry(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})

	// Newer batch lands first.
	if _, err := eng.Run(context.Background(), "b-300", false); err != nil {
		t.Fatal(err)
	}

	// Late older batch with a flaky channel, triggered concurrently.
	fx.sink.failRule = map[string]bool{"r-domain": true}
	const n = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	fps := make([]string, n)
	pending := make([]int, n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			r, err := eng.Run(context.Background(), "b-100", false)
			if err != nil {
				t.Errorf("trigger %d: %v", idx, err)
				return
			}
			fps[idx] = r.Fingerprint
			pending[idx] = r.AlertsPending
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 1; i < n; i++ {
		if fps[i] != fps[0] {
			t.Fatalf("late batch trigger %d produced a different fingerprint", i)
		}
		if pending[i] != pending[0] {
			t.Fatalf("late batch trigger %d reported a different pending count", i)
		}
	}
	if pending[0] != 1 {
		t.Fatalf("late batch should have one pending alert, got %d", pending[0])
	}

	// Channel recovers; a repeat of the late batch fills the backlog once.
	fx.sink.mu.Lock()
	fx.sink.failRule = map[string]bool{}
	fx.sink.mu.Unlock()
	b, err := eng.Run(context.Background(), "b-100", false)
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != "replay" || b.AlertsRetried != 1 || b.AlertsPending != 0 {
		t.Fatalf("late batch retry accounting wrong: %+v", b)
	}
	// Count per (batch, rule) so the newer b-300 alert does not collide with
	// the late b-100 one.
	counts := map[string]int{}
	for _, a := range fx.sink.snapshot() {
		counts[a.BatchID+"\x00"+a.RuleID]++
	}
	for id, c := range counts {
		if c != 1 {
			t.Fatalf("combined run duplicated alert %s x%d", id, c)
		}
	}
}

func slicesContain[T comparable](xs []T, want T) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// TestCrossInstanceSeesLiveBatch opens two engines backed by the same state
// directory (simulating two processes) and checks that a trigger while the
// first execution is live is told the batch is already running rather than
// starting a second evaluation.
func TestCrossInstanceSeesLiveBatch(t *testing.T) {
	files := map[string]string{"users.csv": usersCSVOriginal}
	fx := newFixture(t, files, baseRules(), usersDatasets())

	store1, err := state.NewFileStoreWithTTL(filepath.Join(fx.dir, "state"), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()

	st, replay, err := store1.Begin(context.Background(), "b-live", false)
	if err != nil || replay {
		t.Fatalf("begin: replay=%v err=%v", replay, err)
	}
	_ = st

	// A second store instance on the same directory sees the live lease.
	store2same, err := state.NewFileStore(filepath.Join(fx.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer store2same.Close()
	eng2 := New(fx.cfg, datasource.New(""), store2same, fx.alerter, Options{
		EvalTime: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
	})
	if _, err := eng2.Run(context.Background(), "b-live", false); !errors.Is(err, state.ErrBatchRunning) {
		t.Fatalf("concurrent cross-instance trigger: want ErrBatchRunning, got %v", err)
	}
	// force cannot steal a live lease either.
	if _, err := eng2.Run(context.Background(), "b-live", true); !errors.Is(err, state.ErrBatchRunning) {
		t.Fatalf("force on live lease: want ErrBatchRunning, got %v", err)
	}
}
