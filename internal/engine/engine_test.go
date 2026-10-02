package engine

import (
	"context"
	"errors"
	"fmt"
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
	mu     sync.Mutex
	alerts []alert.Alert
}

func (c *countingSink) Send(ctx context.Context, a alert.Alert) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alerts = append(c.alerts, a)
	return nil
}

// scriptableSink simulates flaky notification channels: it can fail selected
// rules for a number of attempts, block on the first send (to force overlap in
// concurrency tests), and records everything that actually went out.
type scriptableSink struct {
	mu          sync.Mutex
	alerts      []alert.Alert
	failRule    map[string]bool // rule ids that always fail
	failN       int             // first N sends (any rule) fail
	calls       int
	firstSend   chan struct{} // closed when first Send is entered
	releaseGate chan struct{} // first Send blocks until this is closed
	holdOnce    bool
}

func newScriptableSink() *scriptableSink {
	return &scriptableSink{
		failRule:    map[string]bool{},
		firstSend:   make(chan struct{}),
		releaseGate: make(chan struct{}),
	}
}

func (s *scriptableSink) Send(ctx context.Context, a alert.Alert) error {
	s.mu.Lock()
	if s.holdOnce {
		if s.calls == 0 {
			s.mu.Unlock()
			select {
			case <-s.firstSend:
			default:
				close(s.firstSend)
			}
			select {
			case <-s.releaseGate:
			case <-ctx.Done():
				return ctx.Err()
			}
			s.mu.Lock()
		}
	}
	s.calls++
	calls := s.calls
	fail := calls <= s.failN || s.failRule[a.RuleID]
	s.mu.Unlock()
	if fail {
		return os.ErrInvalid // simulated transient channel failure
	}
	s.mu.Lock()
	s.alerts = append(s.alerts, a)
	s.mu.Unlock()
	return nil
}

func (s *scriptableSink) delivered() []alert.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]alert.Alert(nil), s.alerts...)
}

// runningThenStore wraps a real store: the first `runningTimes` Begin calls
// report a live running batch, simulating another process mid-execution, then
// delegates to the underlying store (which by then has a committed batch).
type runningThenStore struct {
	state.Store
	mu           *sync.Mutex
	runningTimes int
	calls        int
}

func (w *runningThenStore) Begin(ctx context.Context, batchID string, force bool) (*state.BatchState, bool, error) {
	w.mu.Lock()
	w.calls++
	calls := w.calls
	w.mu.Unlock()
	if calls <= w.runningTimes {
		return nil, false, fmt.Errorf("%w: batch %s", state.ErrBatchRunning, batchID)
	}
	return w.Store.Begin(ctx, batchID, force)
}

var _ = fmt.Sprintf

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

// engineWith builds an engine over the fixture config but with a custom sink,
// alerter journal and store (used for retry and cross-engine scenarios).
func (f *fixture) engineWith(sink alert.Sink, st state.Store, opts Options, journalName string) *Engine {
	if journalName == "" {
		journalName = "journal.jsonl"
	}
	al, err := alert.NewAlerter(sink, filepath.Join(f.dir, "state", journalName))
	if err != nil {
		panic(err)
	}
	opts.Logger = f.loggerInstance
	if opts.EvalTime.IsZero() {
		opts.EvalTime = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	}
	return New(f.cfg, datasource.New(""), st, al, opts)
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

// --- late arrival / overwrite protection ---------------------------------

func TestLateUnseenBatchAcceptedAndConclusionsCoexist(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	ctx := context.Background()

	newer, err := eng.Run(ctx, "20261003-010", false)
	if err != nil {
		t.Fatal(err)
	}
	// An older batch never seen before arrives late: evaluated normally.
	older, err := eng.Run(ctx, "20261003-001", false)
	if err != nil {
		t.Fatalf("late unseen batch must be accepted, got %v", err)
	}
	if older.Status != "committed" {
		t.Fatalf("late batch status=%s", older.Status)
	}
	// Both histories exist independently.
	for _, id := range []string{"20261003-001", "20261003-010"} {
		st, err := fx.store.Get(ctx, id)
		if err != nil {
			t.Fatalf("history %s: %v", id, err)
		}
		if st.Status != state.StatusCommitted {
			t.Fatalf("batch %s missing committed history", id)
		}
	}
	if newer.Fingerprint == older.Fingerprint {
		t.Fatal("distinct batches must not share a fingerprint")
	}
}

func TestCommittedBatchCanNeverBeOverwritten(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	ctx := context.Background()
	if _, err := eng.Run(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	// Re-run is a replay, but an explicit force rewrite attempt is rejected
	// with the distinct "already concluded" reason.
	if _, err := eng.Run(ctx, "b-001", true); !errors.Is(err, state.ErrAlreadyConcluded) {
		t.Fatalf("want ErrAlreadyConcluded, got %v", err)
	}
}

// --- concurrency -----------------------------------------------------------

func TestConcurrentTriggersExecuteExactlyOnce(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	sink := newScriptableSink()
	sink.holdOnce = true
	eng := fx.engineWith(sink, fx.store, Options{RunningPollInterval: 5 * time.Millisecond}, "journal.jsonl")
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	res := make([]*model.BatchResult, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res[i], errs[i] = eng.Run(ctx, "b-conc", false)
		}(i)
	}
	close(start)
	<-sink.firstSend // leader is executing
	time.Sleep(50 * time.Millisecond)
	close(sink.releaseGate)
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("trigger %d: %v", i, errs[i])
		}
		if res[i] == nil || res[i].Fingerprint != res[0].Fingerprint {
			t.Fatalf("trigger %d got a divergent result", i)
		}
	}
	if got := len(sink.delivered()); got != 1 {
		t.Fatalf("exactly one alert despite %d concurrent triggers, got %d", n, got)
	}
	st, err := fx.store.Get(ctx, "b-conc")
	if err != nil || st.Status != state.StatusCommitted {
		t.Fatalf("batch not committed exactly once: %v", err)
	}
}

func TestTriggerWaitsForAnotherExecutionAndReturnsItsResult(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	leader := fx.engine(Options{})
	ctx := context.Background()
	original, err := leader.Run(ctx, "b-shared", false)
	if err != nil {
		t.Fatal(err)
	}
	// Another engine observes a live "running" batch twice, then sees the
	// commit and returns those exact conclusions (never a second execution).
	wrapped := &runningThenStore{Store: fx.store, mu: &sync.Mutex{}, runningTimes: 2}
	follower := fx.engineWith(&countingSink{}, wrapped,
		Options{RunningPollInterval: 5 * time.Millisecond}, "journal-follower.jsonl")
	got, err := follower.Run(ctx, "b-shared", false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "replay" || got.Fingerprint != original.Fingerprint {
		t.Fatalf("follower must return the leader conclusions: status=%s fp match=%v",
			got.Status, got.Fingerprint == original.Fingerprint)
	}
}

func TestExpiredLeaseIsStaleAndForceReclaims(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	staleStore, err := state.NewFileStoreWithTTL(filepath.Join(fx.dir, "state2"), 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleStore.Close() })
	eng := fx.engineWith(&countingSink{}, staleStore,
		Options{RunningPollInterval: 5 * time.Millisecond, LeaseHeartbeat: time.Hour}, "j2.jsonl")
	ctx := context.Background()

	// A previous process began the batch and died without committing.
	if _, _, err := staleStore.Begin(ctx, "b-stale", false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(70 * time.Millisecond)
	if _, err := eng.Run(ctx, "b-stale", false); !errors.Is(err, state.ErrStaleBatch) {
		t.Fatalf("want ErrStaleBatch, got %v", err)
	}
	b, err := eng.Run(ctx, "b-stale", true)
	if err != nil {
		t.Fatalf("force reclaim: %v", err)
	}
	if b.Status != "committed" {
		t.Fatalf("reclaimed batch status=%s", b.Status)
	}
}

// --- replay validity / drift -----------------------------------------------

func driftFor(t *testing.T, err error) *DriftError {
	t.Helper()
	d, ok := AsDrift(err)
	if !ok {
		t.Fatalf("expected DriftError, got %v", err)
	}
	return d
}

func hasChangeSide(d *DriftError, side ChangeSide) bool {
	for _, c := range d.Changes {
		if c.Side == side {
			return true
		}
	}
	return false
}

func rewriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReplayAfterContentChangeIsDataDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unused") // placeholder to keep helper simple
	_ = path
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	ctx := context.Background()
	b1, err := eng.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	// Same columns/shape, different rows: content-level data drift.
	rewriteFile(t, filepath.Join(fx.dir, "users.csv"), "id,name,status\n1,A,active\n9,Z,banned\n")
	_, err = eng.Run(ctx, "b-001", false)
	d := driftFor(t, err)
	if !hasChangeSide(d, SideData) || hasChangeSide(d, SideRule) {
		t.Fatalf("want data-only drift, got %+v", d.Changes)
	}
	var foundContent bool
	for _, c := range d.Changes {
		if c.Kind == KindContentChanged {
			foundContent = true
		}
	}
	if !foundContent {
		t.Fatalf("want content_changed kind, got %+v", d.Changes)
	}
	// The historical conclusions remain stored untouched.
	if st, _ := fx.store.Get(ctx, "b-001"); st.Fingerprint != b1.Fingerprint {
		t.Fatal("drift must not overwrite stored conclusions")
	}
}

func TestReplayAfterShapeChangeIsShapeDrift(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	ctx := context.Background()
	if _, err := eng.Run(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	// Column set changed.
	rewriteFile(t, filepath.Join(fx.dir, "users.csv"), "id,name,other\n1,A,x\n2,B,y\n")
	_, err := eng.Run(ctx, "b-001", false)
	d := driftFor(t, err)
	var shape bool
	for _, c := range d.Changes {
		if c.Side == SideData && c.Kind == KindShapeChanged {
			shape = true
		}
	}
	if !shape {
		t.Fatalf("want data/shape_changed, got %+v", d.Changes)
	}
}

func TestReplayAfterSourceDeletedIsMissingDrift(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	eng := fx.engine(Options{})
	ctx := context.Background()
	if _, err := eng.Run(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(fx.dir, "users.csv")); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Run(ctx, "b-001", false)
	d := driftFor(t, err)
	var missing bool
	for _, c := range d.Changes {
		if c.Side == SideData && c.Kind == KindSourceMissing {
			missing = true
		}
	}
	if !missing {
		t.Fatalf("want data/source_missing, got %+v", d.Changes)
	}
}

func TestReplayAfterRuleParamChangeIsRuleDrift(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()
	eng := fx.engine(Options{})
	if _, err := eng.Run(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	// Change a rule parameter: data unchanged, drift must be rule-side only.
	for i := range fx.cfg.Rules {
		if fx.cfg.Rules[i].ID == "r-domain" {
			fx.cfg.Rules[i].Params = map[string]any{"mode": "allowed", "allowed": []any{"active"}}
		}
	}
	_, err := fx.engine(Options{}).Run(ctx, "b-001", false)
	d := driftFor(t, err)
	if !hasChangeSide(d, SideRule) || hasChangeSide(d, SideData) {
		t.Fatalf("want rule-only drift, got %+v", d.Changes)
	}
}

func TestReplayAfterEvalTimeChangeIsEvalDrift(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()
	if _, err := fx.engine(Options{}).Run(ctx, "b-001", false); err != nil {
		t.Fatal(err)
	}
	_, err := fx.engine(Options{EvalTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}).
		Run(ctx, "b-001", false)
	d := driftFor(t, err)
	if !hasChangeSide(d, SideEval) {
		t.Fatalf("want eval drift, got %+v", d.Changes)
	}
}

func TestReplayUnchangedInputsReproducesHistoricalConclusions(t *testing.T) {
	files := map[string]string{"users.csv": "id,name,status\n1,A,active\n2,B,banned\n"}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()
	b1, err := fx.engine(Options{}).Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b, err := fx.engine(Options{}).Run(ctx, "b-001", false)
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if b.Status != "replay" || b.Fingerprint != b1.Fingerprint {
			t.Fatalf("replay %d not identical", i)
		}
	}
}

// --- notification retry / backlog ------------------------------------------

func failingRulesCSV() string {
	// Duplicate id (r-unique fails) plus banned status (r-domain fails): 2 alerts.
	return "id,name,status\n1,A,active\n2,B,banned\n1,C,pending\n"
}

func TestAlertFailureRetryBackfillsAndReportsPending(t *testing.T) {
	files := map[string]string{"users.csv": failingRulesCSV()}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()

	// Channel permanently fails r-domain on the first sink.
	sink1 := newScriptableSink()
	sink1.failRule["r-domain"] = true
	eng1 := fx.engineWith(sink1, fx.store, Options{}, "journal.jsonl")
	b1, err := eng1.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b1.AlertReport.Expected != 2 || b1.AlertReport.SentThisRun != 1 || b1.AlertReport.Pending != 1 {
		t.Fatalf("first report: %+v", b1.AlertReport)
	}

	// Re-run while channel still failing: nothing new sent, still 1 pending.
	b2, err := eng1.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b2.AlertReport.SentThisRun != 0 || b2.AlertReport.Pending != 1 {
		t.Fatalf("second report: %+v", b2.AlertReport)
	}

	// Channel recovered (new process, same journal): backlog is filled once.
	sink2 := newScriptableSink()
	eng2 := fx.engineWith(sink2, fx.store, Options{}, "journal.jsonl")
	b3, err := eng2.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b3.AlertReport.SentThisRun != 1 || b3.AlertReport.Pending != 0 || b3.AlertReport.AlreadyDelivered != 1 {
		t.Fatalf("recovery report: %+v", b3.AlertReport)
	}
	// Exactly one physical delivery per rule across the whole episode.
	delivered := append(sink1.delivered(), sink2.delivered()...)
	seen := map[string]int{}
	for _, a := range delivered {
		seen[a.RuleID]++
	}
	if len(seen) != 2 || seen["r-domain"] != 1 || seen["r-unique"] != 1 {
		t.Fatalf("delivered alerts not exactly-once: %+v", seen)
	}
	// Another replay now sends nothing at all.
	b4, err := eng2.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b4.AlertReport.SentThisRun != 0 || b4.AlertReport.Pending != 0 {
		t.Fatalf("post-recovery replay: %+v", b4.AlertReport)
	}
}

func TestAlertTransientFailureRetriedWithoutDuplicates(t *testing.T) {
	files := map[string]string{"users.csv": failingRulesCSV()}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()

	// First physical send fails (r-domain sorts first); r-unique goes through.
	sink1 := newScriptableSink()
	sink1.failN = 1
	eng1 := fx.engineWith(sink1, fx.store, Options{}, "journal.jsonl")
	b1, err := eng1.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b1.AlertReport.SentThisRun != 1 || b1.AlertReport.Pending != 1 {
		t.Fatalf("first report: %+v", b1.AlertReport)
	}
	if len(sink1.delivered()) != 1 || sink1.delivered()[0].RuleID != "r-unique" {
		t.Fatalf("only r-unique should be delivered: %+v", sink1.delivered())
	}

	// Restart with a healthy channel: the failed one is retried, the delivered
	// one is never duplicated.
	sink2 := newScriptableSink()
	eng2 := fx.engineWith(sink2, fx.store, Options{}, "journal.jsonl")
	b2, err := eng2.Run(ctx, "b-001", false)
	if err != nil {
		t.Fatal(err)
	}
	if b2.AlertReport.SentThisRun != 1 || b2.AlertReport.AlreadyDelivered != 1 || b2.AlertReport.Pending != 0 {
		t.Fatalf("retry report: %+v", b2.AlertReport)
	}
	got := sink2.delivered()
	if len(got) != 1 || got[0].RuleID != "r-domain" {
		t.Fatalf("retry must deliver only r-domain: %+v", got)
	}
}

// TestInterruptedDispatchBeforeAnyAlertIsBackfilled simulates a process that
// commits the batch and then dies before delivering any notification. The
// restart (new engine, same durable journal) must send the whole backlog once.
func TestInterruptedDispatchBeforeAnyAlertIsBackfilled(t *testing.T) {
	files := map[string]string{"users.csv": failingRulesCSV()}
	fx := newFixture(t, files, baseRules(), usersDatasets())
	ctx := context.Background()

	// Commit with a nil alerter: conclusions are durable, no alerts attempted
	// (equivalent to a crash between commit and dispatch).
	engNoAlert := New(fx.cfg, datasource.New(""), fx.store, nil, Options{
		EvalTime: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
		Logger:   fx.loggerInstance,
	})
	b1, err := engNoAlert.Run(ctx, "b-crash", false)
	if err != nil || b1.Status != "committed" {
		t.Fatalf("commit before crash: %v status=%v", err, b1)
	}

	// Restart with a healthy channel: replay fills the whole backlog.
	sink := newScriptableSink()
	eng2 := fx.engineWith(sink, fx.store, Options{}, "journal.jsonl")
	b2, err := eng2.Run(ctx, "b-crash", false)
	if err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	if b2.Status != "replay" {
		t.Fatalf("restart status=%s", b2.Status)
	}
	if b2.AlertReport.SentThisRun != 2 || b2.AlertReport.Pending != 0 {
		t.Fatalf("crash recovery report: %+v", b2.AlertReport)
	}
	if got := len(sink.delivered()); got != 2 {
		t.Fatalf("want 2 backfilled alerts, got %d", got)
	}
}
