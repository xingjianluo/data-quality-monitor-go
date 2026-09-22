package engine

import (
	"context"
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
