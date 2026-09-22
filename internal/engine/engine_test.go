package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/logx"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/monitor"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// 测试夹具：在临时目录准备两个数据集文件与公共规则集。
type fixture struct {
	dir   string
	decls []model.DatasetDecl
	rules []model.RuleSpec
	wm    time.Time
}

func newFixture(t *testing.T, ordersJSON, usersJSON string) *fixture {
	t.Helper()
	dir := t.TempDir()
	orders := filepath.Join(dir, "orders.json")
	users := filepath.Join(dir, "users.json")
	if err := os.WriteFile(orders, []byte(ordersJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(users, []byte(usersJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	ordersDecl := model.DatasetDecl{Name: "orders", Path: orders, Columns: []model.Column{
		{Name: "id", Type: model.TypeInt},
		{Name: "user_id", Type: model.TypeInt},
		{Name: "status", Type: model.TypeString},
		{Name: "updated_at", Type: model.TypeTime},
	}}
	usersDecl := model.DatasetDecl{Name: "users", Path: users, Columns: []model.Column{
		{Name: "id", Type: model.TypeInt},
		{Name: "country", Type: model.TypeString},
	}}
	rules := []model.RuleSpec{
		{ID: "orders-id-complete", Type: model.RuleCompleteness, Dataset: "orders",
			Columns: []string{"id"}, MinNonNull: 1.0},
		{ID: "orders-id-unique", Type: model.RuleUniqueness, Dataset: "orders", Columns: []string{"id"}},
		{ID: "orders-status-enum", Type: model.RuleConsistency, Dataset: "orders",
			Columns: []string{"status"}, Allowed: []string{"paid", "pending", "refunded"}},
		{ID: "orders-fresh", Type: model.RuleFreshness, Dataset: "orders",
			Columns: []string{"updated_at"}, MaxAge: "24h"},
		{ID: "users-id-unique", Type: model.RuleUniqueness, Dataset: "users", Columns: []string{"id"}},
		{ID: "users-country-enum", Type: model.RuleConsistency, Dataset: "users",
			Columns: []string{"country"}, Allowed: []string{"CN", "US"}},
	}
	return &fixture{
		dir:   dir,
		decls: []model.DatasetDecl{ordersDecl, usersDecl},
		rules: rules,
		wm:    time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
	}
}

func goodOrders() string {
	return `[
		{"id": 1, "user_id": 10, "status": "paid", "updated_at": "2026-09-23T08:00:00Z"},
		{"id": 2, "user_id": 11, "status": "pending", "updated_at": "2026-09-23T09:00:00Z"},
		{"id": 3, "user_id": 12, "status": "refunded", "updated_at": "2026-09-23T10:00:00Z"}
	]`
}

func goodUsers() string {
	return `[
		{"id": 10, "country": "CN"},
		{"id": 11, "country": "US"},
		{"id": 12, "country": "CN"}
	]`
}

func findRule(res model.BatchResult, id string) model.RuleResult {
	for _, r := range res.Results {
		if r.RuleID == id {
			return r
		}
	}
	return model.RuleResult{}
}

func newEngine(t *testing.T, store state.Store, sink monitor.AlertSink) *Engine {
	t.Helper()
	logger := logx.New(false)
	return New(store, monitor.New(logger), sink, logger, Config{Concurrency: 4, PerRuleTimeout: time.Second})
}

func (fx *fixture) req(batchID string, ordinal uint64) RunRequest {
	return RunRequest{BatchID: batchID, Ordinal: ordinal, Watermark: fx.wm,
		Datasets: fx.decls, Rules: fx.rules}
}

// reqFor 支持自定义批次 ID（同 ordinal 多批次组合场景）。
func (fx *fixture) reqFor(prefix string, ordinal uint64) RunRequest {
	return RunRequest{BatchID: fmt.Sprintf("%s-%d", prefix, ordinal), Ordinal: ordinal,
		Watermark: fx.wm, Datasets: fx.decls, Rules: fx.rules}
}

func TestGreenRunAndDeterministicReplay(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	sink := monitor.NewMemorySink()
	eng := newEngine(t, store, sink)
	ctx := context.Background()

	res1, replayed, err := eng.Run(ctx, fx.req("b1", 1))
	if err != nil || replayed || res1.Status() != model.StatusPass {
		t.Fatalf("first run: err=%v replayed=%v status=%s", err, replayed, res1.Status())
	}
	if len(sink.Snapshot()) != 0 {
		t.Fatalf("green run must not alert, got %d", len(sink.Snapshot()))
	}

	// 同批次重复执行：结论完全一致、replayed=true、无告警。
	res2, replayed, err := eng.Run(ctx, fx.req("b1", 1))
	if err != nil || !replayed {
		t.Fatalf("replay: err=%v replayed=%v", err, replayed)
	}
	if res2.Status() != res1.Status() || len(res2.Results) != len(res1.Results) {
		t.Fatal("replayed result diverges from original")
	}
	if len(sink.Snapshot()) != 0 {
		t.Fatalf("replay must not alert again, got %d", len(sink.Snapshot()))
	}
	rec, ok := store.Get("b1")
	if !ok || !rec.AlertsSent {
		t.Fatal("batch should be committed with alerts confirmed")
	}
}

func TestRuleFailureReasonsAreStableAndDistinguishable(t *testing.T) {
	badOrders := `[
		{"id": 1, "user_id": 10, "status": "paid", "updated_at": "2026-09-23T10:00:00Z"},
		{"id": 1, "user_id": 10, "status": "weird", "updated_at": "2026-09-20T10:00:00Z"},
		{"id": null, "user_id": "x", "status": null, "updated_at": "2026-09-23T11:00:00Z"}
	]`
	badUsers := `[
		{"id": 10, "country": "CN"},
		{"id": 11, "country": "DE"}
	]`
	fx := newFixture(t, badOrders, badUsers)
	store := state.NewMemoryStore()
	sink := monitor.NewMemorySink()
	eng := newEngine(t, store, sink)

	res, _, err := eng.Run(context.Background(), fx.req("b2", 2))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status() != model.StatusFail && res.Status() != model.StatusError {
		t.Fatalf("want non-pass, got %s", res.Status())
	}
	want := map[string]model.ReasonCode{
		"orders.structural":  model.ReasonTypeMismatch,
		"orders-id-complete": model.ReasonRuleViolation,
		"orders-id-unique":   model.ReasonRuleViolation,
		"orders-status-enum": model.ReasonRuleViolation,
		"orders-fresh":       model.ReasonRuleViolation,
		"users-country-enum": model.ReasonRuleViolation,
	}
	for id, reason := range want {
		r := findRule(res, id)
		if r.RuleID == "" {
			t.Fatalf("rule %s missing", id)
		}
		if len(r.Findings) == 0 || r.Findings[0].Reason != reason {
			t.Fatalf("rule %s: want reason %s, got %+v", id, reason, r.Findings)
		}
	}
	// 告警在失败批次上产生，且重跑不重复。
	firstAlerts := len(sink.Snapshot())
	if firstAlerts == 0 {
		t.Fatal("failing batch must alert once")
	}
	if _, _, err := eng.Run(context.Background(), fx.req("b2", 2)); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Snapshot()); got != firstAlerts {
		t.Fatalf("alerts duplicated on rerun: %d -> %d", firstAlerts, got)
	}
}

func TestSchemaDriftAndFieldMissingAcrossBatches(t *testing.T) {
	// b1 含 updated_at；b2 形态变化（去掉 updated_at、增加 currency）。
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())
	if _, _, err := eng.Run(context.Background(), fx.req("b1", 1)); err != nil {
		t.Fatal(err)
	}

	driftedOrders := `[
		{"id": 4, "user_id": 13, "status": "paid", "currency": "CNY"},
		{"id": 5, "user_id": 14, "status": "paid", "currency": "USD"}
	]`
	if err := os.WriteFile(fx.decls[0].Path, []byte(driftedOrders), 0o644); err != nil {
		t.Fatal(err)
	}
	res, _, err := eng.Run(context.Background(), fx.req("b2", 2))
	if err != nil {
		t.Fatal(err)
	}
	str := findRule(res, "orders.structural")
	reasons := map[model.ReasonCode]bool{}
	for _, f := range str.Findings {
		reasons[f.Reason] = true
	}
	if !reasons[model.ReasonSchemaDrift] {
		t.Fatalf("want schema_drift in structural findings: %+v", str.Findings)
	}
	fresh := findRule(res, "orders-fresh")
	if fresh.Status != model.StatusError || fresh.Findings[0].Reason != model.ReasonFieldMissing {
		t.Fatalf("freshness on vanished column must be error/field_missing, got %+v", fresh)
	}
}

func TestMissingSourceIsNormalizedNotSilent(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	if err := os.Remove(fx.decls[1].Path); err != nil {
		t.Fatal(err)
	}
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())
	res, _, err := eng.Run(context.Background(), fx.req("b1", 1))
	if err != nil {
		t.Fatalf("missing source must be a result, not an abort error: %v", err)
	}
	str := findRule(res, "users.structural")
	if str.Status != model.StatusError || str.Findings[0].Reason != model.ReasonSourceUnreadable {
		t.Fatalf("want source_unreadable structural result, got %+v", str)
	}
	for _, id := range []string{"users-id-unique", "users-country-enum"} {
		r := findRule(res, id)
		if r.Status != model.StatusError || r.Findings[0].Reason != model.ReasonSourceUnreadable {
			t.Fatalf("%s must be error/source_unreadable, got %+v", id, r)
		}
	}
	// 其他数据集的规则不受影响。
	if findRule(res, "orders-id-unique").Status != model.StatusPass {
		t.Fatal("orders rules must be unaffected by users load failure")
	}
}

func TestOutOfOrderAcceptedAndOccupiedOrdinalRejected(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())
	ctx := context.Background()

	// 乱序到达：先提交 ordinal=3。
	if _, _, err := eng.Run(ctx, fx.req("b3", 3)); err != nil {
		t.Fatal(err)
	}
	// ordinal=2 迟到：序号未被占用，必须接受且可复现（不漏报）。
	if _, _, err := eng.Run(ctx, fx.req("b2", 2)); err != nil {
		t.Fatalf("late unoccupied ordinal must be accepted: %v", err)
	}
	if _, ok := store.Get("b2"); !ok {
		t.Fatal("late batch b2 must be persisted and reproducible")
	}
	if store.LatestOrdinal() != 3 {
		t.Fatalf("latest = %d, want 3", store.LatestOrdinal())
	}
	// 另一个批次试图占用已提交序号 2：状态回退，明确拒绝，状态不变。
	if _, _, err := eng.Run(ctx, fx.req("b2-evil", 2)); errs.CodeOf(err) != model.ReasonStateRollback {
		t.Fatalf("want state_rollback, got %v", err)
	}
	// ordinal=4 正常前进。
	if _, _, err := eng.Run(ctx, fx.req("b4", 4)); err != nil {
		t.Fatalf("ordinal 4 should advance: %v", err)
	}
}

func TestSameBatchDifferentInputConflict(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())
	ctx := context.Background()
	if _, _, err := eng.Run(ctx, fx.req("b1", 1)); err != nil {
		t.Fatal(err)
	}
	tampered := fx.req("b1", 1)
	tampered.Watermark = fx.wm.Add(time.Second)
	if _, _, err := eng.Run(ctx, tampered); errs.CodeOf(err) != model.ReasonStateConflict {
		t.Fatalf("want state_conflict on watermark change, got %v", err)
	}
	tamperedRules := fx.req("b1", 1)
	tamperedRules.Rules[0].MinNonNull = 0.5
	if _, _, err := eng.Run(ctx, tamperedRules); errs.CodeOf(err) != model.ReasonStateConflict {
		t.Fatalf("want state_conflict on rule change, got %v", err)
	}
}

func TestTimeoutRuleDoesNotAffectOthersAndOrderStable(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())
	eng.cfg.PerRuleTimeout = 20 * time.Millisecond
	req := fx.req("b1", 1)
	res, _, err := eng.RunWithHook(context.Background(), req, "orders-id-unique", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// 顺序：结构结果在前，用户规则按声明顺序。
	if res.Results[2].RuleID != "orders-id-complete" {
		t.Fatalf("unexpected order: %s", res.Results[2].RuleID)
	}
	timedOut := findRule(res, "orders-id-unique")
	if timedOut.Status != model.StatusError || timedOut.Findings[0].Reason != model.ReasonRuleTimeout {
		t.Fatalf("want rule_timeout, got %+v", timedOut)
	}
	if findRule(res, "users-id-unique").Status != model.StatusPass {
		t.Fatal("other rules must still pass")
	}
}

func TestConcurrentSameBatchRunsOnce(t *testing.T) {
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	sink := monitor.NewMemorySink()
	eng := newEngine(t, store, sink)

	const n = 8
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	replayedCh := make(chan bool, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, replayed, err := eng.Run(context.Background(), fx.req("b1", 1))
			errsCh <- err
			replayedCh <- replayed
		}()
	}
	wg.Wait()
	close(errsCh)
	close(replayedCh)
	leaders := 0
	for err := range errsCh {
		if err != nil {
			t.Fatalf("concurrent run error: %v", err)
		}
	}
	for r := range replayedCh {
		if !r {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader evaluation, got %d", leaders)
	}
	if store.LatestOrdinal() != 1 {
		t.Fatalf("state must contain exactly one batch, latest=%d", store.LatestOrdinal())
	}
	// 绿色批次无告警；下面失败批次再验证并发下告警不重复。
	if got := len(sink.Snapshot()); got != 0 {
		t.Fatalf("green batch alerts = %d, want 0", got)
	}
}

func TestConcurrentFailingBatchAlertsOnce(t *testing.T) {
	badOrders := `[
		{"id": 1, "user_id": 10, "status": "weird", "updated_at": "2026-09-20T10:00:00Z"}
	]`
	fx := newFixture(t, badOrders, goodUsers())
	store := state.NewMemoryStore()
	sink := monitor.NewMemorySink()
	eng := newEngine(t, store, sink)

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _, _ = eng.Run(context.Background(), fx.req("b1", 1))
		}()
	}
	wg.Wait()

	alerts := sink.Snapshot()
	rec, ok := store.Get("b1")
	if !ok {
		t.Fatal("batch must be committed")
	}
	want := 0
	for _, r := range rec.Results {
		if r.Status != model.StatusPass {
			want += len(r.Findings)
		}
	}
	if want == 0 {
		t.Fatal("fixture should produce at least one alert")
	}
	if len(alerts) != want {
		t.Fatalf("alerts = %d, want exactly %d (no duplicates)", len(alerts), want)
	}
}

func TestManyConcurrentDistinctBatchesAllCommit(t *testing.T) {
	// 多个不同批次并发（乱序序号）：互不影响，全部稳定提交。
	fx := newFixture(t, goodOrders(), goodUsers())
	store := state.NewMemoryStore()
	eng := newEngine(t, store, monitor.NewMemorySink())

	const n = 10
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	wg.Add(n)
	for i := 1; i <= n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, _, err := eng.Run(context.Background(), fx.reqFor("batch", uint64(i+10)))
			errsCh <- err
		}()
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		if err != nil {
			t.Fatalf("distinct batch error: %v", err)
		}
	}
	if store.LatestOrdinal() != uint64(n+10) {
		t.Fatalf("latest ordinal = %d, want %d", store.LatestOrdinal(), uint64(n+10))
	}
}
