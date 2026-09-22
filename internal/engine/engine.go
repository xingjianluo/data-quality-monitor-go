// Package engine 编排一个批次的完整校验流程：
// 加载输入 -> 内置结构校验 -> 并发规则判定 -> 原子提交状态 -> 告警。
// 单规则隔离、并发受控、结论顺序稳定、异常归一化。
package engine

import (
	"context"
	"sort"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/logx"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/monitor"
	"github.com/xingjianluo/data-quality-monitor-go/internal/rule"
	"github.com/xingjianluo/data-quality-monitor-go/internal/source"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// Config 是引擎运行参数。
type Config struct {
	Concurrency    int
	PerRuleTimeout time.Duration
}

// Engine 是校验编排器。
type Engine struct {
	store  state.Store
	mon    *monitor.Monitor
	sink   monitor.AlertSink
	logger logx.Logger
	cfg    Config
	group  *flightGroup
}

// New 创建引擎。sink 为 nil 时不投递告警。
func New(store state.Store, mon *monitor.Monitor, sink monitor.AlertSink, logger logx.Logger, cfg Config) *Engine {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	return &Engine{store: store, mon: mon, sink: sink, logger: logger, cfg: cfg, group: newFlightGroup()}
}

// RunRequest 描述一次批次运行。
type RunRequest struct {
	BatchID   string
	Ordinal   uint64
	Watermark time.Time // 批次评估水位线；零值时取当前时间（仅首次运行有效，重放沿用已存值）
	Datasets  []model.DatasetDecl
	Rules     []model.RuleSpec
}

// Run 执行一次批次：
//   - 同批次重跑：返回已提交的相同结论（replayed=true），不重复告警；
//   - 序号回退：以 state_rollback 明确拒绝；
//   - 输入未齐：以 input_incomplete 拒绝，且不写入状态；
//   - 规则失败/超时/取消：归一化为结果中的 error，不影响其他规则。
func (e *Engine) Run(ctx context.Context, req RunRequest) (model.BatchResult, bool, error) {
	return e.runWith(ctx, req, "", 0)
}

// RunWithHook 仅供测试：对指定规则注入每条记录的延迟，确定性触发超时场景。
func (e *Engine) RunWithHook(ctx context.Context, req RunRequest, slowRuleID string, delay time.Duration) (model.BatchResult, bool, error) {
	return e.runWith(ctx, req, slowRuleID, delay)
}

func (e *Engine) runWith(ctx context.Context, req RunRequest, slowRuleID string, hookDelay time.Duration) (model.BatchResult, bool, error) {
	// 快速路径：已提交批次直接重放（含崩溃恢复补投），不参与串行化排队。
	if existing, ok := e.store.Get(req.BatchID); ok {
		return e.replayExisting(ctx, existing, req)
	}
	out, leader, err := e.group.Do(req.BatchID, func() (interface{}, error) {
		res, _, gerr := e.execute(ctx, req, slowRuleID, hookDelay)
		if gerr != nil {
			return model.BatchResult{}, gerr
		}
		return res, nil
	})
	if err != nil {
		return model.BatchResult{}, false, err
	}
	if leader {
		return out.(model.BatchResult), false, nil
	}
	// 追随者共享领导者的成果：领导者成功则从状态复现，不重复评估/告警。
	existing, ok := e.store.Get(req.BatchID)
	if !ok {
		return model.BatchResult{}, false, errs.New(model.ReasonInternal,
			"concurrent batch %q finished without committed state", req.BatchID)
	}
	res, _, rerr := e.replayExisting(ctx, existing, req)
	return res, true, rerr
}

// replayExisting 复现已提交批次：冲突校验 + 必要时崩溃恢复补投。
func (e *Engine) replayExisting(ctx context.Context, existing state.BatchRecord, req RunRequest) (model.BatchResult, bool, error) {
	if err := checkReplayConflict(existing, req); err != nil {
		return model.BatchResult{}, false, err
	}
	res := batchResultFromRecord(existing)
	if e.mon != nil && e.sink != nil && !existing.AlertsSent {
		alerts := e.mon.Alerts(existing)
		if len(alerts) > 0 {
			if err := e.sink.Send(ctx, alerts); err != nil {
				return res, true, errs.New(model.ReasonInternal, "alert delivery failed: %v", err)
			}
		}
		if err := e.store.MarkAlertsSent(ctx, req.BatchID); err != nil {
			return res, true, err
		}
	}
	e.logger.InfoContext(ctx, "batch replayed from committed state",
		"batch_id", req.BatchID, "ordinal", req.Ordinal, "overall_status", res.Status())
	return res, true, nil
}

// execute 执行一个尚未提交批次的完整流程。
func (e *Engine) execute(ctx context.Context, req RunRequest, slowRuleID string, hookDelay time.Duration) (model.BatchResult, bool, error) {
	if err := validateRequest(req); err != nil {
		return model.BatchResult{}, false, err
	}
	watermark := req.Watermark
	if watermark.IsZero() {
		watermark = time.Now().UTC()
	}

	e.logger.InfoContext(ctx, "batch run started",
		"batch_id", req.BatchID, "ordinal", req.Ordinal,
		"datasets", len(req.Datasets), "rules", len(req.Rules),
		"watermark", watermark.Format(time.RFC3339Nano))

	// 1) 并发加载输入。加载失败不中止批次：归一化为该数据集的结构校验结果，
	//    与其他数据集的结论一起提交，保证可复现、不静默、可区分。
	loaded := source.NewLoader(e.cfg.Concurrency).Load(ctx, req.Datasets)
	failByName := make(map[string]source.LoadFailure, len(loaded.Failures))
	for _, f := range loaded.Failures {
		failByName[f.Dataset] = f
	}

	// 2) 组装输入摘要与结构基线。
	// 声明指纹用于兼容性比对；实际指纹取数据中真实出现的键集合，用于形态变化检测。
	inputs := make([]model.InputSummary, 0, len(req.Datasets))
	schemas := make(map[string]string, len(req.Datasets))       // 声明指纹
	actualSchemas := make(map[string]string, len(req.Datasets)) // 实际指纹
	dsByName := make(map[string]*model.Dataset, len(req.Datasets))
	for _, decl := range req.Datasets {
		fp := decl.Schema().Fingerprint()
		schemas[decl.Name] = fp
		if f, bad := failByName[decl.Name]; bad {
			actualSchemas[decl.Name] = ""
			inputs = append(inputs, model.InputSummary{Dataset: decl.Name, Rows: 0, Schema: fp})
			e.logger.WarnContext(ctx, "input load failed",
				"batch_id", req.BatchID, "dataset", decl.Name, "reason", f.Reason, "message", f.Message)
			continue
		}
		ds := loaded.Datasets[decl.Name]
		dsByName[decl.Name] = ds
		actualSchemas[decl.Name] = observedSchema(ds).Fingerprint()
		inputs = append(inputs, model.InputSummary{
			Dataset: decl.Name,
			Rows:    len(ds.Records),
			Schema:  fp,
		})
	}
	sort.SliceStable(inputs, func(i, j int) bool { return inputs[i].Dataset < inputs[j].Dataset })
	for _, in := range inputs {
		e.logger.InfoContext(ctx, "input summary",
			"batch_id", req.BatchID, "dataset", in.Dataset, "rows", in.Rows, "schema_fingerprint", in.Schema)
	}

	// 3) 内置结构校验：每个数据集一条，形态基线取最新已提交批次中实际指纹。
	baselines := e.latestObservedSchemas()
	structResults := make([]model.RuleResult, 0, len(req.Datasets))
	for _, decl := range req.Datasets {
		if f, bad := failByName[decl.Name]; bad {
			stub := &model.Dataset{Name: decl.Name, Schema: decl.Schema()}
			structResults = append(structResults, rule.Structural(stub, f.Reason, f.Message, ""))
			continue
		}
		structResults = append(structResults, rule.Structural(
			loaded.Datasets[decl.Name], model.ReasonCode(""), "", baselines[decl.Name]))
	}

	// 4) 并发规则判定（单规则隔离，结论按声明顺序）。
	ec := rule.EvalContext{
		BatchID:   req.BatchID,
		Watermark: watermark,
		Datasets:  dsByName,
	}
	if hookDelay > 0 {
		ec.TestHookPerRecordDelay = hookDelay
		ec.TestHookSlowRuleID = slowRuleID
	}
	userResults := rule.EvaluateAll(ctx, req.Rules, ec, e.cfg.Concurrency, e.cfg.PerRuleTimeout)

	results := make([]model.RuleResult, 0, len(structResults)+len(userResults))
	results = append(results, structResults...)
	results = append(results, userResults...)

	// 5) 打印判定依据。
	for _, r := range results {
		for _, f := range r.Findings {
			e.logger.InfoContext(ctx, "finding",
				"batch_id", req.BatchID, "rule_id", r.RuleID, "dataset", r.Dataset,
				"status", r.Status, "reason", f.Reason, "column", f.Column,
				"count", f.Count, "message", f.Message)
		}
		if len(r.Findings) == 0 {
			e.logger.InfoContext(ctx, "finding",
				"batch_id", req.BatchID, "rule_id", r.RuleID, "dataset", r.Dataset,
				"status", r.Status, "checked", r.Checked, "reason", model.ReasonNone)
		}
	}

	batch := model.BatchResult{
		BatchID:   req.BatchID,
		Ordinal:   req.Ordinal,
		Evaluated: watermark,
		Inputs:    inputs,
		Results:   results,
	}

	// 6) 原子提交状态（回退/冲突在此被明确拒绝）。
	rec := state.BatchRecord{
		BatchID:         req.BatchID,
		Ordinal:         req.Ordinal,
		WatermarkMs:     watermark.UnixMilli(),
		CommittedMs:     time.Now().UTC().UnixMilli(),
		Inputs:          inputs,
		Rules:           cloneRules(req.Rules),
		Results:         cloneResults(results),
		Schemas:         schemas,
		ObservedSchemas: actualSchemas,
	}
	if err := e.store.Commit(ctx, rec); err != nil {
		return model.BatchResult{}, false, err
	}
	e.logger.InfoContext(ctx, "batch committed",
		"batch_id", req.BatchID, "ordinal", req.Ordinal, "overall_status", batch.Status())

	// 7) 首次提交才投递告警；投递确认与状态标记分离，崩溃后重跑至多补投一次。
	if e.mon != nil && e.sink != nil {
		alerts := e.mon.Alerts(rec)
		if len(alerts) > 0 {
			if err := e.sink.Send(ctx, alerts); err != nil {
				// 未确认投递：不标记，返回归一化错误，允许重跑补投。
				return batch, false, errs.New(model.ReasonInternal, "alert delivery failed: %v", err)
			}
		}
		if err := e.store.MarkAlertsSent(ctx, req.BatchID); err != nil {
			return batch, false, err
		}
	}
	return batch, false, nil
}

// validateRequest 做批次级前置校验，不合法则不写入任何状态。
func validateRequest(req RunRequest) error {
	if req.BatchID == "" {
		return errs.New(model.ReasonInputIncomplete, "batch id is required")
	}
	if len(req.Datasets) == 0 {
		return errs.New(model.ReasonInputIncomplete, "batch %q declares no datasets", req.BatchID)
	}
	seen := make(map[string]bool, len(req.Datasets))
	for _, d := range req.Datasets {
		if d.Name == "" {
			return errs.New(model.ReasonInputIncomplete, "batch %q has an unnamed dataset", req.BatchID)
		}
		if seen[d.Name] {
			return errs.New(model.ReasonInputIncomplete, "batch %q declares dataset %q more than once", req.BatchID, d.Name)
		}
		seen[d.Name] = true
		if len(d.Columns) == 0 {
			return errs.New(model.ReasonInputIncomplete, "dataset %q declares no columns", d.Name)
		}
	}
	ruleIDs := make(map[string]bool, len(req.Rules))
	for _, r := range req.Rules {
		if msg := r.Validate(); msg != "" {
			return errs.New(model.ReasonInvalidRule, "%s", msg)
		}
		if ruleIDs[r.ID] {
			return errs.New(model.ReasonInvalidRule, "duplicate rule id %q", r.ID)
		}
		ruleIDs[r.ID] = true
		if !seen[r.Dataset] {
			return errs.New(model.ReasonInvalidRule, "rule %q references undeclared dataset %q", r.ID, r.Dataset)
		}
	}
	return nil
}

// latestObservedSchemas 返回最近已提交批次记录的各数据集实际结构指纹。
func (e *Engine) latestObservedSchemas() map[string]string {
	ordinal := e.store.LatestOrdinal()
	if ordinal == 0 {
		return nil
	}
	rec, ok := e.store.GetByOrdinal(ordinal)
	if !ok {
		return nil
	}
	if len(rec.ObservedSchemas) == 0 {
		return nil
	}
	return rec.ObservedSchemas
}

// batchResultFromRecord 从持久记录还原完全一致的批次结论（复现/重放）。
func batchResultFromRecord(rec state.BatchRecord) model.BatchResult {
	inputs := append([]model.InputSummary(nil), rec.Inputs...)
	results := append([]model.RuleResult(nil), rec.Results...)
	return model.BatchResult{
		BatchID:   rec.BatchID,
		Ordinal:   rec.Ordinal,
		Evaluated: time.UnixMilli(rec.WatermarkMs).UTC(),
		Inputs:    inputs,
		Results:   results,
	}
}

// checkReplayConflict 校验重放请求与已提交记录的业务输入是否完全一致。
func checkReplayConflict(existing state.BatchRecord, req RunRequest) error {
	if existing.Ordinal != req.Ordinal {
		return errs.New(model.ReasonStateConflict,
			"batch %q replay ordinal %d differs from committed ordinal %d",
			req.BatchID, req.Ordinal, existing.Ordinal)
	}
	if !req.Watermark.IsZero() && req.Watermark.UnixMilli() != existing.WatermarkMs {
		return errs.New(model.ReasonStateConflict,
			"batch %q replay watermark %s differs from committed watermark %s",
			req.BatchID, req.Watermark.UTC().Format(time.RFC3339Nano),
			time.UnixMilli(existing.WatermarkMs).UTC().Format(time.RFC3339Nano))
	}
	wantSchemas := make(map[string]string, len(req.Datasets))
	for _, d := range req.Datasets {
		wantSchemas[d.Name] = d.Schema().Fingerprint()
	}
	if len(wantSchemas) != len(existing.Schemas) {
		return errs.New(model.ReasonStateConflict,
			"batch %q replay dataset set differs from committed input", req.BatchID)
	}
	for name, fp := range wantSchemas {
		if existing.Schemas[name] != fp {
			return errs.New(model.ReasonStateConflict,
				"batch %q replay schema for dataset %q differs from committed input", req.BatchID, name)
		}
	}
	committed := existing.Rules
	if len(committed) != len(req.Rules) {
		return errs.New(model.ReasonStateConflict,
			"batch %q replay rule set differs from committed input", req.BatchID)
	}
	for i, spec := range req.Rules {
		if committed[i].SpecKey() != spec.SpecKey() {
			return errs.New(model.ReasonStateConflict,
				"batch %q replay rule at position %d (%q) differs from committed declaration",
				req.BatchID, i, spec.ID)
		}
	}
	return nil
}

// observedSchema 汇总数据中实际出现的键集合（值类型视为未声明，用 any 标记）。
func observedSchema(ds *model.Dataset) model.Schema {
	seen := make(map[string]bool)
	for _, rec := range ds.Records {
		for k := range rec {
			seen[k] = true
		}
	}
	names := make([]string, 0, len(seen))
	for k := range seen {
		names = append(names, k)
	}
	sort.Strings(names)
	cols := make([]model.Column, 0, len(names))
	for _, n := range names {
		// 已知声明列沿用声明类型，其余列标记 any，仅用于形态变化检测。
		if c, ok := ds.Schema.ColumnByName(n); ok {
			cols = append(cols, c)
		} else {
			cols = append(cols, model.Column{Name: n, Type: model.DataType("any")})
		}
	}
	return model.Schema{Columns: cols}
}

func cloneRules(in []model.RuleSpec) []model.RuleSpec {
	out := make([]model.RuleSpec, len(in))
	for i, r := range in {
		r.Columns = append([]string(nil), r.Columns...)
		r.Allowed = append([]string(nil), r.Allowed...)
		out[i] = r
	}
	return out
}

func cloneResults(in []model.RuleResult) []model.RuleResult {
	out := make([]model.RuleResult, len(in))
	for i, r := range in {
		r.Findings = append([]model.Finding(nil), r.Findings...)
		out[i] = r
	}
	return out
}
