// Package engine orchestrates concurrent, resource-bounded, deterministic
// batch evaluation over multiple local datasets.
//
// Guarantees:
//
//   - datasets are loaded once per batch and shared by their rules;
//   - each rule runs under its own timeout and context: one rule's failure,
//     timeout, cancellation or panic never affects another rule;
//   - a semaphore bounds concurrent rule evaluation;
//   - results are always returned in stable rule-id order;
//   - all infrastructure errors are normalized into RuleResult.ErrorCode /
//     Finding codes; raw underlying errors never leak to external results;
//   - a content fingerprint makes replays detectable and conclusions
//     reproducible for the same inputs and evaluation time.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/rule"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// Defaults for resource control.
const (
	DefaultMaxConcurrentRules = 4
	DefaultRuleTimeout        = 10 * time.Second
)

// Options controls engine resource use and deterministic evaluation time.
type Options struct {
	MaxConcurrentRules int
	RuleTimeout        time.Duration
	EvalTime           time.Time
	Logger             *slog.Logger
}

// Engine runs data quality batches.
type Engine struct {
	cfg     *config.Config
	loader  *datasource.Loader
	store   state.Store
	alerter *alert.Alerter
	opts    Options
}

// New constructs an Engine.
func New(cfg *config.Config, loader *datasource.Loader, store state.Store, alerter *alert.Alerter, opts Options) *Engine {
	if opts.MaxConcurrentRules <= 0 {
		opts.MaxConcurrentRules = DefaultMaxConcurrentRules
	}
	if opts.RuleTimeout <= 0 {
		opts.RuleTimeout = DefaultRuleTimeout
	}
	if opts.EvalTime.IsZero() {
		opts.EvalTime = time.Now().UTC()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{cfg: cfg, loader: loader, store: store, alerter: alerter, opts: opts}
}

type loadedDataset struct {
	ds     *model.Dataset
	issues []datasource.ShapeIssue
	err    error
}

// Run evaluates one batch. force=true reclaims a stale running batch; replay of
// a committed batch returns its historical conclusions deterministically.
func (e *Engine) Run(ctx context.Context, batchID string, force bool) (*model.BatchResult, error) {
	log := e.opts.Logger.With("batch_id", batchID)

	st, replay, err := e.store.Begin(ctx, batchID, force)
	if err != nil {
		return nil, err
	}
	if replay {
		log.Info("replaying committed batch", "fingerprint", st.Fingerprint, "rules", len(st.Results))
		return &model.BatchResult{
			BatchID:     batchID,
			StartedAt:   st.StartedAt,
			FinishedAt:  derefTime(st.CommittedAt),
			Status:      "replay",
			Results:     orderedResults(st.Results),
			Fingerprint: st.Fingerprint,
		}, nil
	}
	lease := st.LeaseToken
	startedAt := st.StartedAt

	// Load every dataset once, concurrently but bounded. Shape problems are
	// normalized failures for all rules on that dataset, not silent passes.
	loaded := e.loadDatasets(ctx, log)

	results := e.evaluateRules(ctx, log, loaded)
	results = orderedResults(results)

	fp := fingerprint(batchID, e.opts.EvalTime, results)
	if err := e.store.Commit(ctx, batchID, lease, fp, results); err != nil {
		log.Error("commit failed", "error", normalizedMessage(err))
		_ = e.store.Fail(context.Background(), batchID, lease, "commit_failed")
		return nil, err
	}

	status := "committed"
	alertCount := 0
	if e.alerter != nil {
		n, alertErr := e.alerter.Dispatch(context.Background(), batchID, results, model.SeverityWarning)
		if alertErr != nil {
			// Alerting failure must not change verdicts; surface it in logs only.
			log.Error("alert dispatch partially failed", "error", normalizedMessage(alertErr))
		}
		alertCount = n
	}

	failed, errored := summarizeCounts(results)
	log.Info("batch committed",
		"fingerprint", fp,
		"rules", len(results),
		"passed", len(results)-failed-errored,
		"failed", failed,
		"errored", errored,
		"alerts", alertCount,
	)
	return &model.BatchResult{
		BatchID:     batchID,
		StartedAt:   startedAt,
		FinishedAt:  time.Now().UTC(),
		Status:      status,
		Results:     results,
		Fingerprint: fp,
	}, nil
}

func (e *Engine) loadDatasets(ctx context.Context, log *slog.Logger) map[string]loadedDataset {
	var mu sync.Mutex
	out := map[string]loadedDataset{}
	sem := make(chan struct{}, e.opts.MaxConcurrentRules)
	var wg sync.WaitGroup
	for i := range e.cfg.Datasets {
		decl := e.cfg.Datasets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				out[decl.Name] = loadedDataset{err: ctx.Err()}
				mu.Unlock()
				return
			}
			defer func() { <-sem }()
			t0 := time.Now()
			ds, issues, err := e.loader.Load(ctx, decl)
			log.Info("input summary",
				"dataset", decl.Name,
				"format", decl.Format,
				"declared_fields", len(decl.Schema.Fields),
				"records", recordCount(ds),
				"shape_issues", len(issues),
				"load_ms", time.Since(t0).Milliseconds(),
			)
			mu.Lock()
			out[decl.Name] = loadedDataset{ds: ds, issues: issues, err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (e *Engine) evaluateRules(ctx context.Context, log *slog.Logger, loaded map[string]loadedDataset) []model.RuleResult {
	sem := make(chan struct{}, e.opts.MaxConcurrentRules)
	var wg sync.WaitGroup
	results := make([]model.RuleResult, len(e.cfg.Rules))

	for i := range e.cfg.Rules {
		r := e.cfg.Rules[i]
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] = normalizedErrorResult(r, model.ReasonRuleCancelled, "batch cancelled before rule started")
				return
			}
			defer func() { <-sem }()
			results[idx] = e.evaluateOne(ctx, log, r, loaded)
		}(i)
	}
	wg.Wait()
	return results
}

func (e *Engine) evaluateOne(parent context.Context, log *slog.Logger, r model.Rule, loaded map[string]loadedDataset) (res model.RuleResult) {
	t0 := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			res = normalizedErrorResult(r, model.ReasonRulePanic,
				fmt.Sprintf("rule panicked: %v", rec))
			res.DurationMS = time.Since(t0).Milliseconds()
		}
	}()

	checker, err := rule.NewChecker(r.Kind)
	if err != nil {
		rr := normalizedErrorResult(r, model.ReasonRuleInvalid, normalizedMessage(err))
		rr.DurationMS = time.Since(t0).Milliseconds()
		return rr
	}

	entry := loaded[r.Dataset]
	if entry.err != nil {
		rr := normalizedErrorResult(r, model.ReasonSourceLoadFailure, normalizedMessage(entry.err))
		rr.DurationMS = time.Since(t0).Milliseconds()
		return rr
	}
	if len(entry.issues) > 0 {
		// Structural source change: never silently run or silently pass. The
		// primary error code is the most severe class (a removed declared
		// column breaks evaluation) while every distinct shape class is also
		// attached as a finding so failure reasons stay distinguishable.
		code := entry.issues[0].Code
		findings := make([]model.Finding, 0, len(entry.issues))
		seen := map[model.ReasonCode]bool{}
		for _, is := range entry.issues {
			if is.Code == model.ReasonSchemaFieldMissing {
				code = model.ReasonSchemaFieldMissing
			}
			if seen[is.Code] {
				continue
			}
			seen[is.Code] = true
			findings = append(findings, model.Finding{
				Code:     is.Code,
				Evidence: []model.Evidence{{Detail: is.Detail}},
			})
		}
		sort.Slice(findings, func(i, j int) bool { return findings[i].Code < findings[j].Code })
		rr := normalizedErrorResult(r, code, shapeMessage(entry.issues))
		rr.Findings = findings
		rr.DurationMS = time.Since(t0).Milliseconds()
		return rr
	}

	ruleCtx, cancel := context.WithTimeout(parent, e.opts.RuleTimeout)
	defer cancel()

	done := make(chan model.RuleResult, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- normalizedErrorResult(r, model.ReasonRulePanic, fmt.Sprintf("rule panicked: %v", rec))
			}
		}()
		done <- checker.Check(ruleCtx, entry.ds, r, e.opts.EvalTime)
	}()

	select {
	case rr := <-done:
		if rr.RuleID == "" {
			rr.RuleID = r.ID
		}
		rr.DurationMS = time.Since(t0).Milliseconds()
		logRuleBasis(log, r, rr)
		return rr
	case <-ruleCtx.Done():
		code, msg := model.ReasonRuleTimeout, "rule timed out"
		if parent.Err() != nil {
			code, msg = model.ReasonRuleCancelled, "batch cancelled"
		}
		rr := normalizedErrorResult(r, code, msg)
		rr.DurationMS = time.Since(t0).Milliseconds()
		return rr
	}
}

func normalizedErrorResult(r model.Rule, code model.ReasonCode, msg string) model.RuleResult {
	return model.RuleResult{
		RuleID:    r.ID,
		Dataset:   r.Dataset,
		Kind:      r.Kind,
		Severity:  severityOrDefault(r.Severity),
		Status:    model.StatusErrored,
		ErrorCode: code,
		ErrorMsg:  msg,
		Findings:  []model.Finding{},
	}
}

func severityOrDefault(s model.Severity) model.Severity {
	if s == "" {
		return model.SeverityError
	}
	return s
}

// normalizedMessage strips transport-level noise; callers only see a stable,
// explainable message rather than the raw underlying error text.
func normalizedMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func shapeMessage(issues []datasource.ShapeIssue) string {
	msg := "source shape does not match declared schema:"
	parts := make([]string, 0, len(issues))
	for _, is := range issues {
		parts = append(parts, string(is.Code)+": "+is.Detail)
	}
	sort.Strings(parts)
	return msg + " " + joinStrings(parts, "; ")
}

func joinStrings(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

func logRuleBasis(log *slog.Logger, r model.Rule, rr model.RuleResult) {
	ev := 0
	codes := make([]string, 0, len(rr.Findings))
	for _, f := range rr.Findings {
		ev += len(f.Evidence)
		codes = append(codes, string(f.Code))
	}
	log.Info("rule verdict",
		"rule_id", r.ID,
		"dataset", r.Dataset,
		"kind", r.Kind,
		"status", rr.Status,
		"finding_codes", codes,
		"evidence", ev,
		"duration_ms", rr.DurationMS,
	)
}

func orderedResults(in []model.RuleResult) []model.RuleResult {
	out := append([]model.RuleResult(nil), in...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].RuleID < out[j].RuleID })
	for i := range out {
		if out[i].Findings == nil {
			out[i].Findings = []model.Finding{}
		}
	}
	return out
}

func summarizeCounts(results []model.RuleResult) (failed, errored int) {
	for _, r := range results {
		switch r.Status {
		case model.StatusFailed:
			failed++
		case model.StatusErrored:
			errored++
		}
	}
	return failed, errored
}

func recordCount(ds *model.Dataset) int {
	if ds == nil {
		return 0
	}
	return len(ds.Records)
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// fingerprint hashes the deterministic inputs and verdicts of a batch so the
// same batch id + evaluation time + conclusions always produce the same value.
func fingerprint(batchID string, evalTime time.Time, results []model.RuleResult) string {
	type fpResult struct {
		RuleID    string           `json:"rule_id"`
		Status    model.RuleStatus `json:"status"`
		ErrorCode model.ReasonCode `json:"error_code"`
		ErrorMsg  string           `json:"error_message"`
		Findings  []model.Finding  `json:"findings"`
	}
	cp := make([]fpResult, 0, len(results))
	for _, r := range results {
		cp = append(cp, fpResult{
			RuleID: r.RuleID, Status: r.Status, ErrorCode: r.ErrorCode,
			ErrorMsg: r.ErrorMsg, Findings: r.Findings,
		})
	}
	payload := struct {
		BatchID  string     `json:"batch_id"`
		EvalTime time.Time  `json:"eval_time"`
		Results  []fpResult `json:"results"`
	}{BatchID: batchID, EvalTime: evalTime.UTC(), Results: cp}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
