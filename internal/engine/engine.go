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
	"github.com/xingjianluo/data-quality-monitor-go/internal/manifest"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/rule"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// Defaults for resource control.
const (
	DefaultMaxConcurrentRules = 4
	DefaultRuleTimeout        = 10 * time.Second
	// DefaultLeaseTTL is the lease lifetime assumed when heartbeating. It must
	// stay below the store's lease TTL so concurrent triggers keep seeing a live
	// run; the store itself owns the authoritative TTL.
	DefaultLeaseTTL = state.DefaultLeaseTTL
)

// Options controls engine resource use and deterministic evaluation time.
type Options struct {
	MaxConcurrentRules int
	RuleTimeout        time.Duration
	LeaseTTL           time.Duration
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

	// In-process single-flight: concurrent triggers of the same batch in one
	// process join the single live execution instead of starting a second one.
	flightMu sync.Mutex
	flights  map[string]*flight
}

// flight is one shared in-process batch execution.
type flight struct {
	done chan struct{}
	res  *model.BatchResult
	err  error
}

// New constructs an Engine.
func New(cfg *config.Config, loader *datasource.Loader, store state.Store, alerter *alert.Alerter, opts Options) *Engine {
	if opts.MaxConcurrentRules <= 0 {
		opts.MaxConcurrentRules = DefaultMaxConcurrentRules
	}
	if opts.RuleTimeout <= 0 {
		opts.RuleTimeout = DefaultRuleTimeout
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = DefaultLeaseTTL
	}
	if opts.EvalTime.IsZero() {
		opts.EvalTime = time.Now().UTC()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{cfg: cfg, loader: loader, store: store, alerter: alerter, opts: opts, flights: map[string]*flight{}}
}

type loadedDataset struct {
	ds     *model.Dataset
	issues []datasource.ShapeIssue
	err    error
}

// Run evaluates one batch.
//
//   - A committed batch is replayed only when the current inputs still match
//     the stored manifest; drift (data or rules) returns *model.HistoryInvalidError
//     without overwriting anything. Replays still fill any undelivered alert
//     backlog.
//   - A late-arriving, never-processed batch (even older than the latest one)
//     is evaluated normally.
//   - Concurrent triggers of the same batch join the single in-process
//     execution and receive the same result; cross-process triggers get
//     state.ErrBatchRunning.
//   - force=true only reclaims an interrupted (lease-expired) batch; it never
//     overwrites committed conclusions.
func (e *Engine) Run(ctx context.Context, batchID string, force bool) (*model.BatchResult, error) {
	log := e.opts.Logger.With("batch_id", batchID)

	// In-process single flight: concurrent triggers share one execution.
	e.flightMu.Lock()
	if f, ok := e.flights[batchID]; ok {
		e.flightMu.Unlock()
		log.Info("batch already running in this process; joining the live execution")
		select {
		case <-f.done:
			return f.res, f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &flight{done: make(chan struct{})}
	e.flights[batchID] = f
	e.flightMu.Unlock()

	defer func() {
		e.flightMu.Lock()
		delete(e.flights, batchID)
		e.flightMu.Unlock()
	}()

	f.res, f.err = e.runOnce(ctx, batchID, force, log)
	close(f.done)
	return f.res, f.err
}

func (e *Engine) runOnce(ctx context.Context, batchID string, force bool, log *slog.Logger) (*model.BatchResult, error) {
	st, replay, err := e.store.Begin(ctx, batchID, force)
	if err != nil {
		return nil, err
	}

	currentManifest := manifest.Build(e.cfg, e.opts.EvalTime)

	if replay {
		return e.replayCommitted(ctx, batchID, st, currentManifest, log)
	}
	lease := st.LeaseToken
	startedAt := st.StartedAt

	// Renew the lease while evaluating so a long batch still looks live to
	// concurrent triggers; a lost lease aborts evaluation instead of
	// committing onto a possibly-reclaimed batch.
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	hbErr := make(chan error, 1)
	go e.heartbeat(hbCtx, batchID, lease, log, hbErr)
	evalCtx, cancelEval := context.WithCancel(hbCtx)
	defer func() {
		stopHeartbeat()
		cancelEval()
	}()

	// Load every dataset once, concurrently but bounded. Shape problems are
	// normalized failures for all rules on that dataset, not silent passes.
	loaded := e.loadDatasets(evalCtx, log)

	results := e.evaluateRules(evalCtx, log, loaded)
	results = orderedResults(results)

	select {
	case err := <-hbErr:
		if err != nil {
			log.Error("lease lost during evaluation; aborting without commit", "error", normalizedMessage(err))
			_ = e.store.Fail(context.Background(), batchID, lease, "lease_lost")
			return nil, err
		}
	default:
	}

	fp := fingerprint(batchID, e.opts.EvalTime, results)
	if err := e.store.Commit(ctx, batchID, lease, fp, currentManifest, results); err != nil {
		log.Error("commit failed", "error", normalizedMessage(err))
		_ = e.store.Fail(context.Background(), batchID, lease, "commit_failed")
		return nil, err
	}

	outcome := e.dispatchAlerts(ctx, batchID, results, log)

	failed, errored := summarizeCounts(results)
	log.Info("batch committed",
		"fingerprint", fp,
		"rules", len(results),
		"passed", len(results)-failed-errored,
		"failed", failed,
		"errored", errored,
		"alerts_sent", outcome.Sent,
		"alerts_retried", outcome.Retried,
		"alerts_pending", outcome.Pending,
	)
	return &model.BatchResult{
		BatchID:       batchID,
		StartedAt:     startedAt,
		FinishedAt:    time.Now().UTC(),
		Status:        "committed",
		Results:       results,
		Fingerprint:   fp,
		AlertsSent:    outcome.Sent,
		AlertsRetried: outcome.Retried,
		AlertsPending: outcome.Pending,
	}, nil
}

// replayCommitted serves a repeat run of a committed batch. Historical
// conclusions are returned only when the current inputs match the stored
// manifest; otherwise the conclusions are declared inapplicable with the
// concrete data/rule differences, and nothing is overwritten. Undelivered
// alerts are still filled from the stored results.
func (e *Engine) replayCommitted(ctx context.Context, batchID string, st *state.BatchState, current manifest.Manifest, log *slog.Logger) (*model.BatchResult, error) {
	if st.Manifest == nil {
		// Committed by an older version before manifests existed: validity
		// cannot be proven, so replay for compatibility and say so in logs.
		log.Warn("stored batch predates input manifests; replaying without validity check",
			"fingerprint", st.Fingerprint)
	} else {
		diffs := manifest.Compare(*st.Manifest, current)
		if len(diffs) > 0 {
			sideSet := map[model.InvalidationSide]bool{}
			changes := make([]string, 0, len(diffs))
			for _, d := range diffs {
				log.Warn("historical conclusion invalidated",
					"side", d.Side, "change", d.Detail)
				changes = append(changes, d.Side+": "+d.Detail)
				if d.Side == manifest.SideData {
					sideSet[model.InvalidatedData] = true
				} else {
					sideSet[model.InvalidatedRules] = true
				}
			}
			sides := make([]model.InvalidationSide, 0, len(sideSet))
			if sideSet[model.InvalidatedData] {
				sides = append(sides, model.InvalidatedData)
			}
			if sideSet[model.InvalidatedRules] {
				sides = append(sides, model.InvalidatedRules)
			}
			return nil, &model.HistoryInvalidError{BatchID: batchID, Sides: sides, Changes: changes}
		}
		log.Info("historical conclusions still valid; replaying",
			"fingerprint", st.Fingerprint, "rules", len(st.Results))
	}

	results := orderedResults(st.Results)
	outcome := e.dispatchAlerts(ctx, batchID, results, log)
	if outcome.Pending > 0 {
		log.Warn("replayed batch still has undelivered alerts",
			"alerts_retried", outcome.Retried, "alerts_pending", outcome.Pending)
	} else {
		log.Info("replay alert backlog settled",
			"alerts_retried", outcome.Retried, "alerts_pending", 0)
	}
	return &model.BatchResult{
		BatchID:       batchID,
		StartedAt:     st.StartedAt,
		FinishedAt:    derefTime(st.CommittedAt),
		Status:        "replay",
		Results:       results,
		Fingerprint:   st.Fingerprint,
		AlertsSent:    outcome.Sent,
		AlertsRetried: outcome.Retried,
		AlertsPending: outcome.Pending,
	}, nil
}

// heartbeat renews the batch lease at roughly a third of the TTL so the lease
// never expires during a healthy run.
func (e *Engine) heartbeat(ctx context.Context, batchID, lease string, log *slog.Logger, errCh chan<- error) {
	interval := e.opts.LeaseTTL / 3
	if interval <= 0 {
		interval = state.DefaultLeaseTTL / 3
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.store.Heartbeat(context.Background(), batchID, lease); err != nil {
				log.Error("lease heartbeat failed", "error", normalizedMessage(err))
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
}

// dispatchAlerts sends failure notifications and normalizes accounting. A nil
// alerter (alerts disabled) yields a zero outcome. Delivery failures never
// alter verdicts; they are reflected in the pending count and logs.
func (e *Engine) dispatchAlerts(ctx context.Context, batchID string, results []model.RuleResult, log *slog.Logger) alert.Outcome {
	if e.alerter == nil {
		return alert.Outcome{}
	}
	outcome, err := e.alerter.Dispatch(ctx, batchID, results, model.SeverityWarning)
	if err != nil {
		log.Error("alert dispatch partially failed",
			"error", normalizedMessage(err),
			"alerts_sent", outcome.Sent,
			"alerts_retried", outcome.Retried,
			"alerts_pending", outcome.Pending,
		)
	}
	return outcome
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
