// Package rule 实现四类可归类规则的判定器：
// 完整性、唯一性、取值一致性、新鲜度。
// 每个判定器接收当前批次的所有数据集，返回稳定有序的判定结果。
package rule

import (
	"context"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// EvalContext 携带判定所需的批次上下文。
type EvalContext struct {
	BatchID   string
	Watermark time.Time // 批次评估水位线（新鲜度判定依据，重跑时保持不变）
	Datasets  map[string]*model.Dataset
	// TestHookPerRecordDelay 仅供测试：每条记录处理前睡眠，用于确定性触发超时。
	TestHookPerRecordDelay time.Duration
	// TestHookSlowRuleID 仅供测试：非空时延迟钩子只作用于该规则。
	TestHookSlowRuleID string
}

// Evaluator 是规则判定器接口。
type Evaluator interface {
	Evaluate(ctx context.Context, ec EvalContext) (model.RuleResult, error)
}

// New 按规则类型构造判定器；非法类型返回归一化错误。
func New(spec model.RuleSpec) (Evaluator, error) {
	if msg := spec.Validate(); msg != "" {
		return nil, errs.New(model.ReasonInvalidRule, "%s", msg)
	}
	switch spec.Type {
	case model.RuleCompleteness:
		return completenessEval{spec: spec}, nil
	case model.RuleUniqueness:
		return uniquenessEval{spec: spec}, nil
	case model.RuleConsistency:
		return consistencyEval{spec: spec}, nil
	case model.RuleFreshness:
		return freshnessEval{spec: spec}, nil
	default:
		return nil, errs.New(model.ReasonInvalidRule, "rule %q: unsupported type %q", spec.ID, spec.Type)
	}
}

// EvaluateAll 并发执行多条规则，单条失败/超时/取消/panic 不影响其他规则；
// 结果严格按 specs 原始顺序稳定返回。并发度由 concurrency 控制。
func EvaluateAll(ctx context.Context, specs []model.RuleSpec, ec EvalContext, concurrency int, perRuleTimeout time.Duration) []model.RuleResult {
	if concurrency <= 0 {
		concurrency = 4
	}
	sem := make(chan struct{}, concurrency)
	results := make([]model.RuleResult, len(specs))
	done := make(chan struct{}, len(specs))

	for i, spec := range specs {
		go func(i int, spec model.RuleSpec) {
			defer func() {
				// panic 归一化，不透出底层堆栈，且不影响其他规则。
				if r := recover(); r != nil {
					results[i] = model.RuleResult{
						RuleID:   spec.ID,
						RuleType: spec.Type,
						Dataset:  spec.Dataset,
						Status:   model.StatusError,
						Findings: []model.Finding{{
							Reason:  model.ReasonInternal,
							Message: "rule evaluation panicked",
						}},
					}
				}
				done <- struct{}{}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = evaluateOne(ctx, spec, ec, perRuleTimeout)
		}(i, spec)
	}
	for range specs {
		<-done
	}
	return results
}

// evaluateOne 在独立的超时上下文内执行单条规则。
func evaluateOne(parent context.Context, spec model.RuleSpec, ec EvalContext, perRuleTimeout time.Duration) model.RuleResult {
	eval, err := New(spec)
	if err != nil {
		return ruleErrorResult(spec, errs.CodeOf(err), err.Error())
	}
	ctx := parent
	cancel := func() {}
	if perRuleTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, perRuleTimeout)
	}
	defer cancel()

	res, err := eval.Evaluate(ctx, ec)
	if err == nil {
		return res
	}
	return ruleErrorResult(spec, classifyCtxError(ctx, err), "rule could not be evaluated")
}

// classifyCtxError 区分超时、取消与其他错误，其他错误统一归一化为 internal。
func classifyCtxError(ctx context.Context, err error) model.ReasonCode {
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return model.ReasonRuleTimeout
	case err == context.DeadlineExceeded:
		return model.ReasonRuleTimeout
	case ctx.Err() == context.Canceled || err == context.Canceled:
		return model.ReasonRuleCancelled
	default:
		return errs.CodeOf(err)
	}
}

func ruleErrorResult(spec model.RuleSpec, reason model.ReasonCode, msg string) model.RuleResult {
	return model.RuleResult{
		RuleID:   spec.ID,
		RuleType: spec.Type,
		Dataset:  spec.Dataset,
		Status:   model.StatusError,
		Findings: []model.Finding{{Reason: reason, Message: msg}},
	}
}
