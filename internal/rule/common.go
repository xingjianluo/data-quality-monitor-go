package rule

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// typedCell 按声明类型检查一个单元格：
// text 为归一化文本；nonNull 表示非缺失且非显式 null；matched 表示取值与声明类型匹配。
// 显式 null / 键缺失视为类型匹配（由完整性规则负责），但 nonNull=false。
func typedCell(col model.Column, cell model.Cell) (text string, nonNull bool, matched bool) {
	nonNull = cell.Present && cell.Raw != nil
	if !nonNull {
		return cell.Text, false, true
	}
	switch col.Type {
	case model.TypeString:
		s, ok := cell.Raw.(string)
		return s, true, ok
	case model.TypeBool:
		_, ok := cell.Raw.(bool)
		return cell.Text, true, ok
	case model.TypeInt:
		n, ok := cell.Raw.(json.Number)
		if !ok {
			return cell.Text, true, false
		}
		if _, err := strconv.ParseInt(n.String(), 10, 64); err != nil {
			return n.String(), true, false
		}
		return n.String(), true, true
	case model.TypeFloat:
		n, ok := cell.Raw.(json.Number)
		if !ok {
			return cell.Text, true, false
		}
		if _, err := n.Float64(); err != nil {
			return n.String(), true, false
		}
		return n.String(), true, true
	case model.TypeTime:
		s, ok := cell.Raw.(string)
		if !ok {
			return cell.Text, true, false
		}
		if _, err := time.Parse(time.RFC3339, s); err != nil {
			return s, true, false
		}
		return s, true, true
	default:
		return cell.Text, true, false
	}
}

// checkTypeMismatches 扫描数据集所有记录中指定列的类型匹配情况，
// 返回按列名聚合的不匹配数与首个出错位置说明。
func checkTypeMismatches(ds *model.Dataset, columns []string) map[string]int {
	bad := make(map[string]int)
	for _, name := range columns {
		col, ok := ds.Schema.ColumnByName(name)
		if !ok {
			continue
		}
		for _, rec := range ds.Records {
			cell := rec[name]
			if _, nonNull, matched := typedCell(col, cell); nonNull && !matched {
				bad[name]++
			}
		}
	}
	return bad
}

// joinCols 稳定拼接列名，用于消息文本。
func joinCols(cols []string) string { return strings.Join(cols, "+") }

func errResult(spec model.RuleSpec, reason model.ReasonCode, msg string) model.RuleResult {
	return model.RuleResult{
		RuleID:   spec.ID,
		RuleType: spec.Type,
		Dataset:  spec.Dataset,
		Status:   model.StatusError,
		Findings: []model.Finding{{Reason: reason, Message: msg}},
	}
}

func passResult(spec model.RuleSpec, checked int) model.RuleResult {
	return model.RuleResult{
		RuleID:   spec.ID,
		RuleType: spec.Type,
		Dataset:  spec.Dataset,
		Status:   model.StatusPass,
		Checked:  checked,
	}
}

func failResult(spec model.RuleSpec, checked int, findings ...model.Finding) model.RuleResult {
	return model.RuleResult{
		RuleID:   spec.ID,
		RuleType: spec.Type,
		Dataset:  spec.Dataset,
		Status:   model.StatusFail,
		Checked:  checked,
		Findings: findings,
	}
}

// resolveDataset 取规则目标数据集；未加载（含加载失败）时返回 error 结果，绝不静默通过。
func resolveDataset(spec model.RuleSpec, ec EvalContext) (*model.Dataset, model.RuleResult, bool) {
	ds, ok := ec.Datasets[spec.Dataset]
	if !ok || ds == nil {
		return nil, errResult(spec, model.ReasonSourceUnreadable,
			"dataset "+strconv.Quote(spec.Dataset)+" is not available (missing or failed to load)"), false
	}
	return ds, model.RuleResult{}, true
}

// structuralGate 在数据规则判定前检查结构前提：
// 目标列整体缺失或存在类型不匹配时，数据规则不得判定，直接返回 error 结果。
func structuralGate(spec model.RuleSpec, ds *model.Dataset, columns []string) (model.RuleResult, bool) {
	seen := make(map[string]bool)
	for _, rec := range ds.Records {
		for k := range rec {
			seen[k] = true
		}
	}
	present := make([]string, 0, len(columns))
	for _, name := range columns {
		if _, declared := ds.Schema.ColumnByName(name); !declared {
			return errResult(spec, model.ReasonFieldMissing,
				"column "+strconv.Quote(name)+" is not declared in dataset "+strconv.Quote(ds.Name)), true
		}
		if !seen[name] {
			return errResult(spec, model.ReasonFieldMissing,
				"column "+strconv.Quote(name)+" is absent from every record of dataset "+strconv.Quote(ds.Name)), true
		}
		present = append(present, name)
	}
	if bad := checkTypeMismatches(ds, present); len(bad) > 0 {
		names := make([]string, 0, len(bad))
		for n := range bad {
			names = append(names, n)
		}
		sort.Strings(names)
		return errResult(spec, model.ReasonTypeMismatch,
			"dataset "+strconv.Quote(ds.Name)+" has type mismatches in column(s) "+strings.Join(names, ",")), true
	}
	return model.RuleResult{}, false
}

// maybeSleep 按测试钩子决定是否睡眠（仅测试使用）。
func maybeSleep(ctx context.Context, ec EvalContext, ruleID string) error {
	if ec.TestHookPerRecordDelay <= 0 {
		return nil
	}
	if ec.TestHookSlowRuleID != "" && ec.TestHookSlowRuleID != ruleID {
		return nil
	}
	return sleepCtx(ctx, ec.TestHookPerRecordDelay)
}

// sleepCtx 睡眠 d 或直到 ctx 取消。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
