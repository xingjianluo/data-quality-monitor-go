// Package model 定义数据质量校验流程的核心领域模型：
// 数据形态（Schema/Dataset）、声明式规则（RuleSpec）、
// 判定结果（Finding/RuleResult/BatchResult）与告警（Alert）。
package model

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// DataType 是声明的列类型。
type DataType string

const (
	TypeString DataType = "string"
	TypeInt    DataType = "int"
	TypeFloat  DataType = "float"
	TypeBool   DataType = "bool"
	TypeTime   DataType = "time"
)

// Column 声明数据集中一列的名称与类型。
type Column struct {
	Name string   `json:"name"`
	Type DataType `json:"type"`
}

// Schema 是一个数据集的列结构快照。
type Schema struct {
	Columns []Column `json:"columns"`
}

// Fingerprint 返回列结构的确定性指纹（与列顺序无关），用于形态变化检测。
func (s Schema) Fingerprint() string {
	parts := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		parts[i] = string(c.Type) + ":" + c.Name
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// ColumnByName 按名称查找列，未找到返回 false。
func (s Schema) ColumnByName(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// DatasetDecl 声明一个数据集：标识、数据文件位置与期望结构。
type DatasetDecl struct {
	Name    string   `json:"name"`
	Path    string   `json:"path"`
	Columns []Column `json:"columns"`
}

// Schema 返回声明对应的结构快照。
func (d DatasetDecl) Schema() Schema { return Schema{Columns: d.Columns} }

// RuleType 是可归类的规则类别。
type RuleType string

const (
	RuleCompleteness RuleType = "completeness" // 完整性：列非空率不低于阈值
	RuleUniqueness   RuleType = "uniqueness"   // 唯一性：列组合无重复
	RuleConsistency  RuleType = "consistency"  // 取值一致性：取值落在允许集合内
	RuleFreshness    RuleType = "freshness"    // 新鲜度：时间列不早于水位线
	RuleStructural   RuleType = "structural"   // 内置结构校验（字段缺失/类型不匹配/形态变化），不可由用户声明
)

// RuleSpec 是一条声明式规则。
type RuleSpec struct {
	ID         string   `json:"id"`
	Type       RuleType `json:"type"`
	Dataset    string   `json:"dataset"`
	Columns    []string `json:"columns,omitempty"`
	MinNonNull float64  `json:"min_non_null,omitempty"` // completeness 专用，取值 (0,1]
	Allowed    []string `json:"allowed,omitempty"`      // consistency 专用
	MaxAge     string   `json:"max_age,omitempty"`      // freshness 专用，如 "24h"
	Severity   string   `json:"severity,omitempty"`     // 默认 "error"
}

// Validate 校验规则声明是否合法，返回首个问题描述；合法时返回空串。
func (r RuleSpec) Validate() string {
	if strings.TrimSpace(r.ID) == "" {
		return "rule id is required"
	}
	if r.Type == RuleStructural {
		return "structural rules are built-in and cannot be declared"
	}
	if strings.TrimSpace(r.Dataset) == "" {
		return fmt.Sprintf("rule %q: dataset is required", r.ID)
	}
	switch r.Type {
	case RuleCompleteness:
		if len(r.Columns) != 1 {
			return fmt.Sprintf("rule %q: completeness requires exactly one column", r.ID)
		}
		if r.MinNonNull <= 0 || r.MinNonNull > 1 {
			return fmt.Sprintf("rule %q: min_non_null must be within (0,1]", r.ID)
		}
	case RuleUniqueness:
		if len(r.Columns) == 0 {
			return fmt.Sprintf("rule %q: uniqueness requires at least one column", r.ID)
		}
	case RuleConsistency:
		if len(r.Columns) != 1 {
			return fmt.Sprintf("rule %q: consistency requires exactly one column", r.ID)
		}
		if len(r.Allowed) == 0 {
			return fmt.Sprintf("rule %q: consistency requires a non-empty allowed set", r.ID)
		}
	case RuleFreshness:
		if len(r.Columns) != 1 {
			return fmt.Sprintf("rule %q: freshness requires exactly one column", r.ID)
		}
		if _, err := time.ParseDuration(r.MaxAge); err != nil {
			return fmt.Sprintf("rule %q: max_age %q is not a duration", r.ID, r.MaxAge)
		}
	default:
		return fmt.Sprintf("rule %q: unknown rule type %q", r.ID, r.Type)
	}
	return ""
}

// Cell 是一条记录中某个字段的取值，保留原始值与解析结果。
type Cell struct {
	Present bool   // 键是否存在（缺失与显式 null 可区分）
	Raw     any    // 原始 JSON 值
	Text    string // 归一化后的文本表示（用于唯一性/一致性比较）
}

// Record 是一条数据记录：列名到取值的映射。
type Record map[string]Cell

// Dataset 是加载后的数据集内容。
type Dataset struct {
	Name    string
	Schema  Schema
	Records []Record
}

// RuleStatus 是单条规则的判定结论。
type RuleStatus string

const (
	StatusPass  RuleStatus = "pass"
	StatusFail  RuleStatus = "fail"  // 数据违反规则
	StatusError RuleStatus = "error" // 规则无法判定（输入缺失/类型不符/形态变化/超时/取消）
)

// ReasonCode 是可区分的失败原因分类。
type ReasonCode string

const (
	ReasonNone             ReasonCode = ""
	ReasonRuleViolation    ReasonCode = "rule_violation"    // 数据不满足规则
	ReasonFieldMissing     ReasonCode = "field_missing"     // 声明列在数据中不存在
	ReasonTypeMismatch     ReasonCode = "type_mismatch"     // 取值与声明类型不符
	ReasonSchemaDrift      ReasonCode = "schema_drift"      // 数据源形态相对历史基线变化
	ReasonSourceUnreadable ReasonCode = "source_unreadable" // 数据文件缺失或无法解析
	ReasonRuleTimeout      ReasonCode = "rule_timeout"      // 单条规则超时
	ReasonRuleCancelled    ReasonCode = "rule_cancelled"    // 运行被取消
	ReasonInvalidRule      ReasonCode = "invalid_rule"      // 规则声明非法
	ReasonInputIncomplete  ReasonCode = "input_incomplete"  // 批次输入未齐
	ReasonStateRollback    ReasonCode = "state_rollback"    // 状态回退被拒绝
	ReasonStateConflict    ReasonCode = "state_conflict"    // 状态内容冲突
	ReasonInternal         ReasonCode = "internal"          // 其他内部异常（归一化后）
)

// Finding 是一条可归类的判定发现。
type Finding struct {
	Reason  ReasonCode `json:"reason"`
	Column  string     `json:"column,omitempty"`
	Message string     `json:"message"`
	Count   int        `json:"count,omitempty"`
}

// RuleResult 是单条规则对一个数据集的判定结果。
type RuleResult struct {
	RuleID   string     `json:"rule_id"`
	RuleType RuleType   `json:"rule_type"`
	Dataset  string     `json:"dataset"`
	Status   RuleStatus `json:"status"`
	Findings []Finding  `json:"findings,omitempty"`
	Checked  int        `json:"checked"`
}

// InputSummary 是单个数据集输入的摘要（行数与结构指纹），用于日志与复现。
type InputSummary struct {
	Dataset string `json:"dataset"`
	Rows    int    `json:"rows"`
	Schema  string `json:"schema_fingerprint"`
}

// BatchResult 是一个批次全部规则的稳定有序结果。
type BatchResult struct {
	BatchID   string         `json:"batch_id"`
	Ordinal   uint64         `json:"ordinal"`
	Evaluated time.Time      `json:"evaluated_at"`
	Inputs    []InputSummary `json:"inputs"`
	Results   []RuleResult   `json:"results"`
}

// Status 汇总整批结论：任一 error 则 error，否则任一 fail 则 fail，否则 pass。
func (b BatchResult) Status() RuleStatus {
	status := StatusPass
	for _, r := range b.Results {
		switch r.Status {
		case StatusError:
			return StatusError
		case StatusFail:
			status = StatusFail
		}
	}
	return status
}

// Alert 是一条对外告警，由 Finding 归一化而来，不暴露底层错误细节。
type Alert struct {
	BatchID  string     `json:"batch_id"`
	Ordinal  uint64     `json:"ordinal"`
	RuleID   string     `json:"rule_id"`
	Dataset  string     `json:"dataset"`
	Reason   ReasonCode `json:"reason"`
	Severity string     `json:"severity"`
	Message  string     `json:"message"`
}

// SpecKey 返回规则声明的稳定文本指纹，用于判定两次运行的规则声明是否一致。
func (r RuleSpec) SpecKey() string {
	return fmt.Sprintf("%s|%s|%s|%v|%.6f|%v|%s",
		r.ID, r.Type, r.Dataset, r.Columns, r.MinNonNull, r.Allowed, r.MaxAge)
}
