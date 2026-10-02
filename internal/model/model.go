// Package model defines the core domain types for the data quality monitor:
// rule declarations, datasets, normalized verdicts and batch run results.
package model

import "time"

// RuleKind enumerates the supported declarative rule families.
type RuleKind string

const (
	KindCompleteness RuleKind = "completeness"
	KindUniqueness   RuleKind = "uniqueness"
	KindConsistency  RuleKind = "consistency"
	KindFreshness    RuleKind = "freshness"
)

// Severity controls whether a failed finding produces an alert.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// FieldType is the declared value type used for typed field access.
type FieldType string

const (
	TypeString FieldType = "string"
	TypeInt    FieldType = "int"
	TypeFloat  FieldType = "float"
	TypeBool   FieldType = "bool"
	TypeTime   FieldType = "time"
)

// FieldDecl declares one dataset field.
type FieldDecl struct {
	Name string    `json:"name"`
	Type FieldType `json:"type"`
	// Required declares the field structurally mandatory. When false, a missing
	// key is an explicit null; when true it is a missing-field failure.
	Required bool `json:"required,omitempty"`
	// Format is used by time fields, e.g. "2006-01-02".
	Format string `json:"format,omitempty"`
}

// Schema is the declared shape of a dataset.
type Schema struct {
	Fields []FieldDecl `json:"fields"`
}

// DatasetDecl binds a logical dataset name to a local file.
type DatasetDecl struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	Format string `json:"format"` // "csv" or "jsonl"
	Schema Schema `json:"schema"`
}

// Rule is a declarative quality rule bound to one dataset.
type Rule struct {
	ID       string         `json:"id"`
	Dataset  string         `json:"dataset"`
	Kind     RuleKind       `json:"kind"`
	Severity Severity       `json:"severity"`
	Field    string         `json:"field,omitempty"`
	Fields   []string       `json:"fields,omitempty"`
	Params   map[string]any `json:"params,omitempty"`
}

// Record is one row: Values holds the raw decoded values and Present records
// which keys were explicitly present in the source. FieldIssues holds per-row
// structural problems (missing required fields, type mismatches) discovered at
// load time so they never pass silently.
type Record struct {
	Line        int
	Values      map[string]any
	Present     map[string]bool
	FieldIssues []FieldIssue
}

// FieldIssue is a per-record structural data problem.
type FieldIssue struct {
	Field  string
	Code   ReasonCode
	Detail string
}

// Dataset is a loaded dataset in canonical order.
type Dataset struct {
	Name    string
	Schema  Schema
	Records []Record
	Actual  []string // field names actually observed in source order
}

// ReasonCode is a stable, classifiable failure (or error) category. External
// results always carry one of these instead of raw underlying errors.
type ReasonCode string

const (
	ReasonNone               ReasonCode = ""
	ReasonMissingField       ReasonCode = "missing_field"
	ReasonTypeMismatch       ReasonCode = "type_mismatch"
	ReasonNullValue          ReasonCode = "null_value"
	ReasonDuplicate          ReasonCode = "duplicate_value"
	ReasonValueOutsideSet    ReasonCode = "value_outside_set"
	ReasonValueRange         ReasonCode = "value_out_of_range"
	ReasonRegexMismatch      ReasonCode = "regex_mismatch"
	ReasonStaleRecord        ReasonCode = "stale_record"
	ReasonSchemaFieldMissing ReasonCode = "schema_field_missing"
	ReasonSourceShapeChange  ReasonCode = "source_shape_changed"
	ReasonSourceLoadFailure  ReasonCode = "source_load_failure"
	ReasonRuleInvalid        ReasonCode = "rule_definition_invalid"
	ReasonRuleTimeout        ReasonCode = "rule_timeout"
	ReasonRuleCancelled      ReasonCode = "rule_cancelled"
	ReasonRulePanic          ReasonCode = "rule_panic"
)

// Evidence is one human-readable, deterministic justification line.
type Evidence struct {
	Line   int    `json:"line,omitempty"`
	Field  string `json:"field,omitempty"`
	Detail string `json:"detail"`
}

// Finding is a single rule violation.
type Finding struct {
	Code     ReasonCode `json:"code"`
	Field    string     `json:"field,omitempty"`
	Evidence []Evidence `json:"evidence,omitempty"`
}

// RuleStatus is the normalized per-rule outcome.
type RuleStatus string

const (
	StatusPassed  RuleStatus = "passed"
	StatusFailed  RuleStatus = "failed"
	StatusErrored RuleStatus = "errored"
)

// RuleResult is the normalized verdict of one rule.
type RuleResult struct {
	RuleID   string     `json:"rule_id"`
	Dataset  string     `json:"dataset"`
	Kind     RuleKind   `json:"kind"`
	Severity Severity   `json:"severity"`
	Status   RuleStatus `json:"status"`
	Findings []Finding  `json:"findings,omitempty"`
	// ErrorCode is set for infrastructure-level errors (load/timeout/panic).
	ErrorCode  ReasonCode `json:"error_code,omitempty"`
	ErrorMsg   string     `json:"error_message,omitempty"`
	DurationMS int64      `json:"duration_ms"`
}

// BatchResult is the stable, ordered aggregate of one batch evaluation.
type BatchResult struct {
	BatchID     string       `json:"batch_id"`
	StartedAt   time.Time    `json:"started_at"`
	FinishedAt  time.Time    `json:"finished_at"`
	Status      string       `json:"status"` // committed / replay / aborted
	Results     []RuleResult `json:"results"`
	Fingerprint string       `json:"fingerprint"`
	// AlertReport summarizes notification delivery for this invocation,
	// including retried (newly delivered on a replay) and still-pending alerts.
	AlertReport AlertReport `json:"alert_report"`
}

// AlertReport explains notification delivery for one invocation.
//
//   - Expected: alerts that should exist for the batch conclusions (distinct
//     failed/errored rules at or above the minimum severity).
//   - AlreadyDelivered: already journaled before this invocation (not re-sent).
//   - SentThisRun: successfully delivered during this invocation, including
//     retries that completed an earlier partial/failed dispatch.
//   - Pending: still not successfully delivered after this invocation.
type AlertReport struct {
	Expected         int `json:"expected"`
	AlreadyDelivered int `json:"already_delivered"`
	SentThisRun      int `json:"sent_this_run"`
	Pending          int `json:"pending"`
}

// RuleError is a classifiable engine error carrying a normalized ReasonCode.
type RuleError struct {
	Code    ReasonCode
	Message string
}

func (e *RuleError) Error() string { return string(e.Code) + ": " + e.Message }

// EvalTime is the deterministic wall-clock reference for freshness checks.
type EvalTime = time.Time

// Duration aliases time.Duration for engine options.
type Duration = time.Duration
