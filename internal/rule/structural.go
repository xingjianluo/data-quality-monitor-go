package rule

import (
	"fmt"
	"sort"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// structuralSpec 为内置结构校验构造一个稳定 ID 的伪规则。
func structuralSpec(dataset string) model.RuleSpec {
	return model.RuleSpec{
		ID:       dataset + ".structural",
		Type:     model.RuleStructural,
		Dataset:  dataset,
		Severity: "error",
	}
}

// Structural 对一个数据集执行内置结构校验：
//   - loadReason 非空：数据集本身无法加载（source_unreadable）；
//   - baseline 非空且与当前声明指纹不同：形态变化（schema_drift）；
//   - 声明列在记录中整体缺失：字段缺失（field_missing）；
//   - 取值与声明类型不符：类型不匹配（type_mismatch）。
//
// 结构类问题一律 StatusError：不静默通过，且与数据违反规则（fail）可区分。
func Structural(ds *model.Dataset, loadReason model.ReasonCode, loadMessage string, baseline string) model.RuleResult {
	spec := structuralSpec("")
	if ds != nil {
		spec = structuralSpec(ds.Name)
	}
	if loadReason != model.ReasonNone {
		return errResult(spec, loadReason, loadMessage)
	}
	if ds == nil {
		return errResult(spec, model.ReasonInternal, "dataset unavailable and no load reason provided")
	}

	var findings []model.Finding
	// 实际指纹基于记录中出现的键集合（与引擎持久化的 observed schema 口径一致）。
	seen := make(map[string]bool)
	for _, rec := range ds.Records {
		for k := range rec {
			seen[k] = true
		}
	}
	actualCols := make([]model.Column, 0, len(seen))
	actualNames := make([]string, 0, len(seen))
	for k := range seen {
		actualNames = append(actualNames, k)
	}
	sort.Strings(actualNames)
	for _, n := range actualNames {
		if col, ok := ds.Schema.ColumnByName(n); ok {
			actualCols = append(actualCols, col)
		} else {
			actualCols = append(actualCols, model.Column{Name: n, Type: model.DataType("any")})
		}
	}
	current := (model.Schema{Columns: actualCols}).Fingerprint()
	if baseline != "" && baseline != current {
		findings = append(findings, model.Finding{
			Reason:  model.ReasonSchemaDrift,
			Message: fmt.Sprintf("schema changed from baseline %q to %q", baseline, current),
		})
	}

	// 列缺失检测：以声明列为基准，记录中没有任何一行出现该键即判缺失。
	missingCols := make([]string, 0)
	for _, col := range ds.Schema.Columns {
		if !seen[col.Name] {
			missingCols = append(missingCols, col.Name)
		}
	}
	sort.Strings(missingCols)
	for _, name := range missingCols {
		findings = append(findings, model.Finding{
			Reason:  model.ReasonFieldMissing,
			Column:  name,
			Message: fmt.Sprintf("declared column %q is absent from every record", name),
		})
	}

	// 类型不匹配检测：仅检查存在的声明列。
	columns := make([]string, 0, len(ds.Schema.Columns))
	for _, col := range ds.Schema.Columns {
		if seen[col.Name] {
			columns = append(columns, col.Name)
		}
	}
	bad := checkTypeMismatches(ds, columns)
	badCols := make([]string, 0, len(bad))
	for name := range bad {
		badCols = append(badCols, name)
	}
	sort.Strings(badCols)
	for _, name := range badCols {
		findings = append(findings, model.Finding{
			Reason:  model.ReasonTypeMismatch,
			Column:  name,
			Count:   bad[name],
			Message: fmt.Sprintf("column %q has %d value(s) not matching declared type", name, bad[name]),
		})
	}

	res := model.RuleResult{
		RuleID:   spec.ID,
		RuleType: model.RuleStructural,
		Dataset:  ds.Name,
		Checked:  len(ds.Records),
	}
	if len(findings) > 0 {
		res.Status = model.StatusError
		res.Findings = findings
	} else {
		res.Status = model.StatusPass
	}
	return res
}
