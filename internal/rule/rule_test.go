package rule

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func mustDataset(t *testing.T, name string, cols []model.Column, rows []map[string]any) *model.Dataset {
	t.Helper()
	records := make([]model.Record, len(rows))
	for i, row := range rows {
		rec := make(model.Record, len(row))
		for k, v := range row {
			cell := model.Cell{Present: true, Raw: v}
			switch tv := v.(type) {
			case nil:
				cell.Raw = nil
			case string:
				cell.Text = tv
			case int64:
				// 与 source 加载器一致：JSON 数字统一为 json.Number。
				cell.Raw = json.Number(strconv.FormatInt(tv, 10))
				cell.Text = string(cell.Raw.(json.Number))
			case bool:
				if tv {
					cell.Text = "true"
				} else {
					cell.Text = "false"
				}
			default:
				cell.Text = ""
			}
			rec[k] = cell
		}
		records[i] = rec
	}
	return &model.Dataset{Name: name, Schema: model.Schema{Columns: cols}, Records: records}
}

func TestCompleteness(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{
		{"id": int64(1)}, {"id": nil}, {"x": int64(3)}, // 显式 null 与键缺失都算不完整
	})
	ec := EvalContext{Datasets: map[string]*model.Dataset{"d": ds}}
	spec := model.RuleSpec{ID: "c", Type: model.RuleCompleteness, Dataset: "d",
		Columns: []string{"id"}, MinNonNull: 0.99}
	res, err := EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 2, time.Second)[0], error(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != model.StatusFail || res.Findings[0].Reason != model.ReasonRuleViolation || res.Findings[0].Count != 2 {
		t.Fatalf("unexpected: %+v", res)
	}
	spec.MinNonNull = 0.33
	res = EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 2, time.Second)[0]
	if res.Status != model.StatusPass {
		t.Fatalf("1/3 = 0.333 should pass 0.33: %+v", res)
	}
}

func TestUniqueness(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{
		{"id": int64(1)}, {"id": int64(2)}, {"id": int64(1)},
	})
	ec := EvalContext{Datasets: map[string]*model.Dataset{"d": ds}}
	spec := model.RuleSpec{ID: "u", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"id"}}
	res := EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 2, time.Second)[0]
	if res.Status != model.StatusFail || res.Findings[0].Count != 1 {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestConsistency(t *testing.T) {
	cols := []model.Column{{Name: "s", Type: model.TypeString}}
	ds := mustDataset(t, "d", cols, []map[string]any{
		{"s": "a"}, {"s": "b"}, {"s": "zzz"},
	})
	ec := EvalContext{Datasets: map[string]*model.Dataset{"d": ds}}
	spec := model.RuleSpec{ID: "e", Type: model.RuleConsistency, Dataset: "d",
		Columns: []string{"s"}, Allowed: []string{"a", "b"}}
	res := EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 2, time.Second)[0]
	if res.Status != model.StatusFail || res.Findings[0].Count != 1 {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestFreshness(t *testing.T) {
	cols := []model.Column{{Name: "ts", Type: model.TypeTime}}
	ds := mustDataset(t, "d", cols, []map[string]any{
		{"ts": "2026-09-23T11:00:00Z"},
		{"ts": "2026-09-20T11:00:00Z"}, // 超过 24h
	})
	wm := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	ec := EvalContext{Datasets: map[string]*model.Dataset{"d": ds}, Watermark: wm}
	spec := model.RuleSpec{ID: "f", Type: model.RuleFreshness, Dataset: "d",
		Columns: []string{"ts"}, MaxAge: "24h"}
	res := EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 2, time.Second)[0]
	if res.Status != model.StatusFail || res.Findings[0].Count != 1 {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestStructuralGateReasonsAreDistinguishable(t *testing.T) {
	// 字段缺失（列声明存在但所有行都没有该键）-> error/field_missing
	cols := []model.Column{{Name: "id", Type: model.TypeInt}, {Name: "ghost", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{{"id": int64(1)}})
	ec := EvalContext{Datasets: map[string]*model.Dataset{"d": ds}}
	spec := model.RuleSpec{ID: "c", Type: model.RuleCompleteness, Dataset: "d",
		Columns: []string{"ghost"}, MinNonNull: 1}
	res := EvaluateAll(context.Background(), []model.RuleSpec{spec}, ec, 1, time.Second)[0]
	if res.Status != model.StatusError || res.Findings[0].Reason != model.ReasonFieldMissing {
		t.Fatalf("want field_missing, got %+v", res)
	}

	// 类型不匹配 -> error/type_mismatch，与数据违反规则的 fail 可区分
	ds2 := mustDataset(t, "d", []model.Column{{Name: "id", Type: model.TypeInt}},
		[]map[string]any{{"id": "not-an-int"}})
	ec2 := EvalContext{Datasets: map[string]*model.Dataset{"d": ds2}}
	spec2 := model.RuleSpec{ID: "u", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"id"}}
	res2 := EvaluateAll(context.Background(), []model.RuleSpec{spec2}, ec2, 1, time.Second)[0]
	if res2.Status != model.StatusError || res2.Findings[0].Reason != model.ReasonTypeMismatch {
		t.Fatalf("want type_mismatch, got %+v", res2)
	}
}

func TestStructuralBuiltInReasons(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{{"id": "x"}})
	res := Structural(ds, "", "", "int:id,int:extra")
	if res.RuleType != model.RuleStructural || res.Status != model.StatusError {
		t.Fatalf("want structural error, got %+v", res)
	}
	reasons := map[model.ReasonCode]bool{}
	for _, f := range res.Findings {
		reasons[f.Reason] = true
	}
	if !reasons[model.ReasonSchemaDrift] || !reasons[model.ReasonTypeMismatch] {
		t.Fatalf("want schema_drift + type_mismatch, got %+v", res.Findings)
	}

	// 加载失败显式归一化。
	res2 := Structural(&model.Dataset{Name: "d", Schema: model.Schema{Columns: cols}},
		model.ReasonSourceUnreadable, "no such file", "")
	if res2.Status != model.StatusError || res2.Findings[0].Reason != model.ReasonSourceUnreadable {
		t.Fatalf("want source_unreadable, got %+v", res2)
	}
}

func TestTimeoutIsolatesRuleAndOrderStable(t *testing.T) {
	// 两条规则：r-slow 故意超过超时，r-fast 正常。
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{{"id": int64(1)}, {"id": int64(2)}})
	ec := EvalContext{
		Datasets:               map[string]*model.Dataset{"d": ds},
		TestHookPerRecordDelay: 120 * time.Millisecond,
		TestHookSlowRuleID:     "r-slow",
	}
	slow := model.RuleSpec{ID: "r-slow", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"id"}}
	fast := model.RuleSpec{ID: "r-fast", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"id"}}

	results := EvaluateAll(context.Background(),
		[]model.RuleSpec{slow, fast}, ec, 2, 100*time.Millisecond)
	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
	if results[0].RuleID != "r-slow" || results[1].RuleID != "r-fast" {
		t.Fatalf("order not stable: %s, %s", results[0].RuleID, results[1].RuleID)
	}
	if results[0].Status != model.StatusError || results[0].Findings[0].Reason != model.ReasonRuleTimeout {
		t.Fatalf("r-slow want rule_timeout, got %+v", results[0])
	}
	if results[1].Status != model.StatusPass {
		t.Fatalf("r-fast must be unaffected, got %+v", results[1])
	}
}

func TestParentCancelClassifiedAsCancelled(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	ds := mustDataset(t, "d", cols, []map[string]any{{"id": int64(1)}})
	ec := EvalContext{
		Datasets:               map[string]*model.Dataset{"d": ds},
		TestHookPerRecordDelay: 50 * time.Millisecond,
	}
	spec := model.RuleSpec{ID: "r", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"id"}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	results := EvaluateAll(ctx, []model.RuleSpec{spec}, ec, 1, 0)
	if results[0].Status != model.StatusError || results[0].Findings[0].Reason != model.ReasonRuleCancelled {
		t.Fatalf("want rule_cancelled, got %+v", results[0])
	}
}

func TestInvalidRuleNormalized(t *testing.T) {
	spec := model.RuleSpec{ID: "bad", Type: "completeness", Dataset: "d"} // 缺列/阈值
	results := EvaluateAll(context.Background(), []model.RuleSpec{spec},
		EvalContext{Datasets: map[string]*model.Dataset{}}, 1, time.Second)
	if results[0].Status != model.StatusError || results[0].Findings[0].Reason != model.ReasonInvalidRule {
		t.Fatalf("want invalid_rule, got %+v", results[0])
	}
}
