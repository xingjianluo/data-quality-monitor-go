package manifest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func eval() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }

// manifestBuilder keeps one fixed declared path so two manifests differ only
// in the aspect under test.
type manifestBuilder struct {
	path    string
	content string
	allowed []any
	at      time.Time
	present bool
}

func newManifestBuilder(t *testing.T) *manifestBuilder {
	dir := t.TempDir()
	return &manifestBuilder{
		path:    filepath.Join(dir, "users.csv"),
		content: "id,status\n1,active\n",
		allowed: []any{"active", "pending"},
		at:      eval(),
		present: true,
	}
}

func (b *manifestBuilder) build(t *testing.T) Manifest {
	t.Helper()
	if b.present {
		writeFile(t, b.path, b.content)
	} else {
		_ = os.Remove(b.path)
	}
	decl := model.DatasetDecl{
		Name: "users", Path: b.path, Format: "csv",
		Schema: model.Schema{Fields: []model.FieldDecl{
			{Name: "id", Type: model.TypeInt, Required: true},
			{Name: "status", Type: model.TypeString, Required: true},
		}},
	}
	cfg := &config.Config{Datasets: []model.DatasetDecl{decl}, Rules: []model.Rule{{
		ID: "r-domain", Dataset: "users", Kind: model.KindConsistency, Severity: model.SeverityError,
		Field: "status", Params: map[string]any{"mode": "allowed", "allowed": b.allowed},
	}}}
	return Build(cfg, b.at)
}

func sides(diffs []Diff) map[string]int {
	out := map[string]int{}
	for _, d := range diffs {
		out[d.Side]++
	}
	return out
}

func TestIdenticalInputsMatch(t *testing.T) {
	b := newManifestBuilder(t)
	m1, m2 := b.build(t), b.build(t)
	if m1.Hash == "" || m1.Hash != m2.Hash {
		t.Fatalf("identical inputs must hash equally: %s vs %s", m1.Hash, m2.Hash)
	}
	if diffs := Compare(m1, m2); len(diffs) != 0 {
		t.Fatalf("identical manifests must not differ: %+v", diffs)
	}
	if len(m1.Datasets) != 1 || !m1.Datasets[0].Readable || m1.Datasets[0].ContentSHA == "" {
		t.Fatalf("bad fingerprint: %+v", m1.Datasets)
	}
}

func TestDataContentChangeIsDataSide(t *testing.T) {
	b := newManifestBuilder(t)
	m1 := b.build(t)
	b.content = "id,status\n1,active\n2,pending\n"
	m2 := b.build(t)
	diffs := Compare(m1, m2)
	got := sides(diffs)
	if got[SideData] != 1 || got[SideRules] != 0 {
		t.Fatalf("want one data diff, got %+v (%+v)", got, diffs)
	}
}

func TestSourceFileDeletedIsDataSide(t *testing.T) {
	b := newManifestBuilder(t)
	m1 := b.build(t)
	b.present = false
	m2 := b.build(t)
	if m2.Datasets[0].Readable || m2.Datasets[0].ReadError == "" {
		t.Fatalf("deleted file must be reported: %+v", m2.Datasets[0])
	}
	diffs := Compare(m1, m2)
	got := sides(diffs)
	if got[SideData] != 1 || got[SideRules] != 0 {
		t.Fatalf("want one data diff for deletion, got %+v (%+v)", got, diffs)
	}
	// Restoring identical content removes the difference.
	b.present = true
	m3 := b.build(t)
	if diffs := Compare(m1, m3); len(diffs) != 0 {
		t.Fatalf("restored file should match again: %+v", diffs)
	}
}

func TestRuleParamChangeIsRulesSide(t *testing.T) {
	b := newManifestBuilder(t)
	m1 := b.build(t)
	b.allowed = []any{"active", "pending", "banned"}
	m2 := b.build(t)
	diffs := Compare(m1, m2)
	got := sides(diffs)
	if got[SideRules] != 1 || got[SideData] != 0 {
		t.Fatalf("want one rules diff for changed params, got %+v (%+v)", got, diffs)
	}
}

func TestEvalTimeChangeIsRulesSide(t *testing.T) {
	b := newManifestBuilder(t)
	m1 := b.build(t)
	b.at = b.at.Add(time.Hour)
	m2 := b.build(t)
	diffs := Compare(m1, m2)
	got := sides(diffs)
	if got[SideRules] != 1 || got[SideData] != 0 {
		t.Fatalf("want one rules diff for changed eval time, got %+v (%+v)", got, diffs)
	}
}

func TestBothSidesReportedSeparately(t *testing.T) {
	b := newManifestBuilder(t)
	m1 := b.build(t)
	b.content = "id,status\n1,active\n2,banned\n"
	b.allowed = []any{"active"}
	m2 := b.build(t)
	diffs := Compare(m1, m2)
	got := sides(diffs)
	if got[SideData] != 1 || got[SideRules] != 1 {
		t.Fatalf("want both data and rules diff, got %+v (%+v)", got, diffs)
	}
}
