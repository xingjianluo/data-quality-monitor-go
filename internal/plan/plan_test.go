package plan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileResolvesPathsAndWatermark(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(dir, "plan.json")
	content := `{
		"batch_id": "b",
		"ordinal": 1,
		"watermark": "2026-09-23T12:00:00Z",
		"base_dir": "data",
		"datasets": [
			{"name": "d", "path": "d.json", "columns": [{"name": "id", "type": "int"}]}
		],
		"rules": [
			{"id": "r1", "type": "uniqueness", "dataset": "d", "columns": ["id"]}
		]
	}`
	if err := os.WriteFile(planPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(data, "d.json")
	if p.Datasets[0].Path != want {
		t.Fatalf("path = %q, want %q", p.Datasets[0].Path, want)
	}
	req, err := p.ToRunRequest()
	if err != nil {
		t.Fatal(err)
	}
	if req.Watermark.IsZero() || req.BatchID != "b" || req.Ordinal != 1 || len(req.Rules) != 1 {
		t.Fatalf("request conversion wrong: %+v", req)
	}
}

func TestLoadFileRejectsBadWatermark(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(planPath, []byte(`{
		"batch_id": "b", "ordinal": 1, "watermark": "not-a-time",
		"datasets": [{"name":"d","path":"d.json","columns":[{"name":"id","type":"int"}]}],
		"rules": []
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ToRunRequest(); err == nil {
		t.Fatal("bad watermark must error")
	}
}
