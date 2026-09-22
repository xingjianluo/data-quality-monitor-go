package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadMissingVsNullDistinguished(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "d.json", `[
		{"a": "x", "b": null},
		{"a": "y"}
	]`)
	decl := model.DatasetDecl{Name: "d", Path: path, Columns: []model.Column{
		{Name: "a", Type: model.TypeString}, {Name: "b", Type: model.TypeString},
	}}
	res := NewLoader(2).Load(context.Background(), []model.DatasetDecl{decl})
	if len(res.Failures) != 0 {
		t.Fatalf("unexpected failures: %+v", res.Failures)
	}
	ds := res.Datasets["d"]
	if len(ds.Records) != 2 {
		t.Fatalf("rows = %d", len(ds.Records))
	}
	// 第一行：b 存在且显式 null；第二行：b 键缺失。
	if !ds.Records[0]["b"].Present || ds.Records[0]["b"].Raw != nil {
		t.Fatal("explicit null must be Present with Raw=nil")
	}
	if cell, ok := ds.Records[1]["b"]; ok && cell.Present {
		t.Fatal("absent key must yield non-present zero cell")
	}
}

func TestLoadFailuresDoNotBlockOthers(t *testing.T) {
	dir := t.TempDir()
	good := writeFile(t, dir, "good.json", `[{"a": 1}]`)
	decls := []model.DatasetDecl{
		{Name: "missing", Path: filepath.Join(dir, "nope.json"), Columns: []model.Column{{Name: "a", Type: model.TypeInt}}},
		{Name: "good", Path: good, Columns: []model.Column{{Name: "a", Type: model.TypeInt}}},
		{Name: "broken", Path: writeFile(t, dir, "broken.json", `{not-json`), Columns: []model.Column{{Name: "a"}}},
	}
	res := NewLoader(3).Load(context.Background(), decls)
	if len(res.Failures) != 2 {
		t.Fatalf("want 2 failures, got %d: %+v", len(res.Failures), res.Failures)
	}
	if res.Failures[0].Dataset != "missing" || res.Failures[0].Reason != model.ReasonSourceUnreadable {
		t.Fatalf("first failure must be the missing dataset in decl order: %+v", res.Failures[0])
	}
	if _, ok := res.Datasets["good"]; !ok {
		t.Fatal("good dataset must still be loaded")
	}
}

func TestLoadFailureOrderIsStable(t *testing.T) {
	dir := t.TempDir()
	decls := []model.DatasetDecl{
		{Name: "a", Path: filepath.Join(dir, "a.json")},
		{Name: "b", Path: filepath.Join(dir, "b.json")},
		{Name: "c", Path: filepath.Join(dir, "c.json")},
	}
	for run := 0; run < 10; run++ {
		res := NewLoader(3).Load(context.Background(), decls)
		if len(res.Failures) != 3 || res.Failures[0].Dataset != "a" || res.Failures[2].Dataset != "c" {
			t.Fatalf("failure order unstable on run %d: %+v", run, res.Failures)
		}
	}
}
