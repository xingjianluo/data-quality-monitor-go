package datasource

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

func csvDecl(p string) model.DatasetDecl {
	return model.DatasetDecl{
		Name: "d", Path: p, Format: "csv",
		Schema: model.Schema{Fields: []model.FieldDecl{
			{Name: "id", Type: model.TypeInt, Required: true},
			{Name: "name", Type: model.TypeString, Required: true},
			{Name: "age", Type: model.TypeInt, Required: false},
		}},
	}
}

func issueCodes(ds *model.Dataset, line int) []model.ReasonCode {
	for _, r := range ds.Records {
		if r.Line == line {
			var out []model.ReasonCode
			for _, fi := range r.FieldIssues {
				out = append(out, fi.Code)
			}
			return out
		}
	}
	return nil
}

func TestLoadCSVClassifiesProblems(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "d.csv", "id,name,age\n1,Alice,30\n2,Bob,\nnotint,Carol,40\n")
	l := New("")
	ds, issues, err := l.Load(context.Background(), csvDecl(p))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("unexpected shape issues: %+v", issues)
	}
	if len(ds.Records) != 3 {
		t.Fatalf("want 3 records, got %d", len(ds.Records))
	}
	if got := issueCodes(ds, 3); len(got) != 1 || got[0] != model.ReasonNullValue {
		t.Fatalf("empty optional cell: want [null_value], got %v", got)
	}
	if got := issueCodes(ds, 4); len(got) != 1 || got[0] != model.ReasonTypeMismatch {
		t.Fatalf("bad int: want [type_mismatch], got %v", got)
	}
	// Good row must carry typed value and no issues.
	if ds.Records[0].Values["id"] != int64(1) {
		t.Fatalf("id not decoded to int64: %T %v", ds.Records[0].Values["id"], ds.Records[0].Values["id"])
	}
}

func TestLoadJSONLNullsAndTypes(t *testing.T) {
	dir := t.TempDir()
	content := `{"id":1,"name":"A","age":30}
{"id":2,"name":"B","age":null}
{"id":"bad","name":"C","age":40}
`
	p := writeFile(t, dir, "d.jsonl", content)
	decl := csvDecl(p)
	decl.Format = "jsonl"
	l := New("")
	ds, _, err := l.Load(context.Background(), decl)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := issueCodes(ds, 2); len(got) != 1 || got[0] != model.ReasonNullValue {
		t.Fatalf("json null: want [null_value], got %v", got)
	}
	if got := issueCodes(ds, 3); len(got) != 1 || got[0] != model.ReasonTypeMismatch {
		t.Fatalf("json string id: want [type_mismatch], got %v", got)
	}
	// Actual field set is deterministic (sorted within a line; earlier lines
	// win across lines).
	want := map[string]bool{"id": true, "name": true, "age": true}
	if len(ds.Actual) != len(want) {
		t.Fatalf("actual fields: %v", ds.Actual)
	}
	for _, n := range ds.Actual {
		if !want[n] {
			t.Fatalf("unexpected actual field %q in %v", n, ds.Actual)
		}
	}
}

func TestRequiredMissingIsMissingField(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "d.jsonl", `{"id":1,"name":"A"}`+"\n"+`{"name":"B","age":2}`+"\n")
	decl := csvDecl(p)
	decl.Format = "jsonl"
	l := New("")
	ds, issues, err := l.Load(context.Background(), decl)
	if err != nil {
		t.Fatal(err)
	}
	// id absent from the second object: both a record-level missing_field and a
	// dataset-level shape symptom are possible; record-level must be present.
	found := false
	for _, fi := range ds.Records[1].FieldIssues {
		if fi.Field == "id" && fi.Code == model.ReasonMissingField {
			found = true
		}
	}
	if !found {
		t.Fatalf("want missing_field for required id, got %+v", ds.Records[1].FieldIssues)
	}
	// id absent from some but not all rows is not a whole-source shape issue.
	for _, is := range issues {
		if is.Code == model.ReasonSchemaFieldMissing {
			t.Fatalf("partial absence must not be schema_field_missing: %+v", is)
		}
	}
}

func TestShapeChangeExtraAndMissingColumns(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "d.csv", "id,name,age,extra\n1,A,30,x\n")
	l := New("")

	// Extra undeclared column -> source_shape_changed.
	_, issues, err := l.Load(context.Background(), csvDecl(p))
	if err != nil {
		t.Fatal(err)
	}
	if !hasShapeCode(issues, model.ReasonSourceShapeChange) {
		t.Fatalf("want source_shape_changed, got %+v", issues)
	}

	// Declared column entirely absent -> schema_field_missing.
	p2 := writeFile(t, dir, "d2.csv", "id,name\n1,A\n2,B\n")
	decl2 := csvDecl(p2)
	_, issues2, err := l.Load(context.Background(), decl2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasShapeCode(issues2, model.ReasonSchemaFieldMissing) {
		t.Fatalf("want schema_field_missing, got %+v", issues2)
	}
}

func TestRaggedCSVAndUnreadableFileAreErrors(t *testing.T) {
	dir := t.TempDir()
	l := New("")
	p := writeFile(t, dir, "d.csv", "id,name,age\n1,A\n")
	if _, _, err := l.Load(context.Background(), csvDecl(p)); err == nil {
		t.Fatal("ragged csv row must be a load error")
	}
	missing := csvDecl(filepath.Join(dir, "nope.csv"))
	if _, _, err := l.Load(context.Background(), missing); err == nil {
		t.Fatal("missing file must be a load error")
	}
}

func hasShapeCode(issues []ShapeIssue, code model.ReasonCode) bool {
	for _, is := range issues {
		if is.Code == code {
			return true
		}
	}
	return false
}

func TestInspectFingerprintsContentShapeAndMissing(t *testing.T) {
	dir := t.TempDir()
	l := New("")
	ctx := context.Background()
	p := writeFile(t, dir, "users.csv", "id,name\n1,A\n2,B\n")
	decl := csvDecl(p)

	fp1 := l.Inspect(ctx, decl)
	if fp1.ContentHash == "" || fp1.ContentHash == "missing" || fp1.ShapeHash == "" {
		t.Fatalf("invalid fingerprint: %+v", fp1)
	}
	if fp1.Records != 2 || fp1.LoadCode != model.ReasonNone {
		t.Fatalf("want 2 readable records, got %+v", fp1)
	}
	// Deterministic: same file inspects identically.
	if got := l.Inspect(ctx, decl); got != fp1 {
		t.Fatalf("fingerprint not stable: %+v vs %+v", got, fp1)
	}
	// Same shape, different rows: content hash changes, shape hash stays.
	p2 := writeFile(t, dir, "users2.csv", "id,name\n1,A\n3,C\n")
	fp2 := l.Inspect(ctx, csvDecl(p2))
	if fp2.ContentHash == fp1.ContentHash {
		t.Fatal("content hash must change when rows change")
	}
	if fp2.ShapeHash != fp1.ShapeHash {
		t.Fatal("shape hash must be identical for identical shapes")
	}
	// Different column set: shape hash changes.
	p3 := writeFile(t, dir, "users3.csv", "id,name,extra\n1,A,x\n2,B,y\n")
	fp3 := l.Inspect(ctx, csvDecl(p3))
	if fp3.ShapeHash == fp1.ShapeHash {
		t.Fatal("shape hash must change when columns change")
	}
	// Deleted file: missing content + normalized load code, no error.
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	fp4 := l.Inspect(ctx, decl)
	if fp4.ContentHash != "missing" || fp4.LoadCode != model.ReasonSourceLoadFailure {
		t.Fatalf("missing file fingerprint: %+v", fp4)
	}
}
