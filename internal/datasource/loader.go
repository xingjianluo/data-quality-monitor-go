// Package datasource loads local CSV/JSONL files into canonical datasets and
// detects source shape changes against the declared schema.
//
// Failure reasons are kept distinguishable:
//   - file/parse level problems return an error (source_load_failure upstream);
//   - structural shape drift (declared columns absent, extra columns, ragged CSV
//     rows) is returned as ShapeIssue;
//   - per-record missing values and type mismatches are attached to the record
//     as FieldIssue and later surfaced by rules as missing_field/type_mismatch.
package datasource

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// ShapeIssue describes a structural difference between declared and actual shape.
type ShapeIssue struct {
	Code   model.ReasonCode
	Detail string
}

// Loader loads datasets from local files.
type Loader struct {
	baseDir string
}

// New creates a Loader resolving relative paths against baseDir.
func New(baseDir string) *Loader { return &Loader{baseDir: baseDir} }

// Load loads one dataset and reports shape issues. A non-nil error means the
// source could not be read/parsed at all; shape issues mean the source loaded
// but its shape differs from the declaration.
func (l *Loader) Load(ctx context.Context, decl model.DatasetDecl) (*model.Dataset, []ShapeIssue, error) {
	path := decl.Path
	if !filepath.IsAbs(path) {
		path = filepath.Join(l.baseDir, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", decl.Name, err)
	}
	var ds *model.Dataset
	switch decl.Format {
	case "csv":
		ds, err = l.loadCSV(ctx, decl, data)
	case "jsonl":
		ds, err = l.loadJSONL(ctx, decl, data)
	default:
		return nil, nil, fmt.Errorf("dataset %s: unsupported format %q", decl.Name, decl.Format)
	}
	if err != nil {
		return nil, nil, err
	}
	issues := diffShape(decl, ds)
	return ds, issues, nil
}

func diffShape(decl model.DatasetDecl, ds *model.Dataset) []ShapeIssue {
	var issues []ShapeIssue
	declared := make(map[string]bool, len(decl.Schema.Fields))
	order := make(map[string]int, len(decl.Schema.Fields))
	for i, f := range decl.Schema.Fields {
		declared[f.Name] = true
		order[f.Name] = i
	}
	actual := make(map[string]bool, len(ds.Actual))
	for _, name := range ds.Actual {
		actual[name] = true
	}
	var missing []string
	for _, f := range decl.Schema.Fields {
		if !actual[f.Name] {
			missing = append(missing, f.Name)
		}
	}
	sort.Strings(missing)
	for _, name := range missing {
		issues = append(issues, ShapeIssue{
			Code:   model.ReasonSchemaFieldMissing,
			Detail: fmt.Sprintf("declared field %q is absent from the source", name),
		})
	}
	var extra []string
	for _, name := range ds.Actual {
		if !declared[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	for _, name := range extra {
		issues = append(issues, ShapeIssue{
			Code:   model.ReasonSourceShapeChange,
			Detail: fmt.Sprintf("source contains undeclared field %q", name),
		})
	}
	return issues
}

func (l *Loader) loadCSV(ctx context.Context, decl model.DatasetDecl, data []byte) (*model.Dataset, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1 // validate column counts ourselves for precise errors
	header, err := reader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("dataset %s: csv file is empty", decl.Name)
	}
	if err != nil {
		return nil, fmt.Errorf("dataset %s: read csv header: %w", decl.Name, err)
	}
	ds := &model.Dataset{Name: decl.Name, Schema: decl.Schema, Actual: append([]string(nil), header...)}
	line := 1
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		line++
		if err != nil {
			return nil, fmt.Errorf("dataset %s line %d: parse csv: %w", decl.Name, line, err)
		}
		if len(row) != len(header) {
			return nil, fmt.Errorf("dataset %s line %d: ragged row: got %d columns, header has %d",
				decl.Name, line, len(row), len(header))
		}
		rec := model.Record{Line: line, Values: map[string]any{}, Present: map[string]bool{}}
		for i, col := range header {
			rec.Present[col] = true
			// An empty CSV cell is an explicit null, not an empty string: an
			// empty integer must never look like a type-mismatched string.
			if row[i] == "" {
				rec.Values[col] = nil
				continue
			}
			rec.Values[col] = col2val(decl, col, row[i], line)
		}
		validateRecord(decl, &rec)
		ds.Records = append(ds.Records, rec)
	}
	return ds, nil
}

func (l *Loader) loadJSONL(ctx context.Context, decl model.DatasetDecl, data []byte) (*model.Dataset, error) {
	ds := &model.Dataset{Name: decl.Name, Schema: decl.Schema}
	seen := map[string]int{}
	lines := strings.Split(string(data), "\n")
	for idx, rawLine := range lines {
		line := idx + 1
		trimmed := strings.TrimSpace(rawLine)
		if trimmed == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var obj map[string]json.RawMessage
		dec := json.NewDecoder(strings.NewReader(trimmed))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("dataset %s line %d: parse json: %w", decl.Name, line, err)
		}
		if len(obj) == 0 {
			return nil, fmt.Errorf("dataset %s line %d: expected a JSON object", decl.Name, line)
		}
		rec := model.Record{Line: line, Values: map[string]any{}, Present: map[string]bool{}}
		for k, raw := range obj {
			rec.Present[k] = true
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				rec.Values[k] = nil
				continue
			}
			v, err := decodeValue(decl, k, raw, line)
			if err != nil {
				return nil, err
			}
			rec.Values[k] = v
		}
		for k := range obj {
			if _, ok := seen[k]; !ok {
				seen[k] = line
			}
		}
		validateRecord(decl, &rec)
		ds.Records = append(ds.Records, rec)
	}
	if len(ds.Records) == 0 {
		return nil, fmt.Errorf("dataset %s: jsonl file has no records", decl.Name)
	}
	// Deterministic field order: first-seen line wins, ties broken by name.
	type first struct {
		line int
		name string
	}
	var ordered []first
	for name, ln := range seen {
		ordered = append(ordered, first{ln, name})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].line != ordered[j].line {
			return ordered[i].line < ordered[j].line
		}
		return ordered[i].name < ordered[j].name
	})
	for _, f := range ordered {
		ds.Actual = append(ds.Actual, f.name)
	}
	return ds, nil
}

func fieldDecl(decl model.DatasetDecl, name string) (model.FieldDecl, bool) {
	for _, f := range decl.Schema.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return model.FieldDecl{}, false
}

// validateRecord records a distinguishable FieldIssue for every structural
// problem so a missing key, null value or wrong type can never pass silently:
//
//   - required field absent from the source -> missing_field
//   - optional field absent                    -> explicit null (no issue)
//   - declared field present but null          -> null_value
//   - value not convertible to declared type   -> type_mismatch
func validateRecord(decl model.DatasetDecl, rec *model.Record) {
	for _, fd := range decl.Schema.Fields {
		present := rec.Present[fd.Name]
		if !present {
			if fd.Required {
				rec.FieldIssues = append(rec.FieldIssues, model.FieldIssue{
					Field:  fd.Name,
					Code:   model.ReasonMissingField,
					Detail: fmt.Sprintf("line %d: required field %q is absent", rec.Line, fd.Name),
				})
			}
			continue
		}
		v := rec.Values[fd.Name]
		if v == nil {
			rec.FieldIssues = append(rec.FieldIssues, model.FieldIssue{
				Field:  fd.Name,
				Code:   model.ReasonNullValue,
				Detail: fmt.Sprintf("line %d: field %q is null", rec.Line, fd.Name),
			})
			continue
		}
		if !typeMatches(fd, v) {
			rec.FieldIssues = append(rec.FieldIssues, model.FieldIssue{
				Field: fd.Name,
				Code:  model.ReasonTypeMismatch,
				Detail: fmt.Sprintf("line %d: field %q value %v is not %s",
					rec.Line, fd.Name, v, fd.Type),
			})
		}
	}
}

func typeMatches(fd model.FieldDecl, v any) bool {
	switch fd.Type {
	case model.TypeString:
		_, ok := v.(string)
		return ok
	case model.TypeInt:
		_, ok := v.(int64)
		return ok
	case model.TypeFloat:
		_, ok := v.(float64)
		return ok
	case model.TypeBool:
		_, ok := v.(bool)
		return ok
	case model.TypeTime:
		_, ok := v.(time.Time)
		return ok
	}
	return false
}

// col2val converts a raw CSV cell into the declared type. A mismatched value is
// kept verbatim (as string) so rule evaluation can report type_mismatch with
// the original value rather than silently coercing it.
func col2val(decl model.DatasetDecl, name, raw string, line int) any {
	fd, ok := fieldDecl(decl, name)
	if !ok {
		return raw
	}
	switch fd.Type {
	case model.TypeInt:
		if v, ok := model.AsInt(raw); ok {
			return v
		}
	case model.TypeFloat:
		if v, ok := model.AsFloat(raw); ok {
			return v
		}
	case model.TypeBool:
		if v, ok := model.AsBool(raw); ok {
			return v
		}
	case model.TypeTime:
		if v, ok := model.AsTime(raw, fd.Format); ok {
			return v
		}
	}
	return raw
}

func decodeValue(decl model.DatasetDecl, name string, raw json.RawMessage, line int) (any, error) {
	fd, isDeclared := fieldDecl(decl, name)
	if !isDeclared {
		var v any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			return nil, fmt.Errorf("dataset %s line %d field %q: %w", decl.Name, line, name, err)
		}
		return v, nil
	}
	switch fd.Type {
	case model.TypeString:
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s, nil
		}
		return keepRaw(raw)
	case model.TypeInt:
		var n json.Number
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&n); err == nil {
			if v, ok := model.AsInt(n); ok {
				return v, nil
			}
			return n.String(), nil
		}
		return keepRaw(raw)
	case model.TypeFloat:
		var n json.Number
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&n); err == nil {
			if v, ok := model.AsFloat(n); ok {
				return v, nil
			}
			return n.String(), nil
		}
		return keepRaw(raw)
	case model.TypeBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err == nil {
			return b, nil
		}
		return keepRaw(raw)
	case model.TypeTime:
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if t, ok := model.AsTime(s, fd.Format); ok {
				return t, nil
			}
			return s, nil
		}
		return keepRaw(raw)
	}
	return keepRaw(raw)
}

// keepRaw keeps the raw JSON token as a string for type_mismatch reporting,
// never silently coercing structural mismatches.
func keepRaw(raw json.RawMessage) (any, error) {
	return string(bytes.TrimSpace(raw)), nil
}
