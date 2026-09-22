package rule

import (
	"fmt"
	"strings"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// fieldStatus distinguishes the four possible states of one record field so
// rules never treat missing/null/mistyped values as silently valid.
type fieldStatus int

const (
	fieldOK       fieldStatus = iota // present, non-null, matching declared type
	fieldMissing                     // key absent (only expected for optional fields)
	fieldNull                        // present but JSON null
	fieldMistyped                    // value cannot be converted to declared type
)

// fieldView is the resolved view of one field of one record.
type fieldView struct {
	status fieldStatus
	decl   model.FieldDecl
	raw    any
	text   string // raw textual representation for evidence
	issue  *model.FieldIssue
}

// resolveField resolves a declared field for a record.
func resolveField(ds *model.Dataset, rec model.Record, name string) fieldView {
	fd, ok := findDecl(ds, name)
	if !ok {
		return fieldView{status: fieldMissing, decl: model.FieldDecl{Name: name}}
	}
	for i := range rec.FieldIssues {
		fi := &rec.FieldIssues[i]
		if fi.Field == name {
			st := fieldMistyped
			switch fi.Code {
			case model.ReasonMissingField:
				st = fieldMissing
			case model.ReasonNullValue:
				st = fieldNull
			case model.ReasonTypeMismatch:
				st = fieldMistyped
			}
			return fieldView{status: st, decl: fd, issue: fi}
		}
	}
	// No load-time issue. Derive the state from presence/value so synthetic
	// records (and the loader) agree: an optional field whose key is absent is
	// treated as an explicit null (SQL-style three-valued logic), not as a
	// missing-field failure.
	if !rec.Present[name] {
		return fieldView{
			status: fieldNull,
			decl:   fd,
			issue: &model.FieldIssue{
				Field:  name,
				Code:   model.ReasonNullValue,
				Detail: fmt.Sprintf("line %d: optional field %q is absent (null)", rec.Line, name),
			},
		}
	}
	v := rec.Values[name]
	if v == nil {
		return fieldView{
			status: fieldNull,
			decl:   fd,
			issue: &model.FieldIssue{
				Field:  name,
				Code:   model.ReasonNullValue,
				Detail: fmt.Sprintf("line %d: field %q is null", rec.Line, name),
			},
		}
	}
	return fieldView{status: fieldOK, decl: fd, raw: v, text: render(v)}
}

func findDecl(ds *model.Dataset, name string) (model.FieldDecl, bool) {
	for _, f := range ds.Schema.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return model.FieldDecl{}, false
}

func render(v any) string {
	if s, ok := model.AsString(v); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// structuralFinding converts a load-time field issue into a normalized Finding.
func structuralFinding(fi model.FieldIssue) model.Finding {
	return model.Finding{
		Code:  fi.Code,
		Field: fi.Field,
		Evidence: []model.Evidence{{
			Detail: fi.Detail,
		}},
	}
}

// dedupeEvidence sorts and de-duplicates evidence lines by (line, field,
// detail) to keep results deterministic.
func dedupeEvidence(in []model.Evidence) []model.Evidence {
	seen := map[string]bool{}
	out := in[:0]
	for _, e := range in {
		key := fmt.Sprintf("%d|%s|%s", e.Line, e.Field, e.Detail)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, e)
	}
	return out
}

// targetFields returns the ordered list of fields a rule applies to.
func targetFields(r model.Rule) []string {
	var out []string
	if r.Field != "" {
		out = append(out, r.Field)
	}
	out = append(out, r.Fields...)
	return out
}

func joinFields(fs []string) string { return strings.Join(fs, ",") }
