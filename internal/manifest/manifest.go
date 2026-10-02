// Package manifest captures the exact inputs a batch was evaluated from
// (dataset files, dataset declarations and rule declarations) so that, on a
// repeat run, stored historical conclusions can be proven still valid or
// demonstrably stale. A manifest hash is stable for identical inputs and
// Compare deterministically reports which side drifted (data vs rules).
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Side values for a Diff.
const (
	SideData  = "data"
	SideRules = "rules"
)

// DatasetFingerprint captures one declared dataset file as observed at run time.
type DatasetFingerprint struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Format     string    `json:"format"`
	SchemaHash string    `json:"schema_hash"`
	ContentSHA string    `json:"content_sha256"`
	Size       int64     `json:"size"`
	ModTime    time.Time `json:"mod_time"`
	Readable   bool      `json:"readable"`
	// ReadError is a normalized, single-line reason category when Readable is
	// false (missing / unreadable); raw OS error text is never stored.
	ReadError string `json:"read_error,omitempty"`
}

// Manifest is the full input declaration of one batch evaluation.
type Manifest struct {
	RulesHash string               `json:"rules_hash"`
	EvalTime  time.Time            `json:"eval_time"`
	Datasets  []DatasetFingerprint `json:"datasets"`
	Hash      string               `json:"hash"`
}

// Build computes a manifest from the current configuration and dataset files.
// Declaration drift (schema/path/format/rules/eval time) and data drift (file
// content or availability) are both reflected; data files are streamed so
// large inputs do not have to be fully buffered here.
func Build(cfg *config.Config, evalTime time.Time) Manifest {
	mf := Manifest{
		RulesHash: rulesHash(cfg.Rules),
		EvalTime:  evalTime.UTC(),
		Datasets:  make([]DatasetFingerprint, 0, len(cfg.Datasets)),
	}
	for _, decl := range cfg.Datasets {
		mf.Datasets = append(mf.Datasets, fingerprintDataset(decl))
	}
	sort.Slice(mf.Datasets, func(i, j int) bool { return mf.Datasets[i].Name < mf.Datasets[j].Name })
	mf.Hash = overallHash(mf)
	return mf
}

func fingerprintDataset(decl model.DatasetDecl) DatasetFingerprint {
	fp := DatasetFingerprint{
		Name:       decl.Name,
		Path:       decl.Path,
		Format:     decl.Format,
		SchemaHash: schemaHash(decl.Schema),
	}
	f, err := os.Open(decl.Path)
	if err != nil {
		fp.ReadError = classifyReadError(err)
		return fp
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		fp.ReadError = "stat failed"
		return fp
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		fp.ReadError = "read failed"
		return fp
	}
	fp.Readable = true
	fp.ContentSHA = hex.EncodeToString(h.Sum(nil))
	fp.Size = fi.Size()
	fp.ModTime = fi.ModTime().UTC()
	return fp
}

func classifyReadError(err error) string {
	if os.IsNotExist(err) {
		return "file missing"
	}
	if os.IsPermission(err) {
		return "permission denied"
	}
	return "file unreadable"
}

// stableRules is the JSON-normalized view of a rule declaration.
type stableRules struct {
	ID       string         `json:"id"`
	Dataset  string         `json:"dataset"`
	Kind     model.RuleKind `json:"kind"`
	Severity model.Severity `json:"severity"`
	Field    string         `json:"field,omitempty"`
	Fields   []string       `json:"fields,omitempty"`
	Params   map[string]any `json:"params,omitempty"`
}

func rulesHash(rules []model.Rule) string {
	ordered := make([]stableRules, 0, len(rules))
	for _, r := range rules {
		fields := append([]string(nil), r.Fields...)
		sort.Strings(fields)
		ordered = append(ordered, stableRules{
			ID:       r.ID,
			Dataset:  r.Dataset,
			Kind:     r.Kind,
			Severity: r.Severity,
			Field:    r.Field,
			Fields:   fields,
			Params:   r.Params,
		})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	return hashJSON(ordered)
}

func schemaHash(schema model.Schema) string {
	type field struct {
		Name     string          `json:"name"`
		Type     model.FieldType `json:"type"`
		Required bool            `json:"required"`
		Format   string          `json:"format,omitempty"`
	}
	fields := make([]field, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		fields = append(fields, field{Name: f.Name, Type: f.Type, Required: f.Required, Format: f.Format})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return hashJSON(fields)
}

func overallHash(mf Manifest) string {
	// Only identity-bearing fields feed the hash: mtime/size are informational
	// (rewriting an identical file must not invalidate historical conclusions).
	type ds struct {
		Name       string `json:"name"`
		Path       string `json:"path"`
		Format     string `json:"format"`
		SchemaHash string `json:"schema_hash"`
		ContentSHA string `json:"content_sha256"`
		Readable   bool   `json:"readable"`
		ReadError  string `json:"read_error,omitempty"`
	}
	type payload struct {
		RulesHash string    `json:"rules_hash"`
		EvalTime  time.Time `json:"eval_time"`
		Datasets  []ds      `json:"datasets"`
	}
	p := payload{RulesHash: mf.RulesHash, EvalTime: mf.EvalTime}
	for _, d := range mf.Datasets {
		p.Datasets = append(p.Datasets, ds{
			Name: d.Name, Path: d.Path, Format: d.Format, SchemaHash: d.SchemaHash,
			ContentSHA: d.ContentSHA, Readable: d.Readable, ReadError: d.ReadError,
		})
	}
	return hashJSON(p)
}

// hashJSON hashes the canonical JSON encoding of v.
func hashJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Diff is one concrete difference between a stored and a current manifest.
type Diff struct {
	// Side is SideData (dataset file content/availability) or SideRules
	// (declarations / evaluation time).
	Side   string
	Detail string
}

// Compare reports deterministic per-dataset/per-rule differences. An empty
// result means the historical conclusions still apply.
//
// Classification:
//
//   - file missing/unreadable now or before, or changed content hash -> data
//   - declared path/format/schema change, rule declaration change or a
//     different evaluation time -> rules
func Compare(stored, current Manifest) []Diff {
	var diffs []Diff
	if stored.RulesHash != current.RulesHash {
		diffs = append(diffs, Diff{Side: SideRules, Detail: "rule declarations (ids/kinds/fields/params/severity) differ from the original run"})
	}
	if !stored.EvalTime.Equal(current.EvalTime) {
		diffs = append(diffs, Diff{Side: SideRules,
			Detail: "evaluation time differs: original=" + stored.EvalTime.Format(time.RFC3339) +
				" current=" + current.EvalTime.Format(time.RFC3339)})
	}
	storedDS := map[string]DatasetFingerprint{}
	for _, d := range stored.Datasets {
		storedDS[d.Name] = d
	}
	currentDS := map[string]DatasetFingerprint{}
	for _, d := range current.Datasets {
		currentDS[d.Name] = d
	}
	names := make([]string, 0, len(storedDS)+len(currentDS))
	seen := map[string]bool{}
	for n := range storedDS {
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	for n := range currentDS {
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		old, hasOld := storedDS[name]
		cur, hasCur := currentDS[name]
		switch {
		case hasOld && !hasCur:
			diffs = append(diffs, Diff{Side: SideRules, Detail: "dataset " + name + " was removed from the declaration"})
			continue
		case !hasOld && hasCur:
			diffs = append(diffs, Diff{Side: SideRules, Detail: "dataset " + name + " was added to the declaration"})
			continue
		}
		if old.Path != cur.Path {
			diffs = append(diffs, Diff{Side: SideRules, Detail: "dataset " + name + " source path changed: original=" + old.Path + " current=" + cur.Path})
		}
		if old.Format != cur.Format {
			diffs = append(diffs, Diff{Side: SideRules, Detail: "dataset " + name + " format changed: original=" + old.Format + " current=" + cur.Format})
		}
		if old.SchemaHash != cur.SchemaHash {
			diffs = append(diffs, Diff{Side: SideRules, Detail: "dataset " + name + " schema declaration (fields/types/required) changed"})
		}
		switch {
		case old.Readable && !cur.Readable:
			diffs = append(diffs, Diff{Side: SideData, Detail: "dataset " + name + " source file is no longer readable: " + cur.ReadError})
		case !old.Readable && cur.Readable:
			diffs = append(diffs, Diff{Side: SideData, Detail: "dataset " + name + " source file became readable again (was: " + old.ReadError + ")"})
		case !old.Readable && !cur.Readable && old.ReadError != cur.ReadError:
			diffs = append(diffs, Diff{Side: SideData, Detail: "dataset " + name + " source file unreadability changed: original=" + old.ReadError + " current=" + cur.ReadError})
		case old.Readable && cur.Readable && old.ContentSHA != cur.ContentSHA:
			diffs = append(diffs, Diff{Side: SideData, Detail: "dataset " + name + " file content changed (size " + itoa(old.Size) + "->" + itoa(cur.Size) + ")"})
		}
	}
	return diffs
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
