// Input snapshotting and replay validity for the engine.
//
// A committed batch stores an InputSnapshot describing every input behind its
// conclusions: the deterministic evaluation time, a hash of the full rule
// declaration set and, per dataset, hashes of the file content and of the
// parsed shape (format, actual fields, record count) plus a normalized load
// code when the source was missing or unparseable.
//
// On replay the current inputs are re-snapshotted and compared. Differences
// are returned as a DriftError whose Changes distinguish the data side from
// the rule side, so callers never mistake stale historical conclusions for
// current ones and can tell what changed at a glance.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// ChangeSide says which side of a replay differs.
type ChangeSide string

const (
	SideData ChangeSide = "data" // dataset source files / shapes
	SideRule ChangeSide = "rule" // rule declarations
	SideEval ChangeSide = "eval" // deterministic evaluation time
)

// ChangeKind refines a data-side difference.
type ChangeKind string

const (
	KindContentChanged   ChangeKind = "content_changed" // same shape, different rows
	KindShapeChanged     ChangeKind = "shape_changed"   // fields/columns/record-shape differ
	KindSourceMissing    ChangeKind = "source_missing"  // file deleted / unreadable now
	KindSourceRestored   ChangeKind = "source_restored" // was missing, now readable
	KindDeclarationAdded ChangeKind = "dataset_declared_now"
)

// Change is one explained difference between a stored input snapshot and the
// current inputs.
type Change struct {
	Side    ChangeSide `json:"side"`
	Kind    ChangeKind `json:"kind,omitempty"`
	Dataset string     `json:"dataset,omitempty"`
	Detail  string     `json:"detail"`
}

// DriftError means historical conclusions no longer describe the current
// inputs. Recognize with errors.As(err, **DriftError); Changes are ordered
// deterministically.
type DriftError struct {
	BatchID string
	Changes []Change
}

func (e *DriftError) Error() string {
	msg := fmt.Sprintf("batch %s: historical conclusions are no longer valid; refusing to return them as current", e.BatchID)
	for _, c := range e.Changes {
		msg += "\n  - " + string(c.Side) + ": " + c.Detail
	}
	return msg
}

// AsDrift extracts a *DriftError from err.
func AsDrift(err error) (*DriftError, bool) {
	var d *DriftError
	if errors.As(err, &d) {
		return d, true
	}
	return nil, false
}

// hashJSON marshals v deterministically and hashes it.
func hashJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return "hash-error" // all hashed types here are plain JSON-able declarations
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// rulesHash hashes the complete rule declaration set. Rule order in the config
// file must not matter, so rules are first sorted by id.
func rulesHash(rules []model.Rule) string {
	cp := append([]model.Rule(nil), rules...)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	return hashJSON(cp)
}

// snapshotInputs re-reads every declared input exactly as it is now.
func (e *Engine) snapshotInputs(ctx context.Context) *state.InputSnapshot {
	snap := &state.InputSnapshot{
		EvalTime: e.opts.EvalTime.UTC(),
		RuleHash: rulesHash(e.cfg.Rules),
		Datasets: make([]state.DatasetInput, 0, len(e.cfg.Datasets)),
	}
	for i := range e.cfg.Datasets {
		decl := e.cfg.Datasets[i]
		di := state.DatasetInput{
			Name:       decl.Name,
			Path:       decl.Path,
			Format:     decl.Format,
			SchemaHash: hashJSON(decl),
		}
		fp := e.loader.Inspect(ctx, decl)
		di.ContentHash = fp.ContentHash
		di.ShapeHash = fp.ShapeHash
		di.RecordCount = fp.Records
		if fp.LoadCode != model.ReasonNone {
			di.LoadError = string(fp.LoadCode)
		}
		snap.Datasets = append(snap.Datasets, di)
	}
	sort.Slice(snap.Datasets, func(i, j int) bool { return snap.Datasets[i].Name < snap.Datasets[j].Name })
	return snap
}

// compareSnapshots returns deterministic changes between a stored snapshot
// (recorded at commit time) and the current inputs.
func compareSnapshots(batchID string, stored, current *state.InputSnapshot) *DriftError {
	if stored == nil {
		return &DriftError{BatchID: batchID, Changes: []Change{{
			Side:   SideData,
			Detail: "no input snapshot was recorded with the historical conclusions; validity cannot be verified",
		}}}
	}
	var changes []Change
	if !stored.EvalTime.Equal(current.EvalTime) {
		changes = append(changes, Change{
			Side: SideEval,
			Detail: fmt.Sprintf("evaluation time differs: conclusions taken at %s, replay requested at %s",
				stored.EvalTime.Format(time.RFC3339), current.EvalTime.Format(time.RFC3339)),
		})
	}
	if stored.RuleHash != current.RuleHash {
		changes = append(changes, Change{
			Side:   SideRule,
			Detail: "rule declarations differ from the original run (rule set, bound fields, params or severity changed)",
		})
	}
	curByName := map[string]state.DatasetInput{}
	for _, d := range current.Datasets {
		curByName[d.Name] = d
	}
	storedByName := map[string]state.DatasetInput{}
	for _, d := range stored.Datasets {
		storedByName[d.Name] = d
	}
	names := make([]string, 0, len(stored.Datasets)+len(current.Datasets))
	for _, d := range stored.Datasets {
		names = append(names, d.Name)
	}
	for _, d := range current.Datasets {
		if _, ok := storedByName[d.Name]; !ok {
			names = append(names, d.Name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		old, hadOld := storedByName[name]
		now, hasNow := curByName[name]
		switch {
		case hadOld && !hasNow:
			changes = append(changes, Change{Side: SideRule, Dataset: name,
				Detail: fmt.Sprintf("dataset %q is no longer declared", name)})
		case !hadOld && hasNow:
			changes = append(changes, Change{Side: SideRule, Kind: KindDeclarationAdded, Dataset: name,
				Detail: fmt.Sprintf("dataset %q is declared now but was absent from the original run", name)})
		default:
			changes = append(changes, diffDataset(name, old, now)...)
		}
	}
	if len(changes) == 0 {
		return nil
	}
	return &DriftError{BatchID: batchID, Changes: changes}
}

func diffDataset(name string, old, now state.DatasetInput) []Change {
	switch {
	case now.ContentHash == "missing" && old.ContentHash != "missing":
		return []Change{{Side: SideData, Kind: KindSourceMissing, Dataset: name,
			Detail: fmt.Sprintf("dataset %q source file is missing or unreadable now (%s)", name, now.LoadError)}}
	case old.ContentHash == "missing" && now.ContentHash != "missing":
		return []Change{{Side: SideData, Kind: KindSourceRestored, Dataset: name,
			Detail: fmt.Sprintf("dataset %q source was unreadable in the original run but is readable now", name)}}
	case old.ContentHash == "missing" && now.ContentHash == "missing":
		if old.ShapeHash != now.ShapeHash {
			return []Change{{Side: SideData, Kind: KindShapeChanged, Dataset: name,
				Detail: fmt.Sprintf("dataset %q unreadable-source shape differs: %s vs %s", name, old.ShapeHash, now.ShapeHash)}}
		}
		return nil
	}
	if old.ShapeHash != now.ShapeHash {
		return []Change{{Side: SideData, Kind: KindShapeChanged, Dataset: name,
			Detail: fmt.Sprintf("dataset %q shape changed (fields/columns or record count): %d records at run time, %d records now",
				name, old.RecordCount, now.RecordCount)}}
	}
	if old.ContentHash != now.ContentHash {
		return []Change{{Side: SideData, Kind: KindContentChanged, Dataset: name,
			Detail: fmt.Sprintf("dataset %q file content changed while its shape is unchanged (%d records)", name, now.RecordCount)}}
	}
	return nil
}
