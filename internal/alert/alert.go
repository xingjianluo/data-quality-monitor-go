// Package alert normalizes and deduplicates rule failure notifications.
//
// Alerts are emitted exactly once per (batch, rule). The append-only JSONL
// journal records two kinds of durable facts per batch:
//
//   - an "attempt" entry written before delivery starts (survives pre/during
//     delivery crashes, letting a later run classify backlog deliveries as
//     retried);
//   - a "delivered" entry written only after the sink accepts the alert.
//
// Repeating a batch, an out-of-order late arrival or a post-crash re-run never
// sends duplicate alerts. Delivery failures leave the pair undelivered; the
// next run of the same batch (even a replay) sends the missing backlog and
// reports how many were retried vs. still pending. Sinks that implement
// DeliveryChecker additionally guard against duplicates when a crash happens
// between sink acceptance and journal durability.
package alert

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Alert is one normalized notification.
type Alert struct {
	BatchID  string           `json:"batch_id"`
	RuleID   string           `json:"rule_id"`
	Dataset  string           `json:"dataset"`
	Kind     model.RuleKind   `json:"kind"`
	Severity model.Severity   `json:"severity"`
	Status   model.RuleStatus `json:"status"`
	Code     ReasonCode       `json:"code"`
	Summary  string           `json:"summary"`
}

// Key is the exactly-once identity of an alert.
func (a Alert) Key() string { return a.BatchID + "\x00" + a.RuleID }

// ReasonCode mirrors model.ReasonCode in JSON output.
type ReasonCode = model.ReasonCode

// Sink receives alerts.
type Sink interface {
	Send(ctx context.Context, a Alert) error
}

// DeliveryChecker is implemented by sinks that can report whether an alert was
// already delivered. It closes the crash window between a successful Send and
// the delivered journal entry becoming durable.
type DeliveryChecker interface {
	Delivered(ctx context.Context, a Alert) (bool, error)
}

// FileSink appends one JSON object per alert line to a local file.
type FileSink struct {
	path string
	mu   sync.Mutex
}

// NewFileSink creates (or opens) an append-only FileSink at path.
func NewFileSink(path string) (*FileSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("init alert dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open alert file: %w", err)
	}
	_ = f.Close()
	return &FileSink{path: path}, nil
}

// repairTail drops a trailing partial line left by a crash mid-write. All
// complete lines before it remain intact.
func (s *FileSink) repairTail() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return nil
	}
	cut := bytes.LastIndexByte(data, '\n')
	keep := 0
	if cut >= 0 {
		keep = cut + 1
	}
	// Rewrite only when the tail is actually a corrupt JSON fragment.
	tail := data[keep:]
	var probe any
	if json.Unmarshal(tail, &probe) == nil {
		// A complete JSON object without trailing newline: normalize by adding
		// the newline rather than dropping the record.
		return os.WriteFile(s.path, append(data, '\n'), 0o644)
	}
	return os.WriteFile(s.path, data[:keep], 0o644)
}

// Send implements Sink, appending one durable JSON line per alert. The write
// is followed by Sync so a crash cannot lose an acknowledged alert; a partial
// trailing line from a crash during write is repaired on the next operation.
func (s *FileSink) Send(ctx context.Context, a Alert) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.repairTail(); err != nil {
		return err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(raw); err != nil {
		return fmt.Errorf("write alert: %w", err)
	}
	return f.Sync()
}

// Delivered implements DeliveryChecker by scanning the alert file for the key.
func (s *FileSink) Delivered(ctx context.Context, a Alert) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.repairTail(); err != nil {
		return false, err
	}
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var got Alert
		if err := json.Unmarshal(line, &got); err != nil {
			continue // skip a repaired/foreign line rather than failing delivery
		}
		if got.Key() == a.Key() {
			return true, nil
		}
	}
	return false, sc.Err()
}

// journalEntry records one durable fact in the alert journal. Kind is
// "attempt" (delivery of this batch started/was retried) or "delivered".
type journalEntry struct {
	BatchID string `json:"batch_id"`
	RuleID  string `json:"rule_id"`
	Kind    string `json:"kind,omitempty"`
}

const (
	kindAttempt   = "attempt"
	kindDelivered = "delivered"
)

// Outcome reports delivery accounting for one dispatch.
type Outcome struct {
	// Due is the total number of alerts the batch needs delivered.
	Due int
	// Sent is how many alerts were actually delivered during this dispatch
	// (retried backlog included).
	Sent int
	// Retried is how many of Sent had been attempted (but not delivered) by an
	// earlier run, i.e. backlog filled this time.
	Retried int
	// Pending is how many required alerts are still not delivered afterwards.
	Pending int
}

// Alerter dispatches alerts exactly once per (batch, rule).
type Alerter struct {
	sink    Sink
	journal string
	mu      sync.Mutex
}

// NewAlerter creates an Alerter backed by sink, using journalPath to remember
// delivery attempts and successes per (batch, rule).
func NewAlerter(sink Sink, journalPath string) (*Alerter, error) {
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o755); err != nil {
		return nil, fmt.Errorf("init alert journal dir: %w", err)
	}
	f, err := os.OpenFile(journalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	return &Alerter{sink: sink, journal: journalPath}, nil
}

type journalState struct {
	delivered map[string]bool
	attempted map[string]bool
}

func (a *Alerter) loadJournal() (journalState, error) {
	st := journalState{delivered: map[string]bool{}, attempted: map[string]bool{}}
	f, err := os.Open(a.journal)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return st, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e journalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return st, fmt.Errorf("corrupt alert journal: %w", err)
		}
		// Entries written by older versions have no Kind: treat as delivered.
		key := e.BatchID + "\x00" + e.RuleID
		switch e.Kind {
		case kindAttempt:
			st.attempted[key] = true
		case kindDelivered, "":
			st.delivered[key] = true
		}
	}
	return st, sc.Err()
}

func appendJournal(jf *os.File, e journalEntry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := jf.Write(append(raw, '\n')); err != nil {
		return err
	}
	return jf.Sync()
}

// Dispatch sends alerts for failed/errored results at or above the minimum
// severity. It is idempotent: a (batch, rule) pair already recorded as
// delivered is never sent again. Results are processed in order so the journal
// and output are deterministic. The returned Outcome distinguishes freshly
// sent, retried backlog and still-pending alerts; the error is non-nil when at
// least one delivery failed.
func (a *Alerter) Dispatch(ctx context.Context, batchID string, results []model.RuleResult, minSeverity model.Severity) (Outcome, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	st, err := a.loadJournal()
	if err != nil {
		return Outcome{}, err
	}
	jf, err := os.OpenFile(a.journal, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return Outcome{}, err
	}
	defer jf.Close()

	ordered := append([]model.RuleResult(nil), results...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].RuleID < ordered[j].RuleID })

	// Collect the alert set this batch requires.
	var due []model.RuleResult
	for _, res := range ordered {
		if res.Status == model.StatusPassed {
			continue
		}
		if !severityAtLeast(res.Severity, minSeverity) {
			continue
		}
		due = append(due, res)
	}
	out := Outcome{Due: len(due)}

	var firstErr error
	for _, res := range due {
		if err := ctx.Err(); err != nil {
			out.Pending = out.Due - countDeliveredForBatch(st, batchID, due)
			return out, err
		}
		key := batchID + "\x00" + res.RuleID
		if st.delivered[key] {
			continue // exactly-once across replays/crashes
		}
		al := Alert{
			BatchID:  batchID,
			RuleID:   res.RuleID,
			Dataset:  res.Dataset,
			Kind:     res.Kind,
			Severity: res.Severity,
			Status:   res.Status,
			Code:     primaryCode(res),
			Summary:  summarize(res),
		}
		// Sink-side guard for the crash window between Send and journal Sync.
		if checker, ok := a.sink.(DeliveryChecker); ok {
			got, checkErr := checker.Delivered(ctx, al)
			if checkErr != nil {
				if firstErr == nil {
					firstErr = checkErr
				}
				continue
			}
			if got {
				st.delivered[key] = true
				if err := appendJournal(jf, journalEntry{BatchID: batchID, RuleID: res.RuleID, Kind: kindDelivered}); err != nil && firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		// A durable attempt marker makes this delivery recognizable as backlog
		// on the next run even if we crash before Send returns.
		wasAttempted := st.attempted[key]
		if !wasAttempted {
			if err := appendJournal(jf, journalEntry{BatchID: batchID, RuleID: res.RuleID, Kind: kindAttempt}); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			st.attempted[key] = true
		}
		if err := a.sink.Send(ctx, al); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("deliver alert for rule %s: %w", res.RuleID, err)
			}
			continue
		}
		if err := appendJournal(jf, journalEntry{BatchID: batchID, RuleID: res.RuleID, Kind: kindDelivered}); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		st.delivered[key] = true
		out.Sent++
		if wasAttempted {
			out.Retried++
		}
	}
	out.Pending = out.Due - countDeliveredForBatch(st, batchID, due)
	return out, firstErr
}

func countDeliveredForBatch(st journalState, batchID string, due []model.RuleResult) int {
	n := 0
	for _, res := range due {
		if st.delivered[batchID+"\x00"+res.RuleID] {
			n++
		}
	}
	return n
}

func severityAtLeast(got, min model.Severity) bool {
	if min == model.SeverityWarning {
		return true
	}
	return got == model.SeverityError
}

func primaryCode(res model.RuleResult) ReasonCode {
	if res.ErrorCode != model.ReasonNone {
		return res.ErrorCode
	}
	if len(res.Findings) > 0 {
		return res.Findings[0].Code
	}
	return model.ReasonNone
}

func summarize(res model.RuleResult) string {
	if res.Status == model.StatusErrored {
		return fmt.Sprintf("rule %s on %s errored (%s): %s", res.RuleID, res.Dataset, res.ErrorCode, res.ErrorMsg)
	}
	count := 0
	for _, f := range res.Findings {
		count += len(f.Evidence)
	}
	if len(res.Findings) > 0 {
		return fmt.Sprintf("rule %s on %s failed: %s (%d evidence)",
			res.RuleID, res.Dataset, res.Findings[0].Code, count)
	}
	return fmt.Sprintf("rule %s on %s failed", res.RuleID, res.Dataset)
}
