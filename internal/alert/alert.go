// Package alert normalizes and deduplicates rule failure notifications.
//
// Alerts are emitted at most once per (batch, rule). Delivery is tracked in
// two complementary places so that a process interrupted at *any* point —
// before a send, mid-send, or after a successful send but before the journal
// write — never causes a duplicate on restart:
//
//   - an append-only JSONL journal records every successfully delivered
//     (batch, rule) pair right after the send returns;
//   - sinks implementing DeliverySource (notably FileSink) can enumerate what
//     they have durably delivered, so a delivery whose journal write was lost
//     to a crash is still recognized as delivered and never re-sent.
//
// Keys not yet recorded anywhere are retried on every invocation until they
// succeed (backlog补投). Every Dispatch returns an AlertReport partitioning
// the expected alerts into already-delivered / sent-this-run / still-pending.
package alert

import (
	"bufio"
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

// ReasonCode mirrors model.ReasonCode in JSON output.
type ReasonCode = model.ReasonCode

// Sink receives alerts.
type Sink interface {
	Send(ctx context.Context, a Alert) error
}

// DeliverySource is an optional Sink capability: the sink can enumerate the
// (batch, rule) pairs it has durably delivered. When the sink itself is the
// durable record (e.g. FileSink's append-only file), the Alerter treats it as
// ground truth in addition to its own journal, closing the crash window
// between "send succeeded" and "journal write persisted".
type DeliverySource interface {
	// Delivered returns the set of delivery keys (see deliveryKey) the sink
	// has durably recorded.
	Delivered(ctx context.Context) (map[string]bool, error)
}

// deliveryKey identifies one (batch, rule) notification.
func deliveryKey(batchID, ruleID string) string { return batchID + "\x00" + ruleID }

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

// Send implements Sink, appending one JSON line per alert. The line is
// written with a single write call and fsynced before Send returns, so a
// delivered alert survives a process kill at any later point and is visible
// to Delivered on the next run.
func (s *FileSink) Send(ctx context.Context, a Alert) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// If a previous crash left a torn trailing line (no newline), terminate it
	// first so the new alert starts on its own clean line; the torn remnant
	// stays inert and is ignored by Delivered.
	if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, fi.Size()-1); err == nil && last[0] != '\n' {
			if _, err := f.Write([]byte{'\n'}); err != nil {
				return fmt.Errorf("terminate torn alert line: %w", err)
			}
		}
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write alert: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync alert: %w", err)
	}
	return nil
}

// Delivered implements DeliverySource: the sink file itself is the durable
// delivery record. Every well-formed line yields its (batch, rule) key. A
// malformed trailing line (only possible if a crash tore a write) is treated
// as not delivered: the alert will be re-sent and re-appended as a complete
// line, so nothing is lost and no valid line is ever sent twice.
func (s *FileSink) Delivered(ctx context.Context) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var a Alert
		if err := json.Unmarshal(line, &a); err != nil {
			continue // torn trailing write: not durably delivered
		}
		out[deliveryKey(a.BatchID, a.RuleID)] = true
	}
	return out, sc.Err()
}

// journalEntry records one dispatched (batch, rule) pair.
type journalEntry struct {
	BatchID string `json:"batch_id"`
	RuleID  string `json:"rule_id"`
}

// Alerter dispatches alerts exactly once per (batch, rule).
type Alerter struct {
	sink    Sink
	journal string
	lock    string
	mu      sync.Mutex
}

// NewAlerter creates an Alerter backed by sink, using journalPath to remember
// already dispatched (batch, rule) pairs.
func NewAlerter(sink Sink, journalPath string) (*Alerter, error) {
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o755); err != nil {
		return nil, fmt.Errorf("init alert journal dir: %w", err)
	}
	f, err := os.OpenFile(journalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	return &Alerter{sink: sink, journal: journalPath, lock: journalPath + ".lock"}, nil
}

func (a *Alerter) loadDispatched() (map[string]bool, error) {
	seen := map[string]bool{}
	f, err := os.Open(a.journal)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return seen, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var e journalEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("corrupt alert journal: %w", err)
		}
		seen[deliveryKey(e.BatchID, e.RuleID)] = true
	}
	return seen, sc.Err()
}

// Dispatch sends alerts for failed/errored results at or above the minimum
// severity. It is idempotent: a (batch, rule) pair already present in the
// journal or in the sink's own delivery record is never sent again. Results
// are processed in order so the journal and output are deterministic.
//
// The whole load-check-send-record cycle runs under an in-process mutex and
// a cross-process flock (the journal's sibling .lock file), so two processes
// dispatching the same batch concurrently serialize: the second one observes
// the first one's deliveries and does not duplicate them.
//
// The returned report distinguishes alerts already delivered in earlier
// invocations (retried batches/crash restarts), alerts delivered in this
// invocation (including backlog retries) and alerts still pending after it, so
// callers can tell a retry "补齐了几条、还剩几条".
func (a *Alerter) Dispatch(ctx context.Context, batchID string, results []model.RuleResult, minSeverity model.Severity) (model.AlertReport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	lf, err := os.OpenFile(a.lock, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return model.AlertReport{}, fmt.Errorf("open alert dispatch lock: %w", err)
	}
	defer lf.Close()
	if err := lockFile(lf); err != nil {
		return model.AlertReport{}, fmt.Errorf("acquire alert dispatch lock: %w", err)
	}
	defer unlockFile(lf)

	dispatched, err := a.loadDispatched()
	if err != nil {
		return model.AlertReport{}, err
	}
	// Merge the sink's own durable delivery record (when available). This is
	// what makes a crash between "send succeeded" and "journal persisted"
	// harmless: the delivered alert is still known and never re-sent.
	if src, ok := a.sink.(DeliverySource); ok {
		delivered, err := src.Delivered(ctx)
		if err != nil {
			return model.AlertReport{}, fmt.Errorf("read sink delivery record: %w", err)
		}
		for k := range delivered {
			dispatched[k] = true
		}
	}
	jf, err := os.OpenFile(a.journal, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return model.AlertReport{}, err
	}
	defer jf.Close()

	ordered := append([]model.RuleResult(nil), results...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].RuleID < ordered[j].RuleID })

	report := model.AlertReport{}
	var firstErr error
	for _, res := range ordered {
		if res.Status == model.StatusPassed {
			continue
		}
		if !severityAtLeast(res.Severity, minSeverity) {
			continue
		}
		report.Expected++
		key := deliveryKey(batchID, res.RuleID)
		if dispatched[key] {
			report.AlreadyDelivered++
			continue // exactly-once across replays/crashes
		}
		if err := ctx.Err(); err != nil {
			report.Pending = report.Expected - report.AlreadyDelivered - report.SentThisRun
			return report, err
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
		if err := a.sink.Send(ctx, al); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Record delivery before reporting success; on crash the next run
		// resumes without re-notifying this rule. Even if this journal write
		// is lost, a DeliverySource sink still remembers the delivery.
		raw, _ := json.Marshal(journalEntry{BatchID: batchID, RuleID: res.RuleID})
		if _, err := jf.Write(append(raw, '\n')); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := jf.Sync(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		dispatched[key] = true
		report.SentThisRun++
	}
	report.Pending = report.Expected - report.AlreadyDelivered - report.SentThisRun
	return report, firstErr
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
