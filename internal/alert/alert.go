// Package alert normalizes and deduplicates rule failure notifications.
//
// Alerts are emitted at most once per (batch, rule). The delivery protocol is
// a two-phase, crash-safe, append-only JSONL journal:
//
//  1. before a send, a "pending" entry is fsynced to the journal (the
//     delivery intent and a stable DeliveryID survive a crash);
//  2. the sink is asked to deliver — idempotent sinks (TokenSink, including
//     the built-in FileSink) suppress a repeat of the same DeliveryID;
//  3. on success a "delivered" entry is fsynced.
//
// Recovery on the next invocation reconciles each (batch, rule):
//
//   - delivered entry  -> never sent again;
//   - pending entry    -> retried with the SAME DeliveryID; an idempotent sink
//     that already received it reports "suppressed duplicate" instead of a
//     second physical notification, and the entry is then marked delivered;
//   - no entry         -> a fresh pending intent is created and sent.
//
// Therefore a crash before delivery, during delivery, or after delivery but
// before journaling never causes a duplicate to an idempotent sink, while
// alerts that genuinely never went out are backfilled on every later
// invocation. Every Dispatch returns an AlertReport partitioning the expected
// alerts into already-delivered / sent-this-run / still-pending.
//
// Dispatch for one batch is serialized across processes via a per-batch flock
// (and within a process via a mutex), so concurrent triggers of the same batch
// produce one physical notification. Different batches use different lock
// files and never block each other.
//
// Compatibility: a custom sink implementing only Sink (not TokenSink) cannot
// be told the idempotency key, so a crash strictly between a successful send
// and the delivered journal entry can cause at most one repeat to that sink;
// the journal still guarantees "no duplicates on replay/rerun after a clean
// shutdown". Implement TokenSink for full exactly-once delivery.
package alert

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	// DeliveryID is the stable idempotency key for one (batch, rule) delivery
	// attempt, generated before the send and durable in the alert journal.
	// Idempotent sinks de-duplicate on it so a crash in any delivery window
	// never produces a second physical notification.
	DeliveryID string `json:"delivery_id,omitempty"`
}

// ReasonCode mirrors model.ReasonCode in JSON output.
type ReasonCode = model.ReasonCode

// Sink receives alerts.
type Sink interface {
	Send(ctx context.Context, a Alert) error
}

// TokenSink is implemented by sinks that can suppress a duplicate delivery
// themselves. The built-in FileSink implements it: an alert already present
// with the same DeliveryID is not written again. This is what closes the last
// crash window (delivery succeeded, journal not yet marked delivered).
type TokenSink interface {
	Sink
	// SendToken delivers a. Sent is false and err is nil when the sink had
	// already delivered the same DeliveryID (an idempotent retry suppressed as a
	// duplicate, not a new physical delivery).
	SendToken(ctx context.Context, a Alert) (sent bool, err error)
}

// FileSink appends one JSON object per alert line to a local file.
//
// It implements TokenSink: writes are de-duplicated on Alert.DeliveryID under a
// cross-process flock, so a process restart that retries an alert whose write
// already happened never appends it a second time.
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

// Send implements Sink (without idempotency-key suppression).
func (s *FileSink) Send(ctx context.Context, a Alert) error {
	_, err := s.sendLocked(ctx, a, false)
	return err
}

// SendToken implements TokenSink. When a.DeliveryID is non-empty and an alert
// with that id is already in the file, the write is suppressed and
// (false, nil) is returned.
func (s *FileSink) SendToken(ctx context.Context, a Alert) (bool, error) {
	return s.sendLocked(ctx, a, a.DeliveryID != "")
}

func (s *FileSink) sendLocked(ctx context.Context, a Alert, dedup bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if dedup {
		seen, err := fileHasDeliveryID(s.path, a.DeliveryID)
		if err != nil {
			return false, err
		}
		if seen {
			return false, nil // idempotent retry: suppress duplicate
		}
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return false, fmt.Errorf("write alert: %w", err)
	}
	if err := f.Sync(); err != nil {
		return false, fmt.Errorf("sync alert: %w", err)
	}
	return true, nil
}

// fileHasDeliveryID reports whether any JSON line in the file carries id. The
// alert file is small and local; a linear scan keeps the format append-only
// with no extra index.
func fileHasDeliveryID(path, id string) (bool, error) {
	f, err := os.Open(path)
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
		var got Alert
		if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
			continue // tolerate a torn trailing line written before a crash
		}
		if got.DeliveryID == id {
			return true, nil
		}
	}
	return false, sc.Err()
}

// journalStatus is the delivery phase recorded for one journal entry.
type journalStatus string

const (
	statusPending   journalStatus = "pending"
	statusDelivered journalStatus = "delivered"
)

// journalEntry is one append-only delivery record. The journal is never
// rewritten: a key typically gets a "pending" then a "delivered" line. Legacy
// entries written by older builds have no Status field and are treated as
// "delivered" (they recorded only successful sends).
type journalEntry struct {
	BatchID    string        `json:"batch_id"`
	RuleID     string        `json:"rule_id"`
	Status     journalStatus `json:"status,omitempty"`
	DeliveryID string        `json:"delivery_id,omitempty"`
}

// keyState is the reconciled state of one (batch, rule) from the journal.
type keyState struct {
	delivered bool
	pendingID string // DeliveryID of an unconfirmed delivery intent
}

// Alerter dispatches alerts exactly once per (batch, rule).
type Alerter struct {
	sink    Sink
	journal string
	log     *slog.Logger
	mu      sync.Mutex
}

// NewAlerter creates an Alerter backed by sink, using journalPath to remember
// delivery intents and confirmations.
func NewAlerter(sink Sink, journalPath string) (*Alerter, error) {
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o755); err != nil {
		return nil, fmt.Errorf("init alert journal dir: %w", err)
	}
	f, err := os.OpenFile(journalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	// Per-batch cross-process lock files live next to the journal.
	if err := os.MkdirAll(filepath.Join(filepath.Dir(journalPath), "alert-locks"), 0o755); err != nil {
		return nil, fmt.Errorf("init alert lock dir: %w", err)
	}
	return &Alerter{sink: sink, journal: journalPath, log: slog.Default()}, nil
}

// SetLogger installs the structured logger used to record the per-alert
// decision basis (already-delivered / retried with suppressed duplicate /
// newly sent / left pending and why).
func (a *Alerter) SetLogger(log *slog.Logger) {
	if log != nil {
		a.log = log
	}
}

// loadJournal reconciles the append-only journal into one state per key.
// Legacy entries without a Status are treated as delivered.
func (a *Alerter) loadJournal() (map[string]*keyState, error) {
	states := map[string]*keyState{}
	f, err := os.Open(a.journal)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return states, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var e journalEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			// A torn trailing line (process killed mid-write) must not poison
			// recovery; any earlier complete lines stay valid.
			if line == countNonEmptyLines(a.journal) {
				continue
			}
			return nil, fmt.Errorf("corrupt alert journal line %d: %w", line, err)
		}
		key := e.BatchID + "\x00" + e.RuleID
		st := states[key]
		if st == nil {
			st = &keyState{}
			states[key] = st
		}
		switch e.Status {
		case statusDelivered:
			st.delivered = true
			st.pendingID = ""
		case statusPending:
			if !st.delivered && e.DeliveryID != "" {
				st.pendingID = e.DeliveryID
			}
		case "":
			// Legacy record: older code only journaled after a successful send.
			st.delivered = true
			st.pendingID = ""
		}
	}
	return states, sc.Err()
}

func countNonEmptyLines(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(raw), "\n") {
		if len(strings.TrimSpace(l)) > 0 {
			n++
		}
	}
	return n
}

// appendJournal writes one fsynced entry to the journal.
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

func newDeliveryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// lockPath is the per-batch cross-process dispatch lock, derived from the
// batch id so different batches never contend.
func (a *Alerter) lockPath(batchID string) string {
	sum := sha256.Sum256([]byte(batchID))
	return filepath.Join(filepath.Dir(a.journal), "alert-locks", hex.EncodeToString(sum[:16])+".lock")
}

// Dispatch sends alerts for failed/errored results at or above the minimum
// severity, serialized per batch across processes. It is idempotent: a
// delivered (batch, rule) is never sent again; a crash-interrupted intent is
// retried under the same DeliveryID and suppressed by idempotent sinks; never
// delivered alerts are backfilled. Results are processed in rule-id order so
// the journal and output are deterministic.
//
// The returned report partitions expected alerts into already-delivered (in an
// earlier invocation), sent-this-run (a genuinely new physical delivery,
// including a successful backfill retry) and still-pending, so callers can see
// exactly how many were补齐 this invocation and how many remain undelivered.
func (a *Alerter) Dispatch(ctx context.Context, batchID string, results []model.RuleResult, minSeverity model.Severity) (model.AlertReport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Cross-process serialization for this batch only.
	lf, err := os.OpenFile(a.lockPath(batchID), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return model.AlertReport{}, fmt.Errorf("open alert dispatch lock: %w", err)
	}
	defer lf.Close()
	if err := lockWithContext(ctx, lf); err != nil {
		return model.AlertReport{}, err
	}
	defer func() { _ = unlockFile(lf) }()

	states, err := a.loadJournal()
	if err != nil {
		return model.AlertReport{}, err
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
		if res.Status == model.StatusPassed || !severityAtLeast(res.Severity, minSeverity) {
			continue
		}
		report.Expected++
		key := batchID + "\x00" + res.RuleID
		st := states[key]
		if st != nil && st.delivered {
			report.AlreadyDelivered++
			a.log.Debug("alert already delivered; skipping",
				"batch_id", batchID, "rule_id", res.RuleID)
			continue
		}
		if err := ctx.Err(); err != nil {
			break
		}

		// Reuse a crash-surviving pending DeliveryID; otherwise make the intent
		// durable BEFORE the send.
		deliveryID := ""
		if st != nil && st.pendingID != "" {
			deliveryID = st.pendingID
		} else {
			deliveryID = newDeliveryID()
			if err := appendJournal(jf, journalEntry{
				BatchID: batchID, RuleID: res.RuleID,
				Status: statusPending, DeliveryID: deliveryID,
			}); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}

		al := Alert{
			BatchID:    batchID,
			RuleID:     res.RuleID,
			Dataset:    res.Dataset,
			Kind:       res.Kind,
			Severity:   res.Severity,
			Status:     res.Status,
			Code:       primaryCode(res),
			Summary:    summarize(res),
			DeliveryID: deliveryID,
		}

		sent, sendErr := a.deliver(ctx, al, st != nil && st.pendingID != "")
		if sendErr != nil {
			if firstErr == nil {
				firstErr = sendErr
			}
			a.log.Warn("alert delivery failed; left pending for later backfill",
				"batch_id", batchID, "rule_id", res.RuleID,
				"delivery_id", deliveryID, "error", normalizedErr(sendErr))
			continue
		}
		if err := appendJournal(jf, journalEntry{
			BatchID: batchID, RuleID: res.RuleID,
			Status: statusDelivered, DeliveryID: deliveryID,
		}); err != nil {
			// Physical send happened but the confirmation could not be made
			// durable. Count it as sent; the pending intent plus the idempotent
			// sink keep the next retry from duplicating.
			if firstErr == nil {
				firstErr = err
			}
			a.log.Warn("alert sent but confirmation journal write failed",
				"batch_id", batchID, "rule_id", res.RuleID, "error", normalizedErr(err))
			report.SentThisRun++
			continue
		}
		if st == nil {
			st = &keyState{}
			states[key] = st
		}
		st.delivered = true
		st.pendingID = ""
		report.SentThisRun++
		if sent {
			a.log.Info("alert delivered", "batch_id", batchID, "rule_id", res.RuleID,
				"code", al.Code, "delivery_id", deliveryID)
		} else {
			a.log.Info("crash-interrupted alert confirmed; duplicate suppressed by sink",
				"batch_id", batchID, "rule_id", res.RuleID, "delivery_id", deliveryID)
		}
	}
	if err := ctx.Err(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}
	report.Pending = report.Expected - report.AlreadyDelivered - report.SentThisRun
	return report, firstErr
}

// deliver sends via the idempotent TokenSink when available (returning whether
// a new physical write happened), falling back to a plain Sink. recovered
// marks a retry of a crash-surviving pending intent.
func (a *Alerter) deliver(ctx context.Context, al Alert, recovered bool) (bool, error) {
	if ts, ok := a.sink.(TokenSink); ok {
		sent, err := ts.SendToken(ctx, al)
		if err != nil {
			return false, err
		}
		if !sent && !recovered {
			// A sink suppressed an alert for which we held no pending intent:
			// still an idempotent no-op, count it as delivered, not a new send.
			return false, nil
		}
		return sent, nil
	}
	if err := a.sink.Send(ctx, al); err != nil {
		return false, err
	}
	return true, nil
}

func normalizedErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
