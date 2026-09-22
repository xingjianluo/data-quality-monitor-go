// Package alert normalizes and deduplicates rule failure notifications.
//
// Alerts are emitted exactly once per (batch, rule). Dispatched keys are
// persisted in an append-only JSONL journal, so a repeated batch, an
// out-of-order replay or a post-crash re-run never sends duplicate alerts and a
// crashed dispatch resumes without re-notifying already delivered rules.
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

// Send implements Sink, appending one JSON line per alert.
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
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write alert: %w", err)
	}
	return nil
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
	return &Alerter{sink: sink, journal: journalPath}, nil
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
		seen[e.BatchID+"\x00"+e.RuleID] = true
	}
	return seen, sc.Err()
}

// Dispatch sends alerts for failed/errored results at or above the minimum
// severity. It is idempotent: a (batch, rule) pair already present in the
// journal is never sent again. Results are processed in order so the journal
// and output are deterministic.
func (a *Alerter) Dispatch(ctx context.Context, batchID string, results []model.RuleResult, minSeverity model.Severity) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	dispatched, err := a.loadDispatched()
	if err != nil {
		return 0, err
	}
	jf, err := os.OpenFile(a.journal, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer jf.Close()

	ordered := append([]model.RuleResult(nil), results...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].RuleID < ordered[j].RuleID })

	sent := 0
	var firstErr error
	for _, res := range ordered {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		if res.Status == model.StatusPassed {
			continue
		}
		if !severityAtLeast(res.Severity, minSeverity) {
			continue
		}
		key := batchID + "\x00" + res.RuleID
		if dispatched[key] {
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
		if err := a.sink.Send(ctx, al); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Record delivery before reporting success; on crash the next run
		// resumes without re-notifying this rule.
		raw, _ := json.Marshal(journalEntry{BatchID: batchID, RuleID: res.RuleID})
		if _, err := jf.Write(append(raw, '\n')); err != nil && firstErr == nil {
			firstErr = err
			continue
		}
		dispatched[key] = true
		sent++
	}
	return sent, firstErr
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
