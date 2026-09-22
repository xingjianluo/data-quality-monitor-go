// Package state provides durable batch state, historical conclusions and
// idempotent batch transitions with explicit rollback rejection.
//
// Lifecycle of a batch:
//
//	(absent) --Begin(running, lease)--> running --Commit(lease)--> committed
//
// Semantics:
//
//   - Repeating a committed batch replays its stored conclusions (idempotent);
//     the caller compares the fingerprint before deciding whether to re-evaluate.
//   - A batch left "running" by a crashed process is rejected until force=true
//     reclaims it (it is never silently continued from a partial middle state).
//   - Committing a batch older than the high-water mark is a state rollback and
//     is explicitly rejected; out-of-order arrivals can never move state back.
//   - All updates are atomic (temp file + rename) and process-exclusive (flock).
package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// BatchStatus marks where a batch is in its lifecycle.
type BatchStatus string

const (
	StatusRunning   BatchStatus = "running"
	StatusCommitted BatchStatus = "committed"
)

// Sentinel errors callers can distinguish with errors.Is.
var (
	// ErrStaleBatch means a batch is stuck in "running" and must be reclaimed
	// with force=true before it can run again.
	ErrStaleBatch = errors.New("batch is in running state from a previous interrupted execution; re-run with force to reclaim it")
	// ErrRollback means an attempt would move state behind the high-water mark.
	ErrRollback = errors.New("state rollback rejected: batch is older than the latest committed batch")
	// ErrLease means the lease token is missing, stale or already released.
	ErrLease = errors.New("invalid or expired batch lease")
	// ErrNotFound means no state exists for the batch.
	ErrNotFound = errors.New("batch not found")
	// ErrBusy means another operation currently holds the store lock.
	ErrBusy = errors.New("state store is locked by another operation")
)

// BatchState is the durable state of one batch.
type BatchState struct {
	BatchID     string             `json:"batch_id"`
	Status      BatchStatus        `json:"status"`
	LeaseToken  string             `json:"lease_token,omitempty"`
	Fingerprint string             `json:"fingerprint,omitempty"`
	StartedAt   time.Time          `json:"started_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	CommittedAt *time.Time         `json:"committed_at,omitempty"`
	Results     []model.RuleResult `json:"results,omitempty"`
}

// Store persists batch states and historical conclusions.
type Store interface {
	// Begin starts a batch. It returns the state and true when the batch was
	// already committed (caller should replay the returned conclusions); false
	// means a fresh "running" lease has been granted.
	Begin(ctx context.Context, batchID string, force bool) (*BatchState, bool, error)
	// Commit finalizes a running batch using its lease token.
	Commit(ctx context.Context, batchID, leaseToken, fingerprint string, results []model.RuleResult) error
	// Fail marks a running batch as failed and releases its lease. A failed
	// batch can only be restarted with force=true.
	Fail(ctx context.Context, batchID, leaseToken, reason string) error
	// Get returns the state of one batch.
	Get(ctx context.Context, batchID string) (*BatchState, error)
	// List returns historical batch states in deterministic order.
	List(ctx context.Context) ([]BatchState, error)
	// Close releases underlying resources.
	Close() error
}

// FileStore is a file-backed Store.
type FileStore struct {
	dir      string
	mu       sync.Mutex
	lockFile *os.File
}

// NewFileStore creates (or opens) a file-backed store rooted at dir.
func NewFileStore(dir string) (Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "batches"), 0o755); err != nil {
		return nil, fmt.Errorf("init state dir: %w", err)
	}
	lf, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	if err := lockFile(lf); err != nil {
		lf.Close()
		return nil, err
	}
	return &FileStore{dir: dir, lockFile: lf}, nil
}

func (s *FileStore) Close() error {
	if s.lockFile == nil {
		return nil
	}
	_ = unlockFile(s.lockFile)
	return s.lockFile.Close()
}

func (s *FileStore) batchPath(id string) string {
	return filepath.Join(s.dir, "batches", id+".json")
}

func (s *FileStore) read(id string) (*BatchState, error) {
	raw, err := os.ReadFile(s.batchPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read batch %s: %w", id, err)
	}
	var st BatchState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("corrupt batch state %s: %w", id, err)
	}
	return &st, nil
}

func (s *FileStore) write(st *BatchState) error {
	st.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.batchPath(st.BatchID) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write batch %s: %w", st.BatchID, err)
	}
	if err := os.Rename(tmp, s.batchPath(st.BatchID)); err != nil {
		return fmt.Errorf("commit batch file %s: %w", st.BatchID, err)
	}
	return nil
}

// highWater returns the lexicographically greatest committed batch id.
// Batch ids are ordered (e.g. timestamps/sequence numbers) so the greatest id
// is the most recently committed batch.
func (s *FileStore) highWater() (string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "batches"))
	if err != nil {
		return "", err
	}
	hw := ""
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		st, err := s.read(name[:len(name)-len(".json")])
		if err != nil {
			return "", err
		}
		if st.Status == StatusCommitted && st.BatchID > hw {
			hw = st.BatchID
		}
	}
	return hw, nil
}

func newLeaseToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *FileStore) Begin(ctx context.Context, batchID string, force bool) (*BatchState, bool, error) {
	if batchID == "" {
		return nil, false, errors.New("empty batch id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	hw, err := s.highWater()
	if err != nil {
		return nil, false, err
	}
	if hw != "" && batchID < hw {
		return nil, false, fmt.Errorf("%w: %s < %s", ErrRollback, batchID, hw)
	}

	st, err := s.read(batchID)
	switch {
	case errors.Is(err, ErrNotFound):
		// fresh batch
	case err != nil:
		return nil, false, err
	default:
		if st.Status == StatusCommitted {
			return st, true, nil
		}
		if !force {
			return nil, false, fmt.Errorf("%w: batch %s", ErrStaleBatch, batchID)
		}
		// Explicit reclaim: discard the interrupted middle state and restart.
	}

	now := time.Now().UTC()
	fresh := &BatchState{
		BatchID:    batchID,
		Status:     StatusRunning,
		LeaseToken: newLeaseToken(),
		StartedAt:  now,
		UpdatedAt:  now,
		Results:    []model.RuleResult{},
	}
	if err := s.write(fresh); err != nil {
		return nil, false, err
	}
	return fresh, false, nil
}

func (s *FileStore) Commit(ctx context.Context, batchID, leaseToken, fingerprint string, results []model.RuleResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	hw, err := s.highWater()
	if err != nil {
		return err
	}
	if hw != "" && batchID < hw {
		return fmt.Errorf("%w: %s < %s", ErrRollback, batchID, hw)
	}
	st, err := s.read(batchID)
	if err != nil {
		return err
	}
	if st.Status != StatusRunning {
		return fmt.Errorf("%w: batch %s is %s, not running", ErrLease, batchID, st.Status)
	}
	if st.LeaseToken == "" || st.LeaseToken != leaseToken {
		return fmt.Errorf("%w: lease mismatch for batch %s", ErrLease, batchID)
	}
	now := time.Now().UTC()
	st.Status = StatusCommitted
	st.LeaseToken = ""
	st.Fingerprint = fingerprint
	st.CommittedAt = &now
	if results == nil {
		results = []model.RuleResult{}
	}
	st.Results = results
	return s.write(st)
}

func (s *FileStore) Fail(ctx context.Context, batchID, leaseToken, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := s.read(batchID)
	if err != nil {
		return err
	}
	if st.Status != StatusRunning || st.LeaseToken != leaseToken {
		return fmt.Errorf("%w: cannot fail batch %s", ErrLease, batchID)
	}
	// A failed batch keeps status "running" with an emptied lease so it cannot
	// be silently continued; force=true in Begin is required to reclaim it.
	st.LeaseToken = ""
	st.Fingerprint = "__failed__:" + reason
	st.Results = []model.RuleResult{}
	return s.write(st)
}

func (s *FileStore) Get(ctx context.Context, batchID string) (*BatchState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.read(batchID)
}

func (s *FileStore) List(ctx context.Context) ([]BatchState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.dir, "batches"))
	if err != nil {
		return nil, err
	}
	var out []BatchState
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		id := e.Name()[:len(e.Name())-len(".json")]
		st, err := s.read(id)
		if err != nil {
			return nil, err
		}
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchID < out[j].BatchID })
	return out, nil
}
