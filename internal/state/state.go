// Package state provides durable batch state, historical conclusions and
// idempotent batch transitions with explicit rejection of conclusion rewrites.
//
// Lifecycle of a batch:
//
//	(absent) --Begin(running, lease)--> running --Commit(lease)--> committed
//
// Semantics:
//
//   - Late arrivals are first-class: a batch id that has never been processed
//     is accepted regardless of its id order relative to already committed
//     batches. Ordering never causes an unseen batch to be dropped.
//   - Repeating an already committed batch never overwrites its conclusions.
//     Begin replays the stored conclusions; the caller verifies the inputs
//     behind the conclusions are still valid before treating them as current.
//     force=true on a committed batch is rejected with ErrAlreadyConcluded.
//   - A batch observed "running" with a live lease returns ErrBatchRunning
//     (concurrent triggers must wait/share instead of executing twice). A
//     running batch whose lease has expired is a crash leftover: rejected with
//     ErrStaleBatch until force=true reclaims it (it is never silently
//     continued from a partial middle state).
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
	"strings"
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

// DefaultLeaseTTL is how long a lease stays valid without a heartbeat. A live
// engine renews periodically; a batch still "running" past its lease expiry is
// treated as a crash leftover.
const DefaultLeaseTTL = 30 * time.Second

// Sentinel errors callers can distinguish with errors.Is.
var (
	// ErrAlreadyConcluded means the batch id already has committed conclusions;
	// historical conclusions are never overwritten. Re-run the same inputs as a
	// replay, or use a new batch id. It is distinct from ErrBatchRunning and
	// ErrStaleBatch so the rejection reason is unambiguous.
	ErrAlreadyConcluded = errors.New("batch already has committed conclusions: historical conclusions cannot be overwritten; re-run replays them or use a new batch id")
	// ErrBatchRunning means the batch is currently executing under a live lease.
	// Concurrent triggers must wait for that execution or return its result.
	ErrBatchRunning = errors.New("batch is currently running under another execution")
	// ErrStaleBatch means a batch is stuck in "running" with an expired lease
	// (a crash leftover) and must be reclaimed with force=true.
	ErrStaleBatch = errors.New("batch is in running state from a previous interrupted execution with an expired lease; re-run with force to reclaim it")
	// ErrLease means the lease token is missing, stale or already released.
	ErrLease = errors.New("invalid or expired batch lease")
	// ErrNotFound means no state exists for the batch.
	ErrNotFound = errors.New("batch not found")
	// ErrBusy means another operation currently holds the store lock.
	ErrBusy = errors.New("state store is locked by another operation")
)

// DatasetInput is the durable fingerprint of one dataset input at commit time.
type DatasetInput struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Format      string `json:"format"`
	SchemaHash  string `json:"schema_hash"`  // declared schema + dataset declaration
	ShapeHash   string `json:"shape_hash"`   // fields actually observed in the file
	ContentHash string `json:"content_hash"` // full file content hash; "missing" if unreadable
	RecordCount int    `json:"record_count"`
	LoadError   string `json:"load_error,omitempty"` // normalized code when unreadable/unparseable
}

// InputSnapshot captures every input behind a committed conclusion so a later
// replay can tell whether the historical conclusions are still valid, and
// whether a difference comes from the data side or the rule side.
type InputSnapshot struct {
	EvalTime time.Time      `json:"eval_time"`
	RuleHash string         `json:"rule_hash"`
	Datasets []DatasetInput `json:"datasets"`
}

// BatchState is the durable state of one batch.
type BatchState struct {
	BatchID     string             `json:"batch_id"`
	Status      BatchStatus        `json:"status"`
	LeaseToken  string             `json:"lease_token,omitempty"`
	LeaseExpiry *time.Time         `json:"lease_expiry,omitempty"`
	Fingerprint string             `json:"fingerprint,omitempty"`
	StartedAt   time.Time          `json:"started_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	CommittedAt *time.Time         `json:"committed_at,omitempty"`
	Results     []model.RuleResult `json:"results,omitempty"`
	// Inputs is the input snapshot captured at commit time, used for replay
	// validity checks (data drift vs rule drift).
	Inputs *InputSnapshot `json:"inputs,omitempty"`
}

// Store persists batch states and historical conclusions.
type Store interface {
	// Begin starts a batch. If the batch was already committed it returns the
	// stored state and replay=true without modifying anything. A fresh batch
	// gets a "running" lease. A batch currently executing under a live lease
	// returns ErrBatchRunning; a crash leftover (expired lease) returns
	// ErrStaleBatch unless force=true reclaims it.
	Begin(ctx context.Context, batchID string, force bool) (*BatchState, bool, error)
	// Commit finalizes a running batch using its lease token and records the
	// input snapshot for later replay-validity checks.
	Commit(ctx context.Context, batchID, leaseToken, fingerprint string, results []model.RuleResult, inputs *InputSnapshot) error
	// Heartbeat extends the lease of a running batch while it executes, so a
	// live long-running batch is never mistaken for a crash leftover.
	Heartbeat(ctx context.Context, batchID, leaseToken string) error
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

// lockShards bounds the number of in-process mutexes used to serialize batch
// lock-file open/flock operations within one process.
const lockShards = 64

// FileStore is a file-backed Store.
//
// Locking is deliberately per-batch and held only for the duration of one
// state transition: different batches never block each other and read-only
// queries take no lock at all. The durable cross-process primitive is an flock
// on <dir>/locks/<batch-id>.lock; the sharded in-process mutexes additionally
// serialize same-batch operations inside one process (and stand in for flock
// on platforms without it).
type FileStore struct {
	dir      string
	leaseTTL time.Duration
	shardMu  [lockShards]sync.Mutex
}

// NewFileStore creates (or opens) a file-backed store rooted at dir.
func NewFileStore(dir string) (Store, error) {
	return NewFileStoreWithTTL(dir, DefaultLeaseTTL)
}

// NewFileStoreWithTTL creates a file-backed store with an explicit lease TTL.
// Mainly useful for tests that need fast crash-leftover detection.
func NewFileStoreWithTTL(dir string, leaseTTL time.Duration) (Store, error) {
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	if err := os.MkdirAll(filepath.Join(dir, "batches"), 0o755); err != nil {
		return nil, fmt.Errorf("init state dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "locks"), 0o755); err != nil {
		return nil, fmt.Errorf("init lock dir: %w", err)
	}
	return &FileStore{dir: dir, leaseTTL: leaseTTL}, nil
}

func (s *FileStore) Close() error { return nil }

// validBatchID guards the lock/batch file path against traversal.
func validBatchID(batchID string) bool {
	if batchID == "" || batchID == "." || batchID == ".." {
		return false
	}
	if strings.ContainsAny(batchID, `/\`) {
		return false
	}
	return true
}

// shard returns the in-process mutex shard for a batch id.
func (s *FileStore) shard(batchID string) *sync.Mutex {
	h := fnv32(batchID)
	return &s.shardMu[h%lockShards]
}

func fnv32(s string) uint32 {
	const offset, prime = uint32(2166136261), uint32(16777619)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

// withBatchLock runs fn while holding the cross-process per-batch lock. Read
// paths intentionally do not take it: batch files are replaced atomically via
// rename, so concurrent readers always observe a complete prior or new file.
func (s *FileStore) withBatchLock(ctx context.Context, batchID string, fn func() error) error {
	if !validBatchID(batchID) {
		return errors.New("invalid batch id")
	}
	mu := s.shard(batchID)
	mu.Lock()
	defer mu.Unlock()
	lf, err := os.OpenFile(filepath.Join(s.dir, "locks", batchID+".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open batch lock: %w", err)
	}
	defer lf.Close()
	if err := lockWithContext(ctx, lf); err != nil {
		return err
	}
	defer func() { _ = unlockFile(lf) }()
	return fn()
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

// leaseLive reports whether a running batch still holds an unexpired lease. A
// running state with no token (Fail was called) or a past expiry is a crash
// leftover.
func (s *FileStore) leaseLive(st *BatchState, now time.Time) bool {
	if st.Status != StatusRunning || st.LeaseToken == "" {
		return false
	}
	if st.LeaseExpiry == nil {
		return false
	}
	return st.LeaseExpiry.After(now)
}

func newLeaseToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *FileStore) Begin(ctx context.Context, batchID string, force bool) (*BatchState, bool, error) {
	if !validBatchID(batchID) {
		return nil, false, errors.New("empty batch id")
	}
	var out *BatchState
	replay := false
	err := s.withBatchLock(ctx, batchID, func() error {
		r, err := s.beginLocked(ctx, batchID, force)
		if err != nil {
			return err
		}
		out, replay = r.st, r.replay
		return nil
	})
	return out, replay, err
}

type beginResult struct {
	st     *BatchState
	replay bool
}

func (s *FileStore) beginLocked(ctx context.Context, batchID string, force bool) (beginResult, error) {
	if err := ctx.Err(); err != nil {
		return beginResult{}, err
	}

	st, err := s.read(batchID)
	switch {
	case errors.Is(err, ErrNotFound):
		// Unseen batch — including an older id arriving late. Accepted.
	case err != nil:
		return beginResult{}, err
	default:
		if st.Status == StatusCommitted {
			// Historical conclusions are immutable: a replay never overwrites
			// them, and force cannot reclaim a committed batch.
			if force {
				return beginResult{}, fmt.Errorf("%w: batch %s", ErrAlreadyConcluded, batchID)
			}
			return beginResult{st: st, replay: true}, nil
		}
		if s.leaseLive(st, time.Now().UTC()) {
			return beginResult{}, fmt.Errorf("%w: batch %s", ErrBatchRunning, batchID)
		}
		if !force {
			return beginResult{}, fmt.Errorf("%w: batch %s", ErrStaleBatch, batchID)
		}
		// Explicit reclaim: discard the interrupted middle state and restart.
	}

	now := time.Now().UTC()
	expiry := now.Add(s.leaseTTL)
	fresh := &BatchState{
		BatchID:     batchID,
		Status:      StatusRunning,
		LeaseToken:  newLeaseToken(),
		LeaseExpiry: &expiry,
		StartedAt:   now,
		UpdatedAt:   now,
		Results:     []model.RuleResult{},
	}
	if err := s.write(fresh); err != nil {
		return beginResult{}, err
	}
	return beginResult{st: fresh}, nil
}

func (s *FileStore) Commit(ctx context.Context, batchID, leaseToken, fingerprint string, results []model.RuleResult, inputs *InputSnapshot) error {
	if !validBatchID(batchID) {
		return errors.New("invalid batch id")
	}
	return s.withBatchLock(ctx, batchID, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := s.read(batchID)
		if err != nil {
			return err
		}
		if st.Status == StatusCommitted {
			// Never overwrite an existing conclusion, even with a token.
			return fmt.Errorf("%w: batch %s already committed", ErrAlreadyConcluded, batchID)
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
		st.LeaseExpiry = nil
		st.Fingerprint = fingerprint
		st.CommittedAt = &now
		if results == nil {
			results = []model.RuleResult{}
		}
		st.Results = results
		st.Inputs = inputs
		return s.write(st)
	})
}

func (s *FileStore) Heartbeat(ctx context.Context, batchID, leaseToken string) error {
	if !validBatchID(batchID) {
		return errors.New("invalid batch id")
	}
	return s.withBatchLock(ctx, batchID, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := s.read(batchID)
		if err != nil {
			return err
		}
		if st.Status != StatusRunning || st.LeaseToken == "" || st.LeaseToken != leaseToken {
			return fmt.Errorf("%w: cannot heartbeat batch %s", ErrLease, batchID)
		}
		expiry := time.Now().UTC().Add(s.leaseTTL)
		st.LeaseExpiry = &expiry
		return s.write(st)
	})
}

func (s *FileStore) Fail(ctx context.Context, batchID, leaseToken, reason string) error {
	if !validBatchID(batchID) {
		return errors.New("invalid batch id")
	}
	return s.withBatchLock(ctx, batchID, func() error {
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
		// A failed batch keeps status "running" with an emptied lease so it
		// cannot be silently continued; force=true in Begin is required to
		// reclaim it.
		st.LeaseToken = ""
		st.LeaseExpiry = nil
		st.Fingerprint = "__failed__:" + reason
		st.Results = []model.RuleResult{}
		return s.write(st)
	})
}

// Get is a read-only query. It takes no lock: batch files are replaced
// atomically via temp-file + rename, so a concurrent committer never exposes a
// torn file. This lets history/replay proceed while a batch is running in
// another process.
func (s *FileStore) Get(ctx context.Context, batchID string) (*BatchState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.read(batchID)
}

// List is a read-only query and never blocks on a running batch.
func (s *FileStore) List(ctx context.Context) ([]BatchState, error) {
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
