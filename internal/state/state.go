// Package state provides durable batch state, historical conclusions and
// idempotent batch transitions with explicit conflict classification.
//
// Lifecycle of a batch:
//
//	(absent) --Begin(running, lease)--> running --Commit(lease)--> committed
//
// Semantics:
//
//   - Repeating a committed batch replays its stored conclusions (idempotent);
//     the caller compares the stored input manifest against current inputs
//     before deciding whether those conclusions still apply.
//   - Late-arriving batches (batch ids older than the latest committed one)
//     that have never been processed are accepted normally: high-water marks
//     never block evaluation, they only order history.
//   - A concurrent trigger against a live "running" batch fails with
//     ErrBatchRunning; a "running" batch whose lease heartbeat expired (a
//     crashed process) fails with ErrStaleBatch until force=true reclaims it.
//   - Committed conclusions are immutable. force=true cannot overwrite them;
//     mismatched repeats are reported by the caller via model.HistoryInvalid.
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

	manifestpkg "github.com/xingjianluo/data-quality-monitor-go/internal/manifest"
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
	// ErrStaleBatch means a batch is stuck in "running" with a dead lease
	// (heartbeat expired) and must be reclaimed with force=true before it can
	// run again.
	ErrStaleBatch = errors.New("batch is in running state from a previous interrupted execution; re-run with force to reclaim it")
	// ErrBatchRunning means a live execution currently holds the batch lease.
	// Concurrent triggers must either wait for its result or treat the batch as
	// in progress instead of starting a second evaluation.
	ErrBatchRunning = errors.New("batch is already being executed by another live run")
	// ErrImmutable means a batch already has committed conclusions and the
	// caller attempted to overwrite them (e.g. force=true). Historical
	// conclusions are never rewritten; inputs that no longer match are reported
	// separately via model.HistoryInvalidError.
	ErrImmutable = errors.New("batch already has committed conclusions which cannot be overwritten")
	// ErrLease means the lease token is missing, stale or already released.
	ErrLease = errors.New("invalid or expired batch lease")
	// ErrNotFound means no state exists for the batch.
	ErrNotFound = errors.New("batch not found")
	// ErrBusy means another operation currently holds the store lock.
	ErrBusy = errors.New("state store is locked by another operation")
)

// DefaultLeaseTTL is how long a "running" lease stays alive without a
// heartbeat. An execution renews its heartbeat periodically; a lease older
// than the TTL is considered abandoned (interrupted process).
const DefaultLeaseTTL = 30 * time.Second

// DefaultLockWait bounds how long a Begin waits for another process to leave
// its short store critical section before returning ErrBusy.
const DefaultLockWait = 5 * time.Second

// BatchState is the durable state of one batch.
type BatchState struct {
	BatchID     string      `json:"batch_id"`
	Status      BatchStatus `json:"status"`
	LeaseToken  string      `json:"lease_token,omitempty"`
	Fingerprint string      `json:"fingerprint,omitempty"`
	StartedAt   time.Time   `json:"started_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	// HeartbeatAt is the last lease renewal of the live execution; zero for
	// never-renewed leases.
	HeartbeatAt *time.Time            `json:"heartbeat_at,omitempty"`
	CommittedAt *time.Time            `json:"committed_at,omitempty"`
	Manifest    *manifestpkg.Manifest `json:"manifest,omitempty"`
	Results     []model.RuleResult    `json:"results,omitempty"`
}

// Store persists batch states and historical conclusions.
type Store interface {
	// Begin starts a batch. It returns the state and true when the batch was
	// already committed (caller must validate the stored manifest against the
	// current inputs before replaying the returned conclusions); false means a
	// fresh "running" lease has been granted.
	//
	// A batch whose lease is still alive returns ErrBatchRunning; a batch left
	// running by a crashed (heartbeat-expired) process returns ErrStaleBatch
	// unless force=true reclaims it. force=true never reclaims a committed
	// batch (ErrImmutable).
	Begin(ctx context.Context, batchID string, force bool) (*BatchState, bool, error)
	// Heartbeat renews the lease of a live execution so concurrent triggers
	// keep seeing ErrBatchRunning rather than reclaiming it as stale.
	Heartbeat(ctx context.Context, batchID, leaseToken string) error
	// Commit finalizes a running batch using its lease token and records the
	// manifest of inputs the conclusions were produced from.
	Commit(ctx context.Context, batchID, leaseToken, fingerprint string, mf manifestpkg.Manifest, results []model.RuleResult) error
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
	leaseTTL time.Duration
	lockWait time.Duration
	mu       sync.Mutex
	lockFile *os.File
}

// NewFileStore creates (or opens) a file-backed store rooted at dir.
func NewFileStore(dir string) (Store, error) {
	return NewFileStoreWithTTL(dir, DefaultLeaseTTL)
}

// NewFileStoreWithTTL is NewFileStore with an explicit lease TTL. The TTL
// decides when a "running" lease without heartbeats counts as abandoned.
func NewFileStoreWithTTL(dir string, ttl time.Duration) (Store, error) {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	if err := os.MkdirAll(filepath.Join(dir, "batches"), 0o755); err != nil {
		return nil, fmt.Errorf("init state dir: %w", err)
	}
	lf, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	return &FileStore{dir: dir, leaseTTL: ttl, lockWait: DefaultLockWait, lockFile: lf}, nil
}

func (s *FileStore) Close() error {
	if s.lockFile == nil {
		return nil
	}
	_ = unlockFile(s.lockFile)
	return s.lockFile.Close()
}

// lock acquires the process-exclusive store lock for one short critical
// section; it returns ErrBusy when another process currently holds it. The
// lock is never held for a whole evaluation run, so concurrent processes can
// reach per-batch lease coordination (ErrBatchRunning / replay) instead of
// failing while merely opening the store.
func (s *FileStore) lock() error { return lockFile(s.lockFile) }

func (s *FileStore) unlock() { _ = unlockFile(s.lockFile) }

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

// leaseAlive reports whether a running lease is still being renewed by a live
// execution. A lease is alive within ttl of its latest heartbeat (falling back
// to StartedAt when no heartbeat has ever been written).
func (s *FileStore) leaseAlive(st *BatchState, now time.Time) bool {
	last := st.StartedAt
	if st.HeartbeatAt != nil {
		last = *st.HeartbeatAt
	}
	return now.Sub(last) < s.leaseTTL
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
	// A concurrent trigger waits briefly for an in-flight critical section,
	// then classifies the batch via its lease (live=ErrBatchRunning,
	// expired=ErrStaleBatch, committed=replay) instead of failing on lock
	// contention itself.
	if err := acquireLock(s.lockFile, s.lockWait); err != nil {
		return nil, false, err
	}
	defer s.unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	st, err := s.read(batchID)
	switch {
	case errors.Is(err, ErrNotFound):
		// fresh batch (late arrivals included: an unseen older batch is fine)
	case err != nil:
		return nil, false, err
	default:
		if st.Status == StatusCommitted {
			// Committed conclusions are immutable for everyone. force cannot
			// rewrite them; the caller validates inputs via the manifest.
			if force {
				return nil, false, fmt.Errorf("%w: batch %s already committed", ErrImmutable, batchID)
			}
			return st, true, nil
		}
		// status == running
		if st.LeaseToken != "" && s.leaseAlive(st, time.Now().UTC()) {
			return nil, false, fmt.Errorf("%w: batch %s", ErrBatchRunning, batchID)
		}
		if !force {
			return nil, false, fmt.Errorf("%w: batch %s", ErrStaleBatch, batchID)
		}
		// Explicit reclaim: discard the interrupted middle state and restart.
	}

	now := time.Now().UTC()
	fresh := &BatchState{
		BatchID:     batchID,
		Status:      StatusRunning,
		LeaseToken:  newLeaseToken(),
		StartedAt:   now,
		UpdatedAt:   now,
		HeartbeatAt: &now,
		Results:     []model.RuleResult{},
	}
	if err := s.write(fresh); err != nil {
		return nil, false, err
	}
	return fresh, false, nil
}

func (s *FileStore) Heartbeat(ctx context.Context, batchID, leaseToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock(); err != nil {
		return err
	}
	defer s.unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := s.read(batchID)
	if err != nil {
		return err
	}
	if st.Status != StatusRunning || st.LeaseToken == "" || st.LeaseToken != leaseToken {
		return fmt.Errorf("%w: heartbeat rejected for batch %s", ErrLease, batchID)
	}
	now := time.Now().UTC()
	st.HeartbeatAt = &now
	return s.write(st)
}

func (s *FileStore) Commit(ctx context.Context, batchID, leaseToken, fingerprint string, mf manifestpkg.Manifest, results []model.RuleResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock(); err != nil {
		return err
	}
	defer s.unlock()
	if err := ctx.Err(); err != nil {
		return err
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
	st.HeartbeatAt = nil
	st.Fingerprint = fingerprint
	st.CommittedAt = &now
	manifestCopy := mf
	st.Manifest = &manifestCopy
	if results == nil {
		results = []model.RuleResult{}
	}
	st.Results = results
	return s.write(st)
}

func (s *FileStore) Fail(ctx context.Context, batchID, leaseToken, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock(); err != nil {
		return err
	}
	defer s.unlock()
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
	if err := s.lock(); err != nil {
		return nil, err
	}
	defer s.unlock()
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
