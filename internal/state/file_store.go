package state

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// fileEnvelope 是状态文件的磁盘格式。版本号用于未来格式演进与兼容拒绝。
type fileEnvelope struct {
	Version int           `json:"version"`
	Batches []BatchRecord `json:"batches"`
}

const currentFileVersion = 1

// FileStore 是 JSON 文件实现的 Store：写入采用临时文件 + fsync + 原子重命名。
type FileStore struct {
	mu   sync.Mutex
	path string
	env  fileEnvelope
}

// OpenFileStore 打开（不存在则初始化空）状态文件。
// 不支持的文件版本会被明确拒绝，避免按错误格式继续。
func OpenFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, env: fileEnvelope{Version: currentFileVersion}}
	raw, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		if werr := s.persistLocked(); werr != nil {
			return nil, werr
		}
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("open state file %s: %w", path, err)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s.env); err != nil {
			return nil, fmt.Errorf("parse state file %s: %w", path, err)
		}
	}
	if s.env.Version != currentFileVersion {
		return nil, fmt.Errorf("state file %s has unsupported version %d (supported: %d)",
			path, s.env.Version, currentFileVersion)
	}
	sortRecords(s.env.Batches)
	return s, nil
}

func (s *FileStore) LatestOrdinal() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := uint64(0)
	for _, r := range s.env.Batches {
		if r.Ordinal > latest {
			latest = r.Ordinal
		}
	}
	return latest
}

func (s *FileStore) Get(batchID string) (BatchRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return findByID(s.env.Batches, batchID)
}

func (s *FileStore) GetByOrdinal(ordinal uint64) (BatchRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.env.Batches {
		if r.Ordinal == ordinal {
			return r, true
		}
	}
	return BatchRecord{}, false
}

func (s *FileStore) Commit(ctx context.Context, rec BatchRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := applyCommit(append([]BatchRecord(nil), s.env.Batches...), rec)
	if err != nil {
		return err
	}
	previous := s.env.Batches
	s.env.Batches = next
	if err := s.persistLocked(); err != nil {
		s.env.Batches = previous // 持久化失败回滚内存视图
		return err
	}
	return nil
}

func (s *FileStore) MarkAlertsSent(ctx context.Context, batchID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i := range s.env.Batches {
		if s.env.Batches[i].BatchID == batchID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return errUnknownBatch(batchID)
	}
	previous := s.env.Batches[idx].AlertsSent
	s.env.Batches[idx].AlertsSent = true
	if err := s.persistLocked(); err != nil {
		s.env.Batches[idx].AlertsSent = previous
		return err
	}
	return nil
}

func (s *FileStore) Close() error { return nil }

// persistLocked 原子落盘：临时文件 -> fsync -> rename -> fsync 目录。
// 调用方必须持有 s.mu。
func (s *FileStore) persistLocked() error {
	s.env.Version = currentFileVersion
	raw, err := json.MarshalIndent(s.env, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename state: %w", err)
	}
	// fsync 目录，保证重命名在崩溃后持久。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
