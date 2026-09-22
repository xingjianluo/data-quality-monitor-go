package state

import (
	"context"
	"sync"
)

// MemoryStore 是进程内 Store，主要用于测试。
type MemoryStore struct {
	mu      sync.Mutex
	records []BatchRecord
}

// NewMemoryStore 创建空的内存状态存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) snapshot() []BatchRecord {
	out := make([]BatchRecord, len(s.records))
	copy(out, s.records)
	return out
}

func (s *MemoryStore) LatestOrdinal() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := uint64(0)
	for _, r := range s.records {
		if r.Ordinal > latest {
			latest = r.Ordinal
		}
	}
	return latest
}

func (s *MemoryStore) Get(batchID string) (BatchRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return findByID(s.records, batchID)
}

func (s *MemoryStore) GetByOrdinal(ordinal uint64) (BatchRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records {
		if r.Ordinal == ordinal {
			return r, true
		}
	}
	return BatchRecord{}, false
}

func (s *MemoryStore) Commit(ctx context.Context, rec BatchRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := applyCommit(s.snapshot(), rec)
	if err != nil {
		return err
	}
	s.records = next
	return nil
}

func (s *MemoryStore) MarkAlertsSent(ctx context.Context, batchID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		if s.records[i].BatchID == batchID {
			s.records[i].AlertsSent = true
			return nil
		}
	}
	return errUnknownBatch(batchID)
}

func (s *MemoryStore) Close() error { return nil }
