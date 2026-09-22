package state

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func sampleRecord(batchID string, ordinal uint64) BatchRecord {
	return BatchRecord{
		BatchID:     batchID,
		Ordinal:     ordinal,
		WatermarkMs: 1000,
		CommittedMs: 2000,
		Inputs:      []model.InputSummary{{Dataset: "d", Rows: 1, Schema: "int:x"}},
		Rules:       []model.RuleSpec{{ID: "r1", Type: model.RuleUniqueness, Dataset: "d", Columns: []string{"x"}}},
		Results: []model.RuleResult{{
			RuleID: "r1", RuleType: model.RuleUniqueness, Dataset: "d",
			Status: model.StatusPass, Checked: 1,
		}},
		Schemas: map[string]string{"d": "int:x"},
	}
}

func TestMemoryStoreCommitIdempotent(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	rec := sampleRecord("b1", 1)
	if err := s.Commit(ctx, rec); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := s.Commit(ctx, rec); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	if got, ok := s.Get("b1"); !ok || got.Ordinal != 1 {
		t.Fatalf("get b1 = %+v ok=%v", got, ok)
	}
	if s.LatestOrdinal() != 1 {
		t.Fatalf("latest = %d, want 1", s.LatestOrdinal())
	}
}

func TestMemoryStoreSameIDDifferentContentConflict(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if err := s.Commit(ctx, sampleRecord("b1", 1)); err != nil {
		t.Fatal(err)
	}
	other := sampleRecord("b1", 1)
	other.WatermarkMs = 9999
	err := s.Commit(ctx, other)
	if errs.CodeOf(err) != model.ReasonStateConflict {
		t.Fatalf("want state_conflict, got %v (code=%s)", err, errs.CodeOf(err))
	}
}

func TestMemoryStoreOutOfOrderAcceptedOccupiedOrdinalRejected(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	// 乱序/迟到：先提交 ordinal=2，未被占用的 ordinal=1 随后到达应被接受（不漏报）。
	if err := s.Commit(ctx, sampleRecord("b2", 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, sampleRecord("b1", 1)); err != nil {
		t.Fatalf("unoccupied earlier ordinal is late arrival, want accepted: %v", err)
	}
	if s.LatestOrdinal() != 2 {
		t.Fatalf("latest = %d, want 2", s.LatestOrdinal())
	}
	// 抢占已占用序号：明确拒绝为 state_rollback。
	if err := s.Commit(ctx, sampleRecord("b2-other", 2)); errs.CodeOf(err) != model.ReasonStateRollback {
		t.Fatalf("want state_rollback for occupied ordinal, got %v", err)
	}
	// 空洞序号也接受。
	if err := s.Commit(ctx, sampleRecord("b5", 5)); err != nil {
		t.Fatalf("ordinal 5 should be accepted: %v", err)
	}
	if s.LatestOrdinal() != 5 {
		t.Fatalf("latest = %d, want 5", s.LatestOrdinal())
	}
	// 两个历史批次都可按序号复现。
	if _, ok := s.GetByOrdinal(1); !ok {
		t.Fatal("late batch at ordinal 1 must be retrievable")
	}
}

func TestMemoryStoreMarkAlertsSentIdempotent(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if err := s.Commit(ctx, sampleRecord("b1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAlertsSent(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	rec, _ := s.Get("b1")
	if !rec.AlertsSent {
		t.Fatal("alerts should be marked sent")
	}
	if err := s.MarkAlertsSent(ctx, "b1"); err != nil {
		t.Fatalf("repeat mark should be idempotent: %v", err)
	}
	if err := s.MarkAlertsSent(ctx, "ghost"); errs.CodeOf(err) != model.ReasonStateConflict {
		t.Fatalf("unknown batch mark: want conflict, got %v", err)
	}
}

func TestFileStorePersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()

	s1, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Commit(ctx, sampleRecord("b1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s1.Commit(ctx, sampleRecord("b2", 2)); err != nil {
		t.Fatal(err)
	}
	if err := s1.MarkAlertsSent(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.LatestOrdinal() != 2 {
		t.Fatalf("reloaded latest = %d, want 2", s2.LatestOrdinal())
	}
	rec, ok := s2.Get("b1")
	if !ok || !rec.AlertsSent || len(rec.Results) != 1 {
		t.Fatalf("reloaded b1 = %+v ok=%v", rec, ok)
	}
	byOrd, ok := s2.GetByOrdinal(2)
	if !ok || byOrd.BatchID != "b2" {
		t.Fatalf("get by ordinal 2 = %+v ok=%v", byOrd, ok)
	}
	// ordinal 1 已被 b1 占用：重开后抢占仍被拒绝。
	if err := s2.Commit(ctx, sampleRecord("b0", 1)); errs.CodeOf(err) != model.ReasonStateRollback {
		t.Fatalf("reloaded store rollback: want rollback, got %v", err)
	}
}
