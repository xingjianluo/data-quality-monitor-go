// Package state 实现增量运行的批次状态留存：
// 同批次重跑幂等、历史结论可按批次复现、批次序号只进不退（回退明确拒绝）。
// 状态文件采用临时文件 + 原子重命名写入。
package state

import (
	"context"
	"reflect"

	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// BatchRecord 是一个已提交批次的持久记录。
type BatchRecord struct {
	BatchID     string               `json:"batch_id"`
	Ordinal     uint64               `json:"ordinal"`
	WatermarkMs int64                `json:"watermark_ms"`
	CommittedMs int64                `json:"committed_ms"`
	Inputs      []model.InputSummary `json:"inputs"`
	Rules       []model.RuleSpec     `json:"rules"`
	Results     []model.RuleResult   `json:"results"`
	Schemas     map[string]string    `json:"schemas"`
	// ObservedSchemas 是数据中实际出现列的指纹，用于跨批次形态变化基线。
	ObservedSchemas map[string]string `json:"observed_schemas,omitempty"`
	AlertsSent      bool              `json:"alerts_sent"`
}

// Store 是批次状态存储。
type Store interface {
	// LatestOrdinal 返回已提交的最大批次序号，无记录时为 0。
	LatestOrdinal() uint64
	// Get 按批次 ID 取历史记录。
	Get(batchID string) (BatchRecord, bool)
	// GetByOrdinal 按批次序号取历史记录。
	GetByOrdinal(ordinal uint64) (BatchRecord, bool)
	// Commit 原子提交一个批次：
	//   - 相同 batchID 且内容一致：幂等成功；
	//   - 相同 batchID 但内容不同：state_conflict；
	//   - ordinal 小于已提交最大序号：state_rollback，明确拒绝；
	//   - ordinal 等于最大序号但 batchID 不同：state_conflict。
	Commit(ctx context.Context, rec BatchRecord) error
	// MarkAlertsSent 原子标记某批次告警已投递（重复调用幂等）。
	MarkAlertsSent(ctx context.Context, batchID string) error
	// Close 释放底层资源。
	Close() error
}

// equivalent 判断两条记录的业务内容是否一致（幂等重放依据），
// 忽略提交时间戳与告警投递标记。
func equivalent(a, b BatchRecord) bool {
	return a.BatchID == b.BatchID &&
		a.Ordinal == b.Ordinal &&
		a.WatermarkMs == b.WatermarkMs &&
		reflect.DeepEqual(a.Inputs, b.Inputs) &&
		reflect.DeepEqual(a.Rules, b.Rules) &&
		reflect.DeepEqual(a.Results, b.Results) &&
		reflect.DeepEqual(a.Schemas, b.Schemas) &&
		reflect.DeepEqual(a.ObservedSchemas, b.ObservedSchemas)
}

// applyCommit 是各 Store 实现共享的提交判定逻辑，返回写入后的完整记录列表。
// 调用方需自行保证互斥与持久化。
//
// 语义：
//   - 相同 batchID 且内容一致：幂等成功（重复执行/失败后重跑）；
//   - 相同 batchID 但内容不同：state_conflict；
//   - 新批次的序号尚未被任何批次占用：接受，即使小于最新序号（乱序/迟到到达，不漏报）；
//   - 序号已被另一个批次占用：state_rollback，明确拒绝（不允许覆盖/从中间继续）。
func applyCommit(existing []BatchRecord, rec BatchRecord) ([]BatchRecord, error) {
	if existingByID, ok := findByID(existing, rec.BatchID); ok {
		if equivalent(existingByID, rec) {
			return existing, nil // 幂等：同一批次相同结论
		}
		return nil, errs.New(model.ReasonStateConflict,
			"batch %q already committed with different content; re-evaluation must be identical", rec.BatchID)
	}
	if occupied, ok := findByOrdinal(existing, rec.Ordinal); ok {
		return nil, errs.New(model.ReasonStateRollback,
			"ordinal %d is already occupied by batch %q; rollback is rejected",
			rec.Ordinal, occupied.BatchID)
	}
	out := append(append([]BatchRecord(nil), existing...), rec)
	sortRecords(out)
	return out, nil
}

func findByOrdinal(records []BatchRecord, ordinal uint64) (BatchRecord, bool) {
	for _, r := range records {
		if r.Ordinal == ordinal {
			return r, true
		}
	}
	return BatchRecord{}, false
}

func findByID(records []BatchRecord, batchID string) (BatchRecord, bool) {
	for _, r := range records {
		if r.BatchID == batchID {
			return r, true
		}
	}
	return BatchRecord{}, false
}

func sortRecords(records []BatchRecord) {
	// 插入数量小，简单实现；按 ordinal 升序。
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && records[j-1].Ordinal > records[j].Ordinal; j-- {
			records[j-1], records[j] = records[j], records[j-1]
		}
	}
}

func errUnknownBatch(batchID string) error {
	return errs.New(model.ReasonStateConflict, "unknown batch %q", batchID)
}
