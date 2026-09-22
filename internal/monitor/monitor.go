// Package monitor 将批次结论归一化为告警并投递：
// 每条 Finding 归一化为一条 Alert；按结果顺序生成，顺序稳定。
// 结合状态中的 alerts_sent 标记，常规重跑不重复告警。
package monitor

import (
	"context"

	"github.com/xingjianluo/data-quality-monitor-go/internal/logx"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// AlertSink 是告警投递目标。
type AlertSink interface {
	Send(ctx context.Context, alerts []model.Alert) error
}

// Monitor 负责从批次记录生成告警。
type Monitor struct {
	logger logx.Logger
}

// New 创建监控器。
func New(logger logx.Logger) *Monitor { return &Monitor{logger: logger} }

// Alerts 从批次记录归一化出告警（确定性、顺序稳定）：
// 仅 error/fail 结论产生告警；每条 Finding 一条，顺序与结果一致。
func (m *Monitor) Alerts(rec state.BatchRecord) []model.Alert {
	alerts := make([]model.Alert, 0)
	for _, r := range rec.Results {
		if r.Status == model.StatusPass {
			continue
		}
		for _, f := range r.Findings {
			alerts = append(alerts, model.Alert{
				BatchID:  rec.BatchID,
				Ordinal:  rec.Ordinal,
				RuleID:   r.RuleID,
				Dataset:  r.Dataset,
				Reason:   f.Reason,
				Severity: "error",
				Message:  f.Message,
			})
		}
	}
	return alerts
}

// LogSink 将告警写入结构化日志（默认本地投递目标）。
type LogSink struct {
	logger logx.Logger
}

// NewLogSink 创建日志告警目标。
func NewLogSink(logger logx.Logger) *LogSink { return &LogSink{logger: logger} }

func (s *LogSink) Send(ctx context.Context, alerts []model.Alert) error {
	for _, a := range alerts {
		s.logger.WarnContext(ctx, "alert",
			"batch_id", a.BatchID, "ordinal", a.Ordinal, "rule_id", a.RuleID,
			"dataset", a.Dataset, "reason", a.Reason, "severity", a.Severity, "message", a.Message)
	}
	return nil
}

// MemorySink 缓存告警，供测试断言不重复投递。并发安全。
type MemorySink struct {
	mu     chan struct{}
	Alerts []model.Alert
}

// NewMemorySink 创建内存告警目标。
func NewMemorySink() *MemorySink {
	s := &MemorySink{mu: make(chan struct{}, 1)}
	s.mu <- struct{}{}
	return s
}

func (s *MemorySink) Send(ctx context.Context, alerts []model.Alert) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	<-s.mu
	defer func() { s.mu <- struct{}{} }()
	s.Alerts = append(s.Alerts, alerts...)
	return nil
}

// Snapshot 返回已收到告警的副本。
func (s *MemorySink) Snapshot() []model.Alert {
	<-s.mu
	defer func() { s.mu <- struct{}{} }()
	out := append([]model.Alert(nil), s.Alerts...)
	return out
}
