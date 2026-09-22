// Package logx 提供统一日志：每次运行打印输入摘要与判定依据。
package logx

import (
	"context"
	"log/slog"
	"os"
)

// Logger 是最小日志接口（与 slog 方法集对齐）。
type Logger interface {
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// New 返回标准库 slog 实现的结构化日志器。
// verbose=false 时只输出 INFO 及以上；true 时输出 DEBUG 及以上。
func New(verbose bool) Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(handler)
}
