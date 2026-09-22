// Package plan 负责加载声明式批次计划（JSON）：
// 数据集声明（形态）与规则声明，base_dir 用于解析相对数据路径。
package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Plan 是一个批次计划文件的声明内容。
type Plan struct {
	BatchID   string              `json:"batch_id"`
	Ordinal   uint64              `json:"ordinal"`
	Watermark string              `json:"watermark,omitempty"` // RFC3339，可选；留空则首次运行取当前时间
	BaseDir   string              `json:"base_dir,omitempty"`  // 数据文件相对路径基准（默认相对计划文件）
	Datasets  []model.DatasetDecl `json:"datasets"`
	Rules     []model.RuleSpec    `json:"rules"`
}

// LoadFile 从 JSON 文件加载计划，并把数据路径解析为绝对路径。
func LoadFile(path string) (Plan, error) {
	var p Plan
	raw, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, fmt.Errorf("read plan %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Plan{}, fmt.Errorf("parse plan %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	base := p.BaseDir
	if base == "" {
		base = dir
	} else if !filepath.IsAbs(base) {
		base = filepath.Join(dir, base)
	}
	for i := range p.Datasets {
		if !filepath.IsAbs(p.Datasets[i].Path) {
			p.Datasets[i].Path = filepath.Join(base, p.Datasets[i].Path)
		}
	}
	return p, nil
}

// ToRunRequest 转为引擎运行请求（watermark 为零值时由引擎填充）。
func (p Plan) ToRunRequest() (engine.RunRequest, error) {
	req := engine.RunRequest{
		BatchID:  p.BatchID,
		Ordinal:  p.Ordinal,
		Datasets: p.Datasets,
		Rules:    p.Rules,
	}
	if p.Watermark != "" {
		ts, err := parseWatermark(p.Watermark)
		if err != nil {
			return engine.RunRequest{}, err
		}
		req.Watermark = ts
	}
	return req, nil
}
