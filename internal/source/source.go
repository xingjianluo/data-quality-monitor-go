// Package source 负责从本地数据文件加载数据集（JSON 对象数组）。
// 字段缺失与显式 null 可区分；缺失/不可读的数据源以 LoadFailure
// 形式返回，绝不静默跳过。
package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// LoadFailure 描述一个无法加载的数据集及归一化原因。
type LoadFailure struct {
	Dataset string
	Reason  model.ReasonCode
	Message string
}

// LoadResult 是一次批量加载的结果。
type LoadResult struct {
	Datasets map[string]*model.Dataset
	Failures []LoadFailure
}

// Loader 并发加载多个声明的数据集，资源占用由 concurrency 控制。
type Loader struct {
	concurrency int
}

// NewLoader 创建加载器（concurrency<=0 时取默认值 4）。
func NewLoader(concurrency int) *Loader {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Loader{concurrency: concurrency}
}

// Load 加载全部数据集；单个失败不影响其他数据集。
// 结果与失败列表均按声明顺序排列，保证确定性。
func (l *Loader) Load(ctx context.Context, decls []model.DatasetDecl) LoadResult {
	sem := make(chan struct{}, l.concurrency)
	var wg sync.WaitGroup
	results := make([]*model.Dataset, len(decls))
	failures := make([]*LoadFailure, len(decls))

	for i := range decls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			ds, err := loadOne(ctx, decls[i])
			if err != nil {
				failures[i] = &LoadFailure{
					Dataset: decls[i].Name,
					Reason:  model.ReasonSourceUnreadable,
					Message: err.Error(),
				}
				return
			}
			results[i] = ds
		}(i)
	}
	wg.Wait()

	out := LoadResult{Datasets: make(map[string]*model.Dataset, len(decls))}
	for _, r := range results {
		if r != nil {
			out.Datasets[r.Name] = r
		}
	}
	for _, f := range failures {
		if f != nil {
			out.Failures = append(out.Failures, *f)
		}
	}
	return out
}

// loadOne 读取并解析单个数据文件。
func loadOne(ctx context.Context, decl model.DatasetDecl) (*model.Dataset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(decl.Path)
	if err != nil {
		return nil, fmt.Errorf("dataset %q: read %s: %w", decl.Name, decl.Path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var rows []map[string]json.RawMessage
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("dataset %q: parse %s: %w", decl.Name, decl.Path, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("dataset %q: %s contains multiple JSON values", decl.Name, decl.Path)
	}

	records := make([]model.Record, 0, len(rows))
	for _, row := range rows {
		rec := make(model.Record, len(row))
		// 排序键，保证单条记录内的处理顺序确定。
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			rec[k] = parseCell(row[k])
		}
		records = append(records, rec)
	}
	return &model.Dataset{
		Name:    decl.Name,
		Schema:  decl.Schema(),
		Records: records,
	}, nil
}

// parseCell 解析单个 JSON 原始值为 Cell。
// 显式 null：Present=true 且 Raw=nil；键不存在时由缺失的 map 项表示。
func parseCell(raw json.RawMessage) model.Cell {
	cell := model.Cell{Present: true}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return cell
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		// 理论上不会发生（外层已整体解析）；保留原始文本。
		cell.Text = string(trimmed)
		return cell
	}
	cell.Raw = v
	switch t := v.(type) {
	case string:
		cell.Text = t
	case json.Number:
		cell.Text = t.String()
	case bool:
		if t {
			cell.Text = "true"
		} else {
			cell.Text = "false"
		}
	default:
		cell.Text = string(trimmed)
	}
	return cell
}
