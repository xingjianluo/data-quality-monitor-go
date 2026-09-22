package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	statePath := fs.String("state", "dqm-state.json", "状态文件路径")
	batchID := fs.String("batch", "", "要复现的批次 ID")
	ordinal := fs.Uint64("ordinal", 0, "要复现的批次序号（与 --batch 二选一）")
	outPath := fs.String("out", "", "结果 JSON 输出路径（默认 stdout）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := state.OpenFileStore(*statePath)
	if err != nil {
		return errs.New(model.ReasonInternal, "%v", err)
	}
	defer store.Close()

	var rec state.BatchRecord
	var ok bool
	switch {
	case *batchID != "":
		rec, ok = store.Get(*batchID)
	case *ordinal != 0:
		rec, ok = store.GetByOrdinal(*ordinal)
	default:
		return errs.New(model.ReasonInputIncomplete, "one of --batch or --ordinal is required")
	}
	if !ok {
		return errs.New(model.ReasonInputIncomplete, "batch not found in state")
	}
	// 复现即从持久记录还原，不执行规则、不投递告警。
	return writeResult(batchResultFromRecord(rec), *outPath)
}

func cmdHistory(args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	statePath := fs.String("state", "dqm-state.json", "状态文件路径")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := state.OpenFileStore(*statePath)
	if err != nil {
		return errs.New(model.ReasonInternal, "%v", err)
	}
	defer store.Close()

	fmt.Printf("%-8s  %-24s  %-20s  %s\n", "ORDINAL", "BATCH_ID", "COMMITTED_AT", "STATUS")
	latest := store.LatestOrdinal()
	for n := uint64(1); n <= latest; n++ {
		rec, ok := store.GetByOrdinal(n)
		if !ok {
			continue
		}
		status := batchResultFromRecord(rec).Status()
		fmt.Printf("%-8d  %-24s  %-20s  %s\n",
			rec.Ordinal, rec.BatchID,
			time.UnixMilli(rec.CommittedMs).UTC().Format(time.RFC3339), status)
	}
	return nil
}

func batchResultFromRecord(rec state.BatchRecord) model.BatchResult {
	return model.BatchResult{
		BatchID:   rec.BatchID,
		Ordinal:   rec.Ordinal,
		Evaluated: time.UnixMilli(rec.WatermarkMs).UTC(),
		Inputs:    rec.Inputs,
		Results:   rec.Results,
	}
}

func writeResult(res model.BatchResult, outPath string) error {
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return errs.New(model.ReasonInternal, "encode result: %v", err)
	}
	raw = append(raw, '\n')
	if outPath == "" {
		_, err := os.Stdout.Write(raw)
		return err
	}
	if err := os.WriteFile(outPath, raw, 0o644); err != nil {
		return errs.New(model.ReasonInternal, "write result: %v", err)
	}
	return nil
}

var _ = engine.Config{}
