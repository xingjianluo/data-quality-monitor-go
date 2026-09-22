// Command dqm 是本地数据质量校验与监控的命令行入口。
//
// 子命令：
//
//	dqm run      --plan plan.json --state state.json   执行（或幂等重放）一个批次
//	dqm replay   --state state.json --batch BATCH_ID   按批次复现历史结论
//	dqm history  --state state.json                    列出已提交批次
//
// 退出码：0 整批通过；3 存在 fail/error 结论；2 基础设施类错误（状态拒绝/IO 等）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/errs"
	"github.com/xingjianluo/data-quality-monitor-go/internal/logx"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/monitor"
	"github.com/xingjianluo/data-quality-monitor-go/internal/plan"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

const (
	exitOK      = 0
	exitInfra   = 2
	exitQuality = 3
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return exitInfra
	}
	var err error
	switch args[0] {
	case "run":
		err = cmdRun(args[1:])
	case "replay":
		err = cmdReplay(args[1:])
	case "history":
		err = cmdHistory(args[1:])
	case "-h", "--help", "help":
		usage()
		return exitOK
	default:
		usage()
		return exitInfra
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error (%s): %v\n", errs.CodeOf(err), err)
		return exitInfra
	}
	return exitOK
}

func usage() {
	fmt.Fprint(os.Stderr, `dqm - 本地数据质量校验与监控

用法:
  dqm run --plan PLAN.json --state STATE.json [--concurrency N] [--rule-timeout DUR] [--out RESULT.json] [-v]
  dqm replay --state STATE.json (--batch BATCH_ID | --ordinal N)
  dqm history --state STATE.json

退出码: 0=通过 3=存在质量/结构问题 2=基础设施错误
`)
}

// rootFlags 是 run 子命令的参数。
type rootFlags struct {
	planPath    string
	statePath   string
	concurrency int
	ruleTimeout time.Duration
	outPath     string
	verbose     bool
}

func bindRunFlags(fs *flag.FlagSet) *rootFlags {
	rf := &rootFlags{}
	fs.StringVar(&rf.planPath, "plan", "", "批次计划文件（JSON）")
	fs.StringVar(&rf.statePath, "state", "dqm-state.json", "状态文件路径")
	fs.IntVar(&rf.concurrency, "concurrency", 4, "最大并发度（加载与规则判定共享）")
	fs.DurationVar(&rf.ruleTimeout, "rule-timeout", 2*time.Second, "单条规则超时时间（0 表示不限制）")
	fs.StringVar(&rf.outPath, "out", "", "批次结果 JSON 输出路径（默认输出到 stdout）")
	fs.BoolVar(&rf.verbose, "v", false, "输出调试日志")
	return rf
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	rf := bindRunFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if rf.planPath == "" {
		return errs.New(model.ReasonInputIncomplete, "--plan is required")
	}

	p, err := plan.LoadFile(rf.planPath)
	if err != nil {
		return errs.New(model.ReasonInputIncomplete, "%v", err)
	}
	req, err := p.ToRunRequest()
	if err != nil {
		return errs.New(model.ReasonInputIncomplete, "%v", err)
	}

	store, err := state.OpenFileStore(rf.statePath)
	if err != nil {
		return errs.New(model.ReasonInternal, "%v", err)
	}
	defer store.Close()

	logger := logx.New(rf.verbose)
	ctx, cancel := signalContext()
	defer cancel()

	eng := engine.New(store, monitor.New(logger), monitor.NewLogSink(logger), logger, engine.Config{
		Concurrency:    rf.concurrency,
		PerRuleTimeout: rf.ruleTimeout,
	})
	res, replayed, err := eng.Run(ctx, req)
	if err != nil {
		return err
	}
	if replayed {
		logger.InfoContext(ctx, "result served from existing state (no duplicate alerts)",
			"batch_id", res.BatchID)
	}
	if err := writeResult(res, rf.outPath); err != nil {
		return err
	}
	if res.Status() != model.StatusPass {
		// 质量/结构问题不是基础设施错误：用退出码区分，同时结果已正常输出。
		fmt.Fprintf(os.Stderr, "batch %s overall status: %s\n", res.BatchID, res.Status())
		os.Exit(exitQuality)
	}
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
