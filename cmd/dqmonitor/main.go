// Command dqmonitor runs a local data quality batch from a declarative config.
//
// Usage:
//
//	dqmonitor run      --config config.json --batch-id 20260923-001 [--force] [--eval-time 2026-09-23T10:00:00Z]
//	dqmonitor replay   --state-dir .dq --batch-id 20260923-001
//	dqmonitor history  --state-dir .dq
//
// Exit code is 0 when every rule passed, 1 when at least one rule failed or
// errored, and 2 for usage/state-transition problems (e.g. rollback rejected).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// errQualityFailed signals that at least one rule failed/errored; it maps to
// exit code 1 without skipping deferred cleanup.
var errQualityFailed = errors.New("one or more rules failed or errored")

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "run":
		err = runCmd(args)
	case "replay":
		err = replayCmd(args)
	case "history":
		err = historyCmd(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		if errors.Is(err, errQualityFailed) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `dqmonitor - local data quality monitor

commands:
  run      evaluate a batch (commits state, emits alerts)
  replay   print stored conclusions of a committed batch
  history  list stored batch states

run flags: --config --batch-id [--force] [--eval-time RFC3339] [--state-dir] [--alert-file]
common:    --state-dir (default .dq-state)
`)
}

type commonFlags struct {
	stateDir  string
	alertFile string
	config    string
	batchID   string
	force     bool
	evalTime  string
}

func parseCommon(args []string, needConfig bool) (*flag.FlagSet, *commonFlags) {
	fs := flag.NewFlagSet("dqmonitor", flag.ContinueOnError)
	cf := &commonFlags{}
	fs.StringVar(&cf.stateDir, "state-dir", ".dq-state", "directory for durable state and journals")
	fs.StringVar(&cf.alertFile, "alert-file", "", "alert JSONL file (default <state-dir>/alerts.jsonl)")
	fs.StringVar(&cf.config, "config", "config.json", "path to declarative JSON config")
	fs.StringVar(&cf.batchID, "batch-id", "", "batch identifier (must be monotonically ordered)")
	fs.BoolVar(&cf.force, "force", false, "reclaim a stale running batch after an interrupted execution")
	fs.StringVar(&cf.evalTime, "eval-time", "", "deterministic evaluation time (RFC3339); defaults to now")
	_ = fs.Parse(args)
	return fs, cf
}

func runCmd(args []string) error {
	_, cf := parseCommon(args, true)
	if cf.batchID == "" {
		return errors.New("--batch-id is required")
	}
	evalTime := time.Now().UTC()
	if cf.evalTime != "" {
		t, err := time.Parse(time.RFC3339, cf.evalTime)
		if err != nil {
			return fmt.Errorf("parse --eval-time: %w", err)
		}
		evalTime = t
	}

	cfg, err := config.Load(cf.config)
	if err != nil {
		return err
	}
	store, err := state.NewFileStore(cf.stateDir)
	if err != nil {
		return err
	}
	defer store.Close()

	alertPath := cf.alertFile
	if alertPath == "" {
		alertPath = cf.stateDir + string(os.PathSeparator) + "alerts.jsonl"
	}
	sink, err := alert.NewFileSink(alertPath)
	if err != nil {
		return err
	}
	alerter, err := alert.NewAlerter(sink, cf.stateDir+string(os.PathSeparator)+"alert-journal.jsonl")
	if err != nil {
		return err
	}

	eng := engine.New(cfg, datasource.New(""), store, alerter, engine.Options{
		MaxConcurrentRules: engine.DefaultMaxConcurrentRules,
		RuleTimeout:        engine.DefaultRuleTimeout,
		EvalTime:           evalTime,
	})

	ctx, cancel := signalContext()
	defer cancel()
	res, err := eng.Run(ctx, cf.batchID, cf.force)
	if err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(raw))

	bad := 0
	for _, rr := range res.Results {
		if rr.Status != model.StatusPassed {
			bad++
		}
	}
	if bad > 0 && res.Status != "replay" {
		return errQualityFailed
	}
	return nil
}

func replayCmd(args []string) error {
	_, cf := parseCommon(args, false)
	if cf.batchID == "" {
		return errors.New("--batch-id is required")
	}
	store, err := state.NewFileStore(cf.stateDir)
	if err != nil {
		return err
	}
	defer store.Close()
	st, err := store.Get(context.Background(), cf.batchID)
	if err != nil {
		return err
	}
	if st.Status != state.StatusCommitted {
		return fmt.Errorf("batch %s is %s, not committed", cf.batchID, st.Status)
	}
	raw, _ := json.MarshalIndent(st, "", "  ")
	fmt.Println(string(raw))
	return nil
}

func historyCmd(args []string) error {
	_, cf := parseCommon(args, false)
	store, err := state.NewFileStore(cf.stateDir)
	if err != nil {
		return err
	}
	defer store.Close()
	list, err := store.List(context.Background())
	if err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(list, "", "  ")
	fmt.Println(string(raw))
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
