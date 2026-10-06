package engine_test

// Cross-process end-to-end tests: real child OS processes (the test binary
// re-executed with DQ_E2E_HELPER=1) share one state directory and one alert
// sink file, proving that
//
//   - two processes triggering the same batch produce exactly one conclusion
//     and one notification per failed rule;
//   - a running batch blocks neither other batches nor read-only history;
//   - a process killed mid-dispatch (after a send succeeded but before the
//     journal write) never causes a duplicate after restart, and the alerts
//     it never sent are backfilled automatically with an accurate report.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

var helperEvalTime = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

// helperRunOut is what a "run" child writes to DQ_E2E_OUT.
type helperRunOut struct {
	Status      string            `json:"status"`
	Fingerprint string            `json:"fingerprint"`
	AlertReport model.AlertReport `json:"alert_report"`
	Error       string            `json:"error,omitempty"`
}

// suicideSink delivers through a FileSink but kills the process right after
// the first successful physical send — before the alerter can journal it.
// This reproduces "notification sent, process interrupted during wrap-up".
type suicideSink struct{ inner *alert.FileSink }

func (s *suicideSink) Send(ctx context.Context, a alert.Alert) error {
	if err := s.inner.Send(ctx, a); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "helper: delivered %s/%s, simulating crash before journal write\n", a.BatchID, a.RuleID)
	os.Exit(3)
	return nil
}

func (s *suicideSink) Delivered(ctx context.Context) (map[string]bool, error) {
	return s.inner.Delivered(ctx)
}

// failSink fails its first failN sends (transient channel outage), then
// delegates to the real FileSink.
type failSink struct {
	inner *alert.FileSink
	failN int
	calls int
}

func (s *failSink) Send(ctx context.Context, a alert.Alert) error {
	s.calls++
	if s.calls <= s.failN {
		return errors.New("simulated transient channel outage")
	}
	return s.inner.Send(ctx, a)
}

func (s *failSink) Delivered(ctx context.Context) (map[string]bool, error) {
	return s.inner.Delivered(ctx)
}

// TestCrossProcessHelper is the child-process entry point. The parent runs
// the test binary with DQ_E2E_HELPER=1; the child executes the requested
// mode and exits without touching the testing framework's expectations.
func TestCrossProcessHelper(t *testing.T) {
	if os.Getenv("DQ_E2E_HELPER") != "1" {
		return
	}
	dir := os.Getenv("DQ_E2E_DIR")
	mode := os.Getenv("DQ_E2E_MODE")
	batchID := os.Getenv("DQ_E2E_BATCH")
	outPath := os.Getenv("DQ_E2E_OUT")

	fatal := func(err error) {
		fmt.Fprintln(os.Stderr, "helper:", err)
		os.Exit(2)
	}

	switch mode {
	case "hold":
		// Acquire the batch lease, signal readiness, then keep running (the
		// lease stays live) until the parent lets the process exit.
		store, err := state.NewFileStoreWithTTL(filepath.Join(dir, "state"), 30*time.Second)
		if err != nil {
			fatal(err)
		}
		st, replay, err := store.Begin(context.Background(), batchID, false)
		if err != nil || replay {
			fatal(fmt.Errorf("hold: begin: replay=%v err=%v", replay, err))
		}
		if err := os.WriteFile(outPath, []byte(st.LeaseToken), 0o644); err != nil {
			fatal(err)
		}
		hold := 5 * time.Second
		if ms := os.Getenv("DQ_E2E_HOLD_MS"); ms != "" {
			if d, err := time.ParseDuration(ms + "ms"); err == nil {
				hold = d
			}
		}
		time.Sleep(hold)
		os.Exit(0) // lease left behind, still "running" (live TTL)
	case "run":
		cfg, err := config.Load(filepath.Join(dir, "config.json"))
		if err != nil {
			fatal(err)
		}
		store, err := state.NewFileStoreWithTTL(filepath.Join(dir, "state"), 30*time.Second)
		if err != nil {
			fatal(err)
		}
		sink, err := alert.NewFileSink(filepath.Join(dir, "alerts.jsonl"))
		if err != nil {
			fatal(err)
		}
		var s alert.Sink = sink
		if os.Getenv("DQ_E2E_SUICIDE_AFTER_SEND") == "1" {
			s = &suicideSink{inner: sink}
		} else if n := os.Getenv("DQ_E2E_FAIL_SENDS"); n != "" {
			var failN int
			if _, err := fmt.Sscanf(n, "%d", &failN); err == nil && failN > 0 {
				s = &failSink{inner: sink, failN: failN}
			}
		}
		alerter, err := alert.NewAlerter(s, filepath.Join(dir, "state", "alert-journal.jsonl"))
		if err != nil {
			fatal(err)
		}
		eng := engine.New(cfg, datasource.New(""), store, alerter, engine.Options{
			MaxConcurrentRules:  8,
			RuleTimeout:         5 * time.Second,
			EvalTime:            helperEvalTime,
			RunningPollInterval: 20 * time.Millisecond,
		})
		res, err := eng.Run(context.Background(), batchID, false)
		out := helperRunOut{}
		if err != nil {
			out.Error = err.Error()
		} else {
			out.Status = res.Status
			out.Fingerprint = res.Fingerprint
			out.AlertReport = res.AlertReport
		}
		raw, _ := json.Marshal(out)
		if werr := os.WriteFile(outPath, raw, 0o644); werr != nil {
			fatal(werr)
		}
		if err != nil {
			fatal(err)
		}
		os.Exit(0)
	default:
		fatal(fmt.Errorf("unknown helper mode %q", mode))
	}
}

// runHelper starts one child process and waits for it.
func runHelper(t *testing.T, env ...string) error {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrossProcessHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "DQ_E2E_HELPER=1")
	cmd.Env = append(cmd.Env, env...)
	cmd.Stderr = os.Stderr // child engine logs stay visible in the test output
	return cmd.Run()
}

func helperEnv(dir, mode, batch, out string) []string {
	return []string{
		"DQ_E2E_DIR=" + dir,
		"DQ_E2E_MODE=" + mode,
		"DQ_E2E_BATCH=" + batch,
		"DQ_E2E_OUT=" + out,
	}
}

func readRunOut(t *testing.T, path string) helperRunOut {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read child output: %v", err)
	}
	var out helperRunOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse child output: %v", err)
	}
	return out
}

// alertLineCount parses the shared sink file and returns per-(batch,rule)
// delivery counts.
func alertLineCount(t *testing.T, path string) map[string]int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read alerts file: %v", err)
	}
	out := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var a alert.Alert
		if err := json.Unmarshal([]byte(line), &a); err != nil {
			t.Fatalf("corrupt alert line %q: %v", line, err)
		}
		out[a.BatchID+"/"+a.RuleID]++
	}
	return out
}

// TestCrossProcessSameBatchSingleExecution fires two real processes at the
// same batch simultaneously: exactly one conclusion is committed, both
// processes observe the same fingerprint, and every failed rule is notified
// exactly once across both processes.
func TestCrossProcessSameBatchSingleExecution(t *testing.T) {
	dir := copyExampleData(t)
	out1 := filepath.Join(dir, "out1.json")
	out2 := filepath.Join(dir, "out2.json")

	errCh := make(chan error, 2)
	go func() { errCh <- runHelper(t, helperEnv(dir, "run", "xp-001", out1)...) }()
	go func() { errCh <- runHelper(t, helperEnv(dir, "run", "xp-001", out2)...) }()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("child %d: %v", i, err)
		}
	}

	r1, r2 := readRunOut(t, out1), readRunOut(t, out2)
	if r1.Error != "" || r2.Error != "" {
		t.Fatalf("child errors: %q / %q", r1.Error, r2.Error)
	}
	if r1.Fingerprint == "" || r1.Fingerprint != r2.Fingerprint {
		t.Fatalf("both processes must observe one identical conclusion: %q vs %q", r1.Fingerprint, r2.Fingerprint)
	}
	statuses := map[string]bool{r1.Status: true, r2.Status: true}
	if !statuses["committed"] && !statuses["replay"] {
		t.Fatalf("unexpected statuses: %s / %s", r1.Status, r2.Status)
	}
	// Exactly 5 failed/errored rules in the example config; across both
	// processes exactly 5 physical sends must have happened.
	if sent := r1.AlertReport.SentThisRun + r2.AlertReport.SentThisRun; sent != 5 {
		t.Fatalf("total sends across processes = %d, want 5 (%+v / %+v)", sent, r1.AlertReport, r2.AlertReport)
	}
	counts := alertLineCount(t, filepath.Join(dir, "alerts.jsonl"))
	if len(counts) != 5 {
		t.Fatalf("want 5 distinct delivered alerts, got %v", counts)
	}
	for k, n := range counts {
		if n != 1 {
			t.Fatalf("alert %s delivered %d times across processes", k, n)
		}
		if !strings.HasPrefix(k, "xp-001/") {
			t.Fatalf("alert for unexpected batch: %s", k)
		}
	}

	// The batch was committed exactly once; a third process replays it.
	st, err := mustStore(t, dir).Get(context.Background(), "xp-001")
	if err != nil || st.Status != state.StatusCommitted {
		t.Fatalf("batch must be committed exactly once: st=%+v err=%v", st, err)
	}
}

func mustStore(t *testing.T, dir string) state.Store {
	t.Helper()
	s, err := state.NewFileStoreWithTTL(filepath.Join(dir, "state"), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestCrossProcessUnrelatedWorkNotBlocked starts a child that holds one
// batch running; meanwhile another process must run a different batch to
// completion and serve read-only history queries.
func TestCrossProcessUnrelatedWorkNotBlocked(t *testing.T) {
	dir := copyExampleData(t)
	ready := filepath.Join(dir, "ready")

	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrossProcessHelper$")
	cmd.Env = append(os.Environ(), "DQ_E2E_HELPER=1", "DQ_E2E_HOLD_MS=3000")
	cmd.Env = append(cmd.Env, helperEnv(dir, "hold", "xp-slow", ready)...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Wait() }()

	// Wait until the child actually holds the lease.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never acquired the lease")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Same batch from this process: must be reported as running.
	store := mustStore(t, dir)
	if _, _, err := store.Begin(context.Background(), "xp-slow", false); !errors.Is(err, state.ErrBatchRunning) {
		t.Fatalf("same batch while child holds it: want ErrBatchRunning, got %v", err)
	}

	// A different batch runs to completion right now, in this process.
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := alert.NewFileSink(filepath.Join(dir, "alerts.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	alerter, err := alert.NewAlerter(sink, filepath.Join(dir, "state", "alert-journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(cfg, datasource.New(""), store, alerter, engine.Options{
		EvalTime:            helperEvalTime,
		RunningPollInterval: 20 * time.Millisecond,
	})
	res, err := eng.Run(context.Background(), "xp-fast", false)
	if err != nil {
		t.Fatalf("unrelated batch must run while xp-slow is held: %v", err)
	}
	if res.Status != "committed" {
		t.Fatalf("unrelated batch status=%s", res.Status)
	}

	// Read-only queries work while the child is still running its batch.
	if _, err := store.Get(context.Background(), "xp-slow"); err != nil {
		t.Fatalf("Get of the running batch must not be blocked: %v", err)
	}
	list, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List must not be blocked: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("want xp-slow + xp-fast in history, got %d entries", len(list))
	}
}

// TestCrossProcessCrashDuringDispatchResumes kills a child immediately after
// its first successful physical send (before the journal write). The restart
// must not re-send that alert and must backfill the remaining ones, with the
// report stating exactly how many were re-sent and how many remain.
func TestCrossProcessCrashDuringDispatchResumes(t *testing.T) {
	dir := copyExampleData(t)
	out1 := filepath.Join(dir, "out1.json")

	err := runHelper(t, append(helperEnv(dir, "run", "xp-crash", out1), "DQ_E2E_SUICIDE_AFTER_SEND=1")...)
	// The child suicided after the first send: exit code 3, no output file.
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected child crash (exit 3), got %v", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || ws.ExitStatus() != 3 {
		t.Fatalf("want simulated crash exit code 3, got %v", exitErr)
	}

	sinkPath := filepath.Join(dir, "alerts.jsonl")
	counts := alertLineCount(t, sinkPath)
	if len(counts) != 1 {
		t.Fatalf("exactly one alert must be physically out before the crash, got %v", counts)
	}
	// The journal never recorded it (process died first).
	journalPath := filepath.Join(dir, "state", "alert-journal.jsonl")
	if raw, _ := os.ReadFile(journalPath); len(strings.TrimSpace(string(raw))) != 0 {
		t.Fatalf("journal must be empty right after the crash, got %q", raw)
	}

	// Restart: the batch was already committed, so this is a replay that must
	// reconcile alerts: 1 already delivered (recognized via the sink record,
	// not the lost journal), 4 backfilled now, 0 pending.
	out2 := filepath.Join(dir, "out2.json")
	if err := runHelper(t, helperEnv(dir, "run", "xp-crash", out2)...); err != nil {
		t.Fatalf("restart run: %v", err)
	}
	r2 := readRunOut(t, out2)
	if r2.Error != "" {
		t.Fatalf("restart child error: %s", r2.Error)
	}
	if r2.Status != "replay" {
		t.Fatalf("restart must replay the committed batch, got %s", r2.Status)
	}
	rep := r2.AlertReport
	if rep.Expected != 5 || rep.AlreadyDelivered != 1 || rep.SentThisRun != 4 || rep.Pending != 0 {
		t.Fatalf("recovery report must show 1 kept + 4 backfilled + 0 pending: %+v", rep)
	}

	// Physical truth: every failed rule notified exactly once, no duplicates.
	counts = alertLineCount(t, sinkPath)
	if len(counts) != 5 {
		t.Fatalf("want 5 distinct alerts after recovery, got %v", counts)
	}
	for k, n := range counts {
		if n != 1 {
			t.Fatalf("alert %s delivered %d times after crash recovery", k, n)
		}
	}

	// A further replay is completely idle.
	out3 := filepath.Join(dir, "out3.json")
	if err := runHelper(t, helperEnv(dir, "run", "xp-crash", out3)...); err != nil {
		t.Fatalf("third run: %v", err)
	}
	r3 := readRunOut(t, out3)
	if r3.AlertReport.SentThisRun != 0 || r3.AlertReport.Pending != 0 || r3.AlertReport.AlreadyDelivered != 5 {
		t.Fatalf("idle replay expected, got %+v", r3.AlertReport)
	}
	if counts := alertLineCount(t, sinkPath); len(counts) != 5 {
		t.Fatalf("idle replay must not send anything: %v", counts)
	}
}

// TestCrossProcessLostJournalDoesNotDuplicate removes the journal after a
// completed run (the extreme of "delivered but not journaled"): the sink's
// own delivery record alone must prevent any duplicate on the next trigger.
func TestCrossProcessLostJournalDoesNotDuplicate(t *testing.T) {
	dir := copyExampleData(t)
	out1 := filepath.Join(dir, "out1.json")
	if err := runHelper(t, helperEnv(dir, "run", "xp-lost", out1)...); err != nil {
		t.Fatalf("first run: %v", err)
	}
	r1 := readRunOut(t, out1)
	if r1.AlertReport.SentThisRun != 5 || r1.AlertReport.Pending != 0 {
		t.Fatalf("first run report: %+v", r1.AlertReport)
	}

	if err := os.Remove(filepath.Join(dir, "state", "alert-journal.jsonl")); err != nil {
		t.Fatal(err)
	}

	out2 := filepath.Join(dir, "out2.json")
	if err := runHelper(t, helperEnv(dir, "run", "xp-lost", out2)...); err != nil {
		t.Fatalf("second run: %v", err)
	}
	r2 := readRunOut(t, out2)
	if r2.AlertReport.SentThisRun != 0 || r2.AlertReport.AlreadyDelivered != 5 || r2.AlertReport.Pending != 0 {
		t.Fatalf("lost journal must not cause resends: %+v", r2.AlertReport)
	}
	if counts := alertLineCount(t, filepath.Join(dir, "alerts.jsonl")); len(counts) != 5 {
		t.Fatalf("duplicates after journal loss: %v", counts)
	}
}

// TestCrossProcessFailureRetryThenConcurrentReplay combines the three
// behaviors in one flow:
//
//  1. process A runs a batch while its notification channel is down — the
//     batch commits, all 5 alerts stay pending;
//  2. processes B and C then trigger the same batch concurrently — together
//     they backfill exactly the 5 pending alerts, with no duplicates and no
//     second conclusion;
//  3. a later trigger is fully idle.
func TestCrossProcessFailureRetryThenConcurrentReplay(t *testing.T) {
	dir := copyExampleData(t)
	sinkPath := filepath.Join(dir, "alerts.jsonl")

	// 1. Channel down: everything pending, nothing delivered.
	outA := filepath.Join(dir, "outA.json")
	envA := append(helperEnv(dir, "run", "xp-retry", outA), "DQ_E2E_FAIL_SENDS=1000000")
	if err := runHelper(t, envA...); err != nil {
		t.Fatalf("run with failing channel: %v", err)
	}
	ra := readRunOut(t, outA)
	if ra.Status != "committed" {
		t.Fatalf("batch must still commit when the channel is down: %s", ra.Status)
	}
	if ra.AlertReport.Expected != 5 || ra.AlertReport.SentThisRun != 0 || ra.AlertReport.Pending != 5 {
		t.Fatalf("all-pending report expected, got %+v", ra.AlertReport)
	}
	if counts := alertLineCount(t, sinkPath); len(counts) != 0 {
		t.Fatalf("nothing may be delivered while the channel is down: %v", counts)
	}

	// 2. Two processes retry concurrently: exactly 5 physical sends total.
	outB, outC := filepath.Join(dir, "outB.json"), filepath.Join(dir, "outC.json")
	errCh := make(chan error, 2)
	go func() { errCh <- runHelper(t, helperEnv(dir, "run", "xp-retry", outB)...) }()
	go func() { errCh <- runHelper(t, helperEnv(dir, "run", "xp-retry", outC)...) }()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("retry child %d: %v", i, err)
		}
	}
	rb, rc := readRunOut(t, outB), readRunOut(t, outC)
	if rb.Fingerprint != ra.Fingerprint || rc.Fingerprint != ra.Fingerprint {
		t.Fatalf("retries must reproduce the same conclusion: %q / %q / %q",
			ra.Fingerprint, rb.Fingerprint, rc.Fingerprint)
	}
	if sent := rb.AlertReport.SentThisRun + rc.AlertReport.SentThisRun; sent != 5 {
		t.Fatalf("concurrent retries must backfill exactly 5 alerts, got %d (%+v / %+v)",
			sent, rb.AlertReport, rc.AlertReport)
	}
	counts := alertLineCount(t, sinkPath)
	if len(counts) != 5 {
		t.Fatalf("want 5 distinct alerts after concurrent retry, got %v", counts)
	}
	for k, n := range counts {
		if n != 1 {
			t.Fatalf("alert %s delivered %d times under concurrent retry", k, n)
		}
	}

	// 3. Idle afterwards.
	outD := filepath.Join(dir, "outD.json")
	if err := runHelper(t, helperEnv(dir, "run", "xp-retry", outD)...); err != nil {
		t.Fatalf("final run: %v", err)
	}
	rd := readRunOut(t, outD)
	if rd.AlertReport.SentThisRun != 0 || rd.AlertReport.Pending != 0 || rd.AlertReport.AlreadyDelivered != 5 {
		t.Fatalf("final run must be idle: %+v", rd.AlertReport)
	}
}
