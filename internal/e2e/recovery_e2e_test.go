package engine_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/data-quality-monitor-go/internal/alert"
	"github.com/xingjianluo/data-quality-monitor-go/internal/config"
	"github.com/xingjianluo/data-quality-monitor-go/internal/datasource"
	"github.com/xingjianluo/data-quality-monitor-go/internal/engine"
	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
	"github.com/xingjianluo/data-quality-monitor-go/internal/state"
)

// idemChannel is an in-process stand-in for a real notification channel shared
// by several "processes". It suppresses redelivery of an already accepted
// DeliveryID (mirroring the built-in alert.FileSink's idempotence contract) and
// can be told to fail the next physical send a fixed number of times.
type idemChannel struct {
	mu          sync.Mutex
	accepted    map[string]alert.Alert
	order       []alert.Alert
	failNextN   int
	blockOnRule string        // block sends for this rule until releaseCh
	releaseCh   chan struct{} // closed to release blocked sends
	startedCh   chan string   // records rule ids when a blocked send starts
}

func newIdemChannel() *idemChannel {
	return &idemChannel{
		accepted:  map[string]alert.Alert{},
		releaseCh: make(chan struct{}),
		startedCh: make(chan string, 64),
	}
}

func (c *idemChannel) Send(ctx context.Context, a alert.Alert) error {
	_, err := c.SendToken(ctx, a)
	return err
}

func (c *idemChannel) SendToken(ctx context.Context, a alert.Alert) (bool, error) {
	if c.blockOnRule != "" && a.RuleID == c.blockOnRule {
		c.startedCh <- a.RuleID
		select {
		case <-c.releaseCh:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.accepted[a.DeliveryID]; ok {
		return false, nil // idempotent duplicate suppressed
	}
	if c.failNextN > 0 {
		c.failNextN--
		return false, os.ErrPermission
	}
	c.accepted[a.DeliveryID] = a
	c.order = append(c.order, a)
	return true, nil
}

func (c *idemChannel) physicalKeys() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, a := range c.order {
		out[a.BatchID+"\x00"+a.RuleID]++
	}
	return out
}

// newProcEngine builds an engine with its OWN FileStore/Alerter handles over
// the same durable directory — exactly what a separately launched OS process
// opens on restart/concurrent trigger.
func newProcEngine(t *testing.T, dir string, cfg *config.Config, evalTime time.Time, sink alert.Sink) *engine.Engine {
	t.Helper()
	st, err := state.NewFileStoreWithTTL(filepath.Join(dir, "state"), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	al, err := alert.NewAlerter(sink, filepath.Join(dir, "state", "alert-journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return engine.New(cfg, datasource.New(""), st, al, engine.Options{
		MaxConcurrentRules:  8,
		RuleTimeout:         5 * time.Second,
		EvalTime:            evalTime,
		RunningPollInterval: 5 * time.Millisecond,
	})
}

// countAlertFileLines counts physical alert records written to the JSONL sink.
func countAlertFileLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	return n
}

func alertFileKeys(t *testing.T, path string) map[string]int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a alert.Alert
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			continue
		}
		out[a.BatchID+"\x00"+a.RuleID]++
	}
	return out
}

// truncateJournalToPending keeps only pending journal records, simulating a
// process killed after sending an alert but before the delivered confirmation.
func truncateJournalToPending(t *testing.T, journal string) {
	t.Helper()
	raw, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	var kept [][]byte
	start := 0
	flush := func(line []byte) {
		var e map[string]any
		if json.Unmarshal(line, &e) == nil && e["status"] == "pending" {
			kept = append(kept, append(append([]byte(nil), line...), '\n'))
		}
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\n' {
			flush(raw[start:i])
			start = i + 1
		}
	}
	if start < len(raw) {
		flush(raw[start:])
	}
	if err := os.WriteFile(journal, concat(kept), 0o644); err != nil {
		t.Fatal(err)
	}
}

func concat(parts [][]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// TestCrossProcessSameBatchOneConclusionOneNotification launches many
// independent "processes" (separate store handles over one directory) that
// trigger the same batch at the same instant. Exactly one execution commits;
// the others wait and receive the same conclusions. Notifications: one per
// failing rule. Meanwhile a read-only history query in another handle never
// fails.
func TestCrossProcessSameBatchOneConclusionOneNotification(t *testing.T) {
	dir := copyExampleData(t)
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	evalTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	channel := newIdemChannel()
	const batch = "20260923-100"
	ctx := context.Background()

	const procs = 6
	var wg sync.WaitGroup
	res := make([]*model.BatchResult, procs)
	pErr := make([]error, procs)
	start := make(chan struct{})
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eng := newProcEngine(t, dir, cfg, evalTime, channel)
			<-start
			res[i], pErr[i] = eng.Run(ctx, batch, false)
		}(i)
	}

	// While everyone is racing, a further process only queries history and
	// must never be blocked into failure by the in-flight batch.
	queryOK := make(chan struct{})
	go func() {
		defer close(queryOK)
		ro, err := state.NewFileStore(filepath.Join(dir, "state"))
		if err != nil {
			t.Errorf("read-only store open: %v", err)
			return
		}
		defer ro.Close()
		// Poll until the batch is committed (query itself must never error with
		// busy/running).
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if list, err := ro.List(ctx); err == nil {
				for _, st := range list {
					if st.BatchID == batch && st.Status == state.StatusCommitted {
						return
					}
				}
			} else {
				t.Errorf("history query failed while batch running: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	close(start)
	wg.Wait()
	<-queryOK

	var fp string
	committed, replay := 0, 0
	for i := 0; i < procs; i++ {
		if pErr[i] != nil {
			t.Fatalf("proc %d: %v", i, pErr[i])
		}
		if res[i] == nil {
			t.Fatalf("proc %d nil result", i)
		}
		if i == 0 {
			fp = res[i].Fingerprint
		}
		if res[i].Fingerprint != fp {
			t.Fatalf("proc %d got divergent fingerprint", i)
		}
		switch res[i].Status {
		case "committed":
			committed++
		case "replay":
			replay++
		default:
			t.Fatalf("proc %d unexpected status %s", i, res[i].Status)
		}
	}
	if committed != 1 || replay != procs-1 {
		t.Fatalf("want exactly 1 committed and %d shared replays, got committed=%d replay=%d",
			procs-1, committed, replay)
	}

	// One physical notification per failing rule.
	keys := channel.physicalKeys()
	want := 0
	for _, r := range res[0].Results {
		if r.Status != model.StatusPassed {
			want++
			if keys[batch+"\x00"+r.RuleID] != 1 {
				t.Fatalf("alert %s physical count=%d, want 1", r.RuleID, keys[batch+"\x00"+r.RuleID])
			}
		}
	}
	if len(keys) != want {
		t.Fatalf("physical alerts=%d, want %d: %+v", len(keys), want, keys)
	}
}

// TestCrashAfterAlertThenRestartNoDuplicate runs a batch, simulates a process
// killed right after an alert was written but before its journal confirmation,
// then restarts a fresh process. The built-in file sink must suppress the
// duplicate via the stable DeliveryID; nothing is sent twice and the final
// report shows the alert reconciled with zero pending.
func TestCrashAfterAlertThenRestartNoDuplicate(t *testing.T) {
	dir := copyExampleData(t)
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	evalTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	const batch = "20260923-200"
	alertFile := filepath.Join(dir, "state", "alerts.jsonl")
	journal := filepath.Join(dir, "state", "alert-journal.jsonl")
	ctx := context.Background()

	sink, err := alert.NewFileSink(alertFile)
	if err != nil {
		t.Fatal(err)
	}
	eng := newProcEngine(t, dir, cfg, evalTime, sink)
	res, err := eng.Run(ctx, batch, false)
	if err != nil || res.Status != "committed" {
		t.Fatalf("initial run: %v status=%v", err, res.Status)
	}
	wantAlerts := 0
	for _, r := range res.Results {
		if r.Status != model.StatusPassed {
			wantAlerts++
		}
	}
	if wantAlerts == 0 {
		t.Fatal("test data should produce failing rules")
	}
	if before := countAlertFileLines(t, alertFile); before != wantAlerts {
		t.Fatalf("initial physical alerts=%d want %d", before, wantAlerts)
	}

	// Simulate the crash window: keep only durable pending intents.
	truncateJournalToPending(t, journal)

	// Restart as a new process (fresh handles) using the SAME sink file.
	sink2, err := alert.NewFileSink(alertFile)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := newProcEngine(t, dir, cfg, evalTime, sink2)
	res2, err := eng2.Run(ctx, batch, false)
	if err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	if res2.Status != "replay" {
		t.Fatalf("restart status=%s want replay", res2.Status)
	}
	// The reconciliation confirmed the recovered intents; nothing remains
	// pending and nothing was physically written again.
	if res2.AlertReport.Pending != 0 {
		t.Fatalf("pending after recovery: %+v", res2.AlertReport)
	}
	if after := countAlertFileLines(t, alertFile); after != wantAlerts {
		t.Fatalf("physical alerts after crash recovery=%d, want %d (no duplicates)", after, wantAlerts)
	}
	for k, n := range alertFileKeys(t, alertFile) {
		if n != 1 {
			t.Fatalf("alert %q physically written %d times", k, n)
		}
	}

	// A third, clean process reports purely already-delivered.
	sink3, _ := alert.NewFileSink(alertFile)
	eng3 := newProcEngine(t, dir, cfg, evalTime, sink3)
	res3, err := eng3.Run(ctx, batch, false)
	if err != nil || res3.AlertReport.SentThisRun != 0 || res3.AlertReport.Pending != 0 {
		t.Fatalf("clean process report: %+v err=%v", res3.AlertReport, err)
	}
}

// TestDeliveryFailureThenBackfillAcrossProcesses: the first process's channel
// fails several physical sends (pending intents stay durable); subsequent
// process triggers of the same batch backfill them, reporting how many were
// sent now and how many remain, until pending reaches zero — with no
// duplicates.
func TestDeliveryFailureThenBackfillAcrossProcesses(t *testing.T) {
	dir := copyExampleData(t)
	cfg, _ := config.Load(filepath.Join(dir, "config.json"))
	evalTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	const batch = "20260923-300"
	ctx := context.Background()

	channel := newIdemChannel()
	// Fail the first 2 physical sends; rule order is by rule id so exactly two
	// alerts remain pending after the first process.
	channel.failNextN = 2

	eng1 := newProcEngine(t, dir, cfg, evalTime, channel)
	res1, err := eng1.Run(ctx, batch, false)
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	total := res1.AlertReport.Expected
	if total < 2 || res1.AlertReport.Pending != 2 || res1.AlertReport.SentThisRun != total-2 {
		t.Fatalf("first process report: %+v total=%d", res1.AlertReport, total)
	}

	// Second process (fresh handle), same shared channel now healthy: it must
	// backfill exactly the 2 pending alerts.
	eng2 := newProcEngine(t, dir, cfg, evalTime, channel)
	res2, err := eng2.Run(ctx, batch, false)
	if err != nil {
		t.Fatalf("second process: %v", err)
	}
	if res2.AlertReport.SentThisRun != 2 || res2.AlertReport.Pending != 0 ||
		res2.AlertReport.AlreadyDelivered != total-2 {
		t.Fatalf("backfill report must show 2 retried, 0 pending: %+v", res2.AlertReport)
	}

	// Exactly one physical delivery per failing rule across both processes.
	keys := channel.physicalKeys()
	if len(keys) != total {
		t.Fatalf("physical distinct alerts=%d want %d", len(keys), total)
	}
	for _, r := range res1.Results {
		if r.Status == model.StatusPassed {
			continue
		}
		if keys[batch+"\x00"+r.RuleID] != 1 {
			t.Fatalf("alert %s delivered %d times", r.RuleID, keys[batch+"\x00"+r.RuleID])
		}
	}

	// Third process sees everything already delivered, sends nothing.
	eng3 := newProcEngine(t, dir, cfg, evalTime, channel)
	res3, err := eng3.Run(ctx, batch, false)
	if err != nil || res3.AlertReport.SentThisRun != 0 || res3.AlertReport.Pending != 0 ||
		res3.AlertReport.AlreadyDelivered != total {
		t.Fatalf("third process report: %+v err=%v", res3.AlertReport, err)
	}
}

// TestUnrelatedBatchRunsWhileAnotherInFlight proves a different batch is fully
// servable while a long-running batch is mid-flight in another process.
func TestUnrelatedBatchRunsWhileAnotherInFlight(t *testing.T) {
	dir := copyExampleData(t)
	cfg, _ := config.Load(filepath.Join(dir, "config.json"))
	evalTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Blocking channel for the in-flight batch A.
	channelA := newIdemChannel()
	channelA.blockOnRule = "events-ts-fresh"

	engA := newProcEngine(t, dir, cfg, evalTime, channelA)
	doneA := make(chan struct{})
	var resA *model.BatchResult
	go func() {
		defer close(doneA)
		var err error
		resA, err = engA.Run(ctx, "20260923-400", false)
		if err != nil {
			t.Errorf("batch A: %v", err)
		}
	}()
	select {
	case <-channelA.startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("batch A never reached the blocked alert")
	}

	// A separate process fully runs a DIFFERENT batch while A is still blocked.
	channelB := newIdemChannel()
	engB := newProcEngine(t, dir, cfg, evalTime, channelB)
	resB, err := engB.Run(ctx, "20260924-401", false)
	if err != nil {
		t.Fatalf("unrelated batch B must complete while A is in flight: %v", err)
	}
	if resB.Status != "committed" {
		t.Fatalf("batch B status=%s", resB.Status)
	}

	// Read-only history also works mid-flight.
	ro, err := state.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	list, err := ro.List(ctx)
	_ = ro.Close()
	if err != nil {
		t.Fatalf("history while A in flight: %v", err)
	}
	foundB := false
	for _, st := range list {
		if st.BatchID == "20260924-401" && st.Status == state.StatusCommitted {
			foundB = true
		}
	}
	if !foundB {
		t.Fatal("batch B not visible in history while A in flight")
	}

	// Release A and let it finish; both histories coexist independently.
	close(channelA.releaseCh)
	<-doneA
	if resA.Status != "committed" {
		t.Fatalf("batch A final status=%s", resA.Status)
	}
	if resA.Fingerprint == resB.Fingerprint {
		t.Fatal("batches A and B must have independent conclusions")
	}
}
