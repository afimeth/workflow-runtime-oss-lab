package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func journal(t *testing.T) *Journal {
	t.Helper()
	j, e := OpenJournal(filepath.Join(t.TempDir(), "journal.json"))
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func node(id string, deps ...string) Node { return Node{ID: id, Deps: deps, Adapter: "test"} }
func TestDAGOrder(t *testing.T) {
	j := journal(t)
	var calls []string
	a := map[string]Adapter{"test": func(_ context.Context, s string) (string, error) { calls = append(calls, s); return s, nil }}
	nodes := []Node{node("b", "a"), node("a")}
	nodes[0].Input = "b"
	nodes[1].Input = "a"
	if e := Run(context.Background(), nodes, a, j, 2); e != nil {
		t.Fatal(e)
	}
	if len(calls) != 2 || calls[0] != "a" || calls[1] != "b" {
		t.Fatal(calls)
	}
}
func TestRejectCycle(t *testing.T) {
	if e := validate([]Node{node("a", "b"), node("b", "a")}, map[string]Adapter{"test": func(context.Context, string) (string, error) { return "", nil }}); e == nil {
		t.Fatal("cycle accepted")
	}
}
func TestRejectMissingDependency(t *testing.T) {
	if e := validate([]Node{node("a", "missing")}, map[string]Adapter{"test": func(context.Context, string) (string, error) { return "", nil }}); e == nil {
		t.Fatal("missing accepted")
	}
}
func TestRejectDuplicate(t *testing.T) {
	if e := validate([]Node{node("a"), node("a")}, map[string]Adapter{"test": func(context.Context, string) (string, error) { return "", nil }}); e == nil {
		t.Fatal("duplicate accepted")
	}
}
func TestUnknownAdapter(t *testing.T) {
	if e := validate([]Node{node("a")}, nil); e == nil {
		t.Fatal("unknown accepted")
	}
}
func TestBoundedConcurrency(t *testing.T) {
	j := journal(t)
	var active, peak atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return "", nil
	}}
	done := make(chan error)
	go func() { done <- Run(context.Background(), []Node{node("a"), node("b"), node("c"), node("d")}, a, j, 2) }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if peak.Load() != 2 {
		t.Fatal(peak.Load())
	}
}
func TestExplicitSafeRetry(t *testing.T) {
	j := journal(t)
	n := node("a")
	n.Retries = 1
	calls := 0
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) {
		calls++
		if calls == 1 {
			return "", Retryable{"no effect"}
		}
		return "ok", nil
	}}
	if e := Run(context.Background(), []Node{n}, a, j, 1); e != nil {
		t.Fatal(e)
	}
	if j.get("a").Attempts != 2 || j.get("a").State != "DONE" {
		t.Fatal(j.get("a"))
	}
}
func TestAmbiguousFailureNeverRetries(t *testing.T) {
	j := journal(t)
	n := node("a")
	n.Retries = 5
	calls := 0
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) {
		calls++
		return "", errors.New("effect outcome unknown")
	}}
	Run(context.Background(), []Node{n, node("b", "a")}, a, j, 1)
	if calls != 1 || j.get("a").State != "HOLD" || j.get("b").State != "HOLD" {
		t.Fatal(calls, j.Records)
	}
}
func TestRetryExhaustion(t *testing.T) {
	j := journal(t)
	n := node("a")
	n.Retries = 1
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) { return "", Retryable{"safe"} }}
	Run(context.Background(), []Node{n}, a, j, 1)
	if j.get("a").State != "FAILED" || j.get("a").Attempts != 2 {
		t.Fatal(j.Records)
	}
}
func TestInterruptedRunningBecomesHold(t *testing.T) {
	j := journal(t)
	j.set("a", Record{State: "RUNNING", Attempts: 1})
	reopened, e := OpenJournal(j.Path)
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) { calls++; return "", nil }}
	Run(context.Background(), []Node{node("a")}, a, reopened, 1)
	if calls != 0 || reopened.get("a").State != "HOLD" {
		t.Fatal("replayed")
	}
}
func TestCompletedNotRepeated(t *testing.T) {
	j := journal(t)
	calls := 0
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) { calls++; return "", nil }}
	Run(context.Background(), []Node{node("a")}, a, j, 1)
	j, _ = OpenJournal(j.Path)
	Run(context.Background(), []Node{node("a")}, a, j, 1)
	if calls != 1 {
		t.Fatal(calls)
	}
}
func TestCancellationBeforeStart(t *testing.T) {
	j := journal(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := map[string]Adapter{"test": func(context.Context, string) (string, error) { t.Fatal("called"); return "", nil }}
	if e := Run(ctx, []Node{node("a")}, a, j, 1); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if j.get("a").State != "CANCELLED" {
		t.Fatal(j.Records)
	}
}
func TestCancellationDuringAdapterHolds(t *testing.T) {
	j := journal(t)
	ctx, cancel := context.WithCancel(context.Background())
	a := map[string]Adapter{"test": func(ctx context.Context, _ string) (string, error) { cancel(); <-ctx.Done(); return "", ctx.Err() }}
	Run(ctx, []Node{node("a")}, a, j, 1)
	if j.get("a").State != "HOLD" {
		t.Fatal(j.Records)
	}
}
func TestCorruptJournalFailsClosed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.json")
	os.WriteFile(p, []byte("bad"), 0600)
	if _, e := OpenJournal(p); e == nil {
		t.Fatal("accepted corruption")
	}
}
func TestNullJournalFailsClosed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.json")
	os.WriteFile(p, []byte("null"), 0600)
	if _, e := OpenJournal(p); e == nil {
		t.Fatal("null accepted")
	}
}
func TestInvalidLimit(t *testing.T) {
	if e := Run(context.Background(), nil, nil, journal(t), 0); e == nil {
		t.Fatal("accepted zero")
	}
}
func TestSpecMismatch(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "workflow.json")
	j := filepath.Join(dir, "j.json")
	os.WriteFile(p, []byte(`[{"id":"a","adapter":"uppercase","input":"one"}]`), 0600)
	if e := execute(p, j); e != nil {
		t.Fatal(e)
	}
	os.WriteFile(p, []byte(`[{"id":"a","adapter":"uppercase","input":"two"}]`), 0600)
	if e := execute(p, j); e == nil {
		t.Fatal("different spec replayed")
	}
}
func BenchmarkDAG(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j, _ := OpenJournal(filepath.Join(b.TempDir(), "j.json"))
		a := map[string]Adapter{"test": func(context.Context, string) (string, error) { return "ok", nil }}
		if e := Run(context.Background(), []Node{node("a"), node("b", "a"), node("c", "b")}, a, j, 2); e != nil {
			b.Fatal(e)
		}
	}
}
