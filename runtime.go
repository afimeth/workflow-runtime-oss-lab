package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Node struct {
	ID      string   `json:"id"`
	Deps    []string `json:"deps"`
	Adapter string   `json:"adapter"`
	Input   string   `json:"input"`
	Retries int      `json:"retries"`
}
type Record struct {
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
	Output   string `json:"output,omitempty"`
}
type Adapter func(context.Context, string) (string, error)

// Only an adapter-declared safe failure can be retried. Other errors hold for reconciliation.
type Retryable struct{ Cause string }

func (e Retryable) Error() string { return e.Cause }

type Journal struct {
	mu      sync.Mutex
	Path    string
	Records map[string]Record `json:"records"`
}

func OpenJournal(path string) (*Journal, error) {
	j := &Journal{Path: path, Records: map[string]Record{}}
	b, e := os.ReadFile(path)
	if errors.Is(e, os.ErrNotExist) {
		return j, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(b, &j.Records); e != nil {
		return nil, e
	}
	if j.Records == nil {
		return nil, errors.New("null journal")
	}
	for id, r := range j.Records {
		switch r.State {
		case "RUNNING":
			r.State = "HOLD"
			j.Records[id] = r
		case "DONE", "HOLD", "FAILED", "CANCELLED":
		default:
			return nil, fmt.Errorf("invalid state %s", r.State)
		}
	}
	return j, j.save()
}
func (j *Journal) save() error {
	b, e := json.MarshalIndent(j.Records, "", "  ")
	if e != nil {
		return e
	}
	// One scheduler owns this file. Replace + file sync; power-loss directory durability is not claimed.
	f, e := os.CreateTemp(filepath.Dir(j.Path), "journal-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	return os.Rename(name, j.Path)
}
func (j *Journal) set(id string, r Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Records[id] = r
	return j.save()
}
func (j *Journal) get(id string) Record { j.mu.Lock(); defer j.mu.Unlock(); return j.Records[id] }
func validate(nodes []Node, adapters map[string]Adapter) error {
	ids := map[string]Node{}
	for _, n := range nodes {
		if n.ID == "" || n.Retries < 0 || n.Retries > 5 {
			return errors.New("invalid node")
		}
		if _, ok := ids[n.ID]; ok {
			return errors.New("duplicate node")
		}
		if adapters[n.Adapter] == nil {
			return errors.New("unknown adapter")
		}
		ids[n.ID] = n
	}
	colors := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if colors[id] == 1 {
			return errors.New("cycle")
		}
		if colors[id] == 2 {
			return nil
		}
		n, ok := ids[id]
		if !ok {
			return errors.New("missing dependency")
		}
		colors[id] = 1
		for _, d := range n.Deps {
			if e := visit(d); e != nil {
				return e
			}
		}
		colors[id] = 2
		return nil
	}
	for id := range ids {
		if e := visit(id); e != nil {
			return e
		}
	}
	return nil
}

type completion struct {
	id  string
	err error
}

func Run(ctx context.Context, nodes []Node, adapters map[string]Adapter, j *Journal, limit int) error {
	if limit < 1 {
		return errors.New("positive concurrency required")
	}
	if e := validate(nodes, adapters); e != nil {
		return e
	}
	// Journal IDs are bound to the exact specification by the CLI's spec receipt. API callers must do likewise.
	pending := map[string]Node{}
	for _, n := range nodes {
		if j.get(n.ID).State == "" {
			pending[n.ID] = n
		}
	}
	done := make(chan completion, limit)
	active := 0
	var first error
	for len(pending) > 0 || active > 0 {
		launched := false
		keys := []string{}
		for id := range pending {
			keys = append(keys, id)
		}
		sort.Strings(keys)
		for _, id := range keys {
			n := pending[id]
			if ctx.Err() != nil {
				if e := j.set(id, Record{State: "CANCELLED"}); e != nil {
					return e
				}
				delete(pending, id)
				continue
			}
			ready, blocked := true, false
			for _, d := range n.Deps {
				state := j.get(d).State
				if state != "DONE" {
					ready = false
				}
				if state == "HOLD" || state == "FAILED" || state == "CANCELLED" {
					blocked = true
				}
			}
			if blocked {
				if e := j.set(id, Record{State: "HOLD"}); e != nil {
					return e
				}
				delete(pending, id)
				continue
			}
			if !ready || active >= limit {
				continue
			}
			if e := j.set(id, Record{State: "RUNNING", Attempts: 1}); e != nil {
				return e
			}
			delete(pending, id)
			active++
			launched = true
			go func(n Node) {
				r := Record{State: "RUNNING", Attempts: 1}
				for {
					if ctx.Err() != nil {
						r.State = "CANCELLED"
						done <- completion{n.ID, j.set(n.ID, r)}
						return
					}
					output, e := adapters[n.Adapter](ctx, n.Input)
					if e == nil {
						r.State = "DONE"
						r.Output = output
						done <- completion{n.ID, j.set(n.ID, r)}
						return
					}
					var safe Retryable
					if errors.As(e, &safe) {
						if r.Attempts <= n.Retries {
							r.Attempts++
							if e = j.set(n.ID, r); e != nil {
								done <- completion{n.ID, e}
								return
							}
							continue
						}
						r.State = "FAILED"
					} else {
						r.State = "HOLD"
					}
					done <- completion{n.ID, j.set(n.ID, r)}
					return
				}
			}(n)
		}
		if active > 0 {
			c := <-done
			active--
			if c.err != nil && first == nil {
				first = c.err
			}
			if first != nil {
				break
			}
		} else if len(pending) > 0 && !launched {
			return errors.New("no runnable nodes")
		}
	}
	// Join all active workers before returning; adapters must cooperate with context cancellation.
	for active > 0 {
		c := <-done
		active--
		if first == nil {
			first = c.err
		}
	}
	if first != nil {
		return first
	}
	return ctx.Err()
}
