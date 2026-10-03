package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run . workflow.json journal.json")
		os.Exit(2)
	}
	if e := execute(os.Args[1], os.Args[2]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func execute(spec, path string) error {
	b, e := os.ReadFile(spec)
	if e != nil {
		return e
	}
	var nodes []Node
	if e = json.Unmarshal(b, &nodes); e != nil {
		return e
	}
	hash := sha256.Sum256(b)
	digest := hex.EncodeToString(hash[:])
	bound, e := os.ReadFile(path + ".spec")
	if e == nil && string(bound) != digest {
		return fmt.Errorf("journal specification mismatch")
	}
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if os.IsNotExist(e) {
		if _, e = os.Stat(path); e == nil {
			return fmt.Errorf("unbound existing journal")
		}
		if !os.IsNotExist(e) {
			return e
		}
		if e = os.WriteFile(path+".spec", []byte(digest), 0600); e != nil {
			return e
		}
	}
	j, e := OpenJournal(path)
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	adapters := map[string]Adapter{"uppercase": func(ctx context.Context, s string) (string, error) { return strings.ToUpper(s), nil }}
	if e = Run(ctx, nodes, adapters, j, 2); e != nil {
		return e
	}
	out, _ := json.MarshalIndent(j.Records, "", "  ")
	fmt.Println(string(out))
	for _, r := range j.Records {
		if r.State != "DONE" {
			return fmt.Errorf("workflow incomplete: %s", r.State)
		}
	}
	return nil
}
