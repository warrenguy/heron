//go:build rapid

package election_test

import (
	"flag"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rapid"
)

const rapidChecksSBEDefault = 500

var rapidChecksFlagMu sync.Mutex

func rapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()
	rapidWithFlags(tb, map[string]int{"rapid.checks": checks}, prop)
}

// rapidCheckSteps sets both -rapid.checks and -rapid.steps under the same mutex
// as rapidCheck, then restores both. Stateful tests that call this must not use
// t.Parallel(): flag mutation is process-wide and serializes only while the lock
// is held for the Check call.
func rapidCheckSteps(tb testing.TB, checks, steps int, prop func(*rapid.T)) {
	tb.Helper()
	rapidWithFlags(tb, map[string]int{
		"rapid.checks": checks,
		"rapid.steps":  steps,
	}, prop)
}

func rapidWithFlags(tb testing.TB, flags map[string]int, prop func(*rapid.T)) {
	tb.Helper()

	lookups := make(map[string]*flag.Flag, len(flags))
	for name := range flags {
		f := flag.Lookup(name)
		if f == nil {
			rapid.Check(tb, prop)
			return
		}
		lookups[name] = f
	}

	rapidChecksFlagMu.Lock()
	defer rapidChecksFlagMu.Unlock()

	prev := make(map[string]string, len(lookups))
	for name, f := range lookups {
		prev[name] = f.Value.String()
	}
	defer func() {
		for name, v := range prev {
			_ = flag.Set(name, v)
		}
	}()

	for name, v := range flags {
		if err := flag.Set(name, strconv.Itoa(v)); err != nil {
			tb.Fatalf("%s: %v", name, err)
		}
	}

	rapid.Check(tb, prop)
}
