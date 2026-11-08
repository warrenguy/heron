package testutil

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"pgregory.net/rapid"
)

var rapidFlagsMu sync.Mutex

// visitedFlags caches the first flag.Visit. A later flag.Visit also reports a
// flag that flag.Set changed.
var visitedFlags = sync.OnceValue(func() map[string]bool {
	visited := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { visited[f.Name] = true })

	return visited
})

// IsRapidFlagSet reports whether the command line or the RAPID_* environment
// variable set the rapid flag name, such as "rapid.checks". The first call must
// run after flag.Parse and before any flag.Set.
func IsRapidFlagSet(name string) bool {
	if visitedFlags()[name] {
		return true
	}

	_, ok := os.LookupEnv("RAPID_" + strings.ToUpper(strings.TrimPrefix(name, "rapid.")))

	return ok
}

// RapidCheck sets -rapid.checks to checks and runs rapid.Check. A command-line
// or environment value wins. A parallel test that calls rapid.Check directly
// races with the flag writes.
func RapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()
	RapidCheckSteps(tb, checks, 0, prop)
}

// RapidCheckSteps works like RapidCheck and also sets -rapid.steps to steps. A
// zero count leaves its flag unchanged.
func RapidCheckSteps(tb testing.TB, checks, steps int, prop func(*rapid.T)) {
	tb.Helper()

	if IsRapidFlagSet("rapid.checks") {
		checks = 0
	}

	if IsRapidFlagSet("rapid.steps") {
		steps = 0
	}

	if checks == 0 && steps == 0 {
		rapid.Check(tb, prop)
		return
	}

	rapidFlagsMu.Lock()
	defer rapidFlagsMu.Unlock()

	defer setRapidFlag(tb, "rapid.checks", checks)()
	defer setRapidFlag(tb, "rapid.steps", steps)()

	rapid.Check(tb, prop)
}

func setRapidFlag(tb testing.TB, name string, value int) func() {
	tb.Helper()

	if value == 0 {
		return func() {}
	}

	previous := flag.Lookup(name).Value.String()
	if err := flag.Set(name, strconv.Itoa(value)); err != nil {
		tb.Fatalf("%s: %v", name, err)
	}

	return func() { _ = flag.Set(name, previous) }
}
