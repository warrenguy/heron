//go:build rapid

package codecs_test

import (
	"flag"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rapid"
)

const (
	rapidChecksSBEDefault        = 1000
	rapidChecksSessionEvent      = 500
	rapidChecksSessionConnectReq = 200
)

var rapidChecksFlagMu sync.Mutex

func rapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()

	f := flag.Lookup("rapid.checks")
	if f == nil {
		rapid.Check(tb, prop)
		return
	}

	rapidChecksFlagMu.Lock()
	defer rapidChecksFlagMu.Unlock()

	previous := f.Value.String()
	defer func() { _ = flag.Set("rapid.checks", previous) }()

	if err := flag.Set("rapid.checks", strconv.Itoa(checks)); err != nil {
		tb.Fatalf("rapid.checks: %v", err)
	}

	rapid.Check(tb, prop)
}
