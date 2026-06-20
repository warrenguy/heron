//go:build rapid

package codecs_test

import (
	"flag"
	"strconv"
	"testing"

	"pgregory.net/rapid"
)

const (
	rapidChecksSBEDefault        = 1000
	rapidChecksSessionEvent      = 500
	rapidChecksSessionConnectReq = 200
)

func rapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()

	f := flag.Lookup("rapid.checks")
	if f == nil {
		rapid.Check(tb, prop)
		return
	}

	previous := f.Value.String()
	if err := flag.Set("rapid.checks", strconv.Itoa(checks)); err != nil {
		tb.Fatalf("rapid.checks: %v", err)
	}
	tb.Cleanup(func() { _ = flag.Set("rapid.checks", previous) })

	rapid.Check(tb, prop)
}
