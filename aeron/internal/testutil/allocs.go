package testutil

import "testing"

func AssertZeroAllocs(tb testing.TB, label string, n int, fn func()) {
	tb.Helper()
	if allocs := testing.AllocsPerRun(n, fn); allocs != 0 {
		tb.Fatalf("%s allocs = %v, want 0", label, allocs)
	}
}
