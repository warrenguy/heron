package testutil

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestIterationTB_failureReachesRapid checks that a helper failure fails the
// rapid check, not the outer test.
func TestIterationTB_failureReachesRapid(t *testing.T) {
	cases := []struct {
		name string
		fail func(tb testing.TB)
	}{
		{name: "require in body", fail: func(tb testing.TB) { require.Fail(tb, "helper failed") }},
		{
			name: "fatal in cleanup",
			fail: func(tb testing.TB) {
				tb.Cleanup(func() { tb.Fatal("helper failed") })
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// t.Chdir keeps the rapid failfile out of the package directory.
			t.Chdir(t.TempDir())

			report := &recordingRapidTB{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				rapid.Check(report, func(rt *rapid.T) {
					tc.fail(IterationTB(t, rt))
				})
			}()
			<-done

			require.False(t, t.Failed())
			require.True(t, report.Failed())
			require.Contains(t, report.errors(), "[rapid] failed after")
		})
	}
}

// recordingRapidTB records the rapid report. FailNow and the skip methods end
// the calling goroutine.
type recordingRapidTB struct {
	failed bool
	lines  []string
}

func (tb *recordingRapidTB) Helper()                   {}
func (tb *recordingRapidTB) Name() string              { return "recording" }
func (tb *recordingRapidTB) Logf(string, ...any)       {}
func (tb *recordingRapidTB) Log(...any)                {}
func (tb *recordingRapidTB) Skipf(string, ...any)      { runtime.Goexit() }
func (tb *recordingRapidTB) Skip(...any)               { runtime.Goexit() }
func (tb *recordingRapidTB) SkipNow()                  { runtime.Goexit() }
func (tb *recordingRapidTB) Fail()                     { tb.failed = true }
func (tb *recordingRapidTB) Failed() bool              { return tb.failed }
func (tb *recordingRapidTB) Error(args ...any)         { tb.Errorf("%s", fmt.Sprint(args...)) }
func (tb *recordingRapidTB) Fatal(args ...any)         { tb.Error(args...); tb.FailNow() }
func (tb *recordingRapidTB) FailNow()                  { tb.failed = true; runtime.Goexit() }
func (tb *recordingRapidTB) errors() string            { return strings.Join(tb.lines, "\n") }
func (tb *recordingRapidTB) Fatalf(f string, a ...any) { tb.Errorf(f, a...); tb.FailNow() }

func (tb *recordingRapidTB) Errorf(format string, args ...any) {
	tb.failed = true
	tb.lines = append(tb.lines, fmt.Sprintf(format, args...))
}
