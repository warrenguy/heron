package transportfault

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeReporter reports a fixed terminating flag and counts calls.
type fakeReporter struct {
	terminating atomic.Bool
	calls       atomic.Int64
}

func (reporter *fakeReporter) IsTerminating() bool {
	reporter.calls.Add(1)
	return reporter.terminating.Load()
}

func TestMonitor_TerminationCause(t *testing.T) {
	t.Parallel()

	resourceErr := errors.New("resource fault")
	terminationErr := errors.New("termination fault")

	tests := []struct {
		name        string
		nilMonitor  bool
		errs        []error
		terminating bool
		want        error
		wantCalls   int64
	}{
		{name: "nil_monitor", nilMonitor: true, terminating: true},
		{name: "no_error_running"},
		{name: "no_error_terminating", terminating: true},
		{name: "resource_error_running", errs: []error{resourceErr}, wantCalls: 1},
		{
			name:        "termination_error",
			errs:        []error{terminationErr},
			terminating: true,
			want:        terminationErr,
			wantCalls:   1,
		},
		{
			name:        "resource_error_before_termination_error",
			errs:        []error{resourceErr, terminationErr},
			terminating: true,
			want:        terminationErr,
			wantCalls:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var monitor *Monitor
			var nextGot []error
			if !tc.nilMonitor {
				monitor = &Monitor{}
				handler := monitor.Handler(func(err error) { nextGot = append(nextGot, err) })
				for _, err := range tc.errs {
					handler(err)
				}
			}
			require.Equal(t, tc.errs, nextGot)
			reporter := &fakeReporter{}
			reporter.terminating.Store(tc.terminating)

			// Phase: the first check consumes the pending error.
			got := monitor.TerminationCause(reporter)
			if tc.want == nil {
				require.NoError(t, got)
			} else {
				require.ErrorIs(t, got, tc.want)
			}
			require.Equal(t, tc.wantCalls, reporter.calls.Load())

			// Phase: the first check cleared the pending error.
			require.NoError(t, monitor.TerminationCause(reporter))
			require.Equal(t, tc.wantCalls, reporter.calls.Load())
		})
	}
}

func TestMonitor_Handler_concurrent(t *testing.T) {
	t.Parallel()

	const goroutines = 4
	const callsPerGoroutine = 1000

	monitor := &Monitor{}
	var nextCalls atomic.Int64
	handler := monitor.Handler(func(error) { nextCalls.Add(1) })
	reporter := &fakeReporter{}
	reporter.terminating.Store(true)
	errs := make([]error, goroutines)
	for i := range errs {
		errs[i] = fmt.Errorf("fault %d", i)
	}

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			for range callsPerGoroutine {
				handler(errs[i])
			}
		})
	}
	for nextCalls.Load() < goroutines*callsPerGoroutine {
		if cause := monitor.TerminationCause(reporter); cause != nil {
			require.Contains(t, errs, cause)
		}
	}
	wg.Wait()

	require.Equal(t, int64(goroutines*callsPerGoroutine), nextCalls.Load())
	cause := monitor.TerminationCause(reporter)
	if cause != nil {
		require.Contains(t, errs, cause)
	}
	require.NoError(t, monitor.TerminationCause(reporter))
}
