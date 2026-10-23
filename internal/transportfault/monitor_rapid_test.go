//go:build rapid

package transportfault

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type monitorRapidOp int

const (
	monitorRapidOpHandler monitorRapidOp = iota
	monitorRapidOpTerminate
	monitorRapidOpCheck
)

// monitorRapidModel tracks the pending error and the terminating flag of
// the fake reporter.
type monitorRapidModel struct {
	pending     error
	terminating bool
	handled     []error
}

func TestMonitor_model_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		monitor := &Monitor{}
		var nextGot []error
		handler := monitor.Handler(func(err error) { nextGot = append(nextGot, err) })
		reporter := &fakeReporter{}
		model := &monitorRapidModel{}

		steps := rapid.IntRange(1, 64).Draw(rt, "steps")
		for step := range steps {
			op := monitorRapidOp(rapid.IntRange(
				int(monitorRapidOpHandler),
				int(monitorRapidOpCheck),
			).Draw(rt, "op"))
			switch op {
			case monitorRapidOpHandler:
				err := fmt.Errorf("fault %d", step)
				handler(err)
				model.pending = err
				model.handled = append(model.handled, err)
			case monitorRapidOpTerminate:
				// The flag never clears once set, as on a transport client.
				reporter.terminating.Store(true)
				model.terminating = true
			case monitorRapidOpCheck:
				requireMonitorRapidCheck(rt, monitor, reporter, model)
			}
			require.Equal(rt, model.handled, nextGot)
		}
	})
}

func requireMonitorRapidCheck(
	rt *rapid.T,
	monitor *Monitor,
	reporter *fakeReporter,
	model *monitorRapidModel,
) {
	rt.Helper()

	got := monitor.TerminationCause(reporter)
	if model.pending != nil && model.terminating {
		require.Same(rt, model.pending, got)
	} else {
		require.NoError(rt, got)
	}
	model.pending = nil
	require.Nil(rt, monitor.pending.Load())
}
