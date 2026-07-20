//go:build testhooks

package consensus

import "testing"

// agentDoWorkStepRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with Agent.DoWork.
var agentDoWorkStepRecorder func(step string)

// SetDoWorkStepRecorderForTest installs a callback invoked at each DoWork step
// boundary (slow tick, ingress poll, election or consensus work).
func SetDoWorkStepRecorderForTest(tb testing.TB, recorder func(step string)) {
	tb.Helper()
	agentDoWorkStepRecorder = recorder
	tb.Cleanup(func() { agentDoWorkStepRecorder = nil })
}

func noteDoWorkStep(step string) {
	if agentDoWorkStepRecorder != nil {
		agentDoWorkStepRecorder(step)
	}
}
