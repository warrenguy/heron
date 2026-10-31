//go:build testhooks

package consensus

import "testing"

// agentDoWorkStepRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with Agent.DoWork.
var agentDoWorkStepRecorder func(step string)

var agentModuleStateChangeRecorder func(from, to ModuleState, reason string)

// agentUnexpectedHigherTermRecorder is unsynchronized. Same invoker-only rule
// as agentDoWorkStepRecorder.
var agentUnexpectedHigherTermRecorder func(nowNs, term int64)

// SetDoWorkStepRecorderForTest installs a callback invoked at each DoWork step
// boundary (slow tick, ingress poll, election or consensus work).
func SetDoWorkStepRecorderForTest(tb testing.TB, recorder func(step string)) {
	tb.Helper()
	agentDoWorkStepRecorder = recorder
	tb.Cleanup(func() { agentDoWorkStepRecorder = nil })
}

// SetUnexpectedHigherTermRecorderForTest installs a callback invoked when
// steady ingress observes a leadership term above Agent's current term.
func SetUnexpectedHigherTermRecorderForTest(
	tb testing.TB,
	recorder func(nowNs, term int64),
) {
	tb.Helper()
	agentUnexpectedHigherTermRecorder = recorder
	tb.Cleanup(func() { agentUnexpectedHigherTermRecorder = nil })
}

func noteDoWorkStep(step string) {
	if agentDoWorkStepRecorder != nil {
		agentDoWorkStepRecorder(step)
	}
}

func SetModuleStateChangeRecorderForTest(
	tb testing.TB,
	recorder func(from, to ModuleState, reason string),
) {
	tb.Helper()
	agentModuleStateChangeRecorder = recorder
	tb.Cleanup(func() { agentModuleStateChangeRecorder = nil })
}

func noteModuleStateChange(from, to ModuleState, reason string) {
	if agentModuleStateChangeRecorder != nil {
		agentModuleStateChangeRecorder(from, to, reason)
	}
}

func noteUnexpectedHigherTerm(nowNs, term int64) {
	if agentUnexpectedHigherTermRecorder != nil {
		agentUnexpectedHigherTermRecorder(nowNs, term)
	}
}
