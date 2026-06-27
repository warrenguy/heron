//go:build testhooks

package client

import "testing"

// ingressRetryDoWorkRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with IngressOfferUntilPositive.
var ingressRetryDoWorkRecorder func()

// SetIngressRetryDoWorkRecorderForTest installs a callback invoked after each
// aeronClient.DoWork() inside IngressOfferUntilPositive.
func SetIngressRetryDoWorkRecorderForTest(tb testing.TB, recorder func()) {
	tb.Helper()
	ingressRetryDoWorkRecorder = recorder
	tb.Cleanup(func() { ingressRetryDoWorkRecorder = nil })
}

func recordIngressRetryDoWork() {
	if ingressRetryDoWorkRecorder != nil {
		ingressRetryDoWorkRecorder()
	}
}
