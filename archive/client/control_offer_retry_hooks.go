//go:build testhooks

package client

import "testing"

// controlOfferRetryDoWorkRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with ControlOfferUntilPositive.
var controlOfferRetryDoWorkRecorder func()

// SetControlOfferRetryDoWorkRecorderForTest installs a callback invoked after each
// aeronClient.DoWork() inside ControlOfferUntilPositive.
func SetControlOfferRetryDoWorkRecorderForTest(tb testing.TB, recorder func()) {
	tb.Helper()
	controlOfferRetryDoWorkRecorder = recorder
	tb.Cleanup(func() { controlOfferRetryDoWorkRecorder = nil })
}

func recordControlOfferRetryDoWork() {
	if controlOfferRetryDoWorkRecorder != nil {
		controlOfferRetryDoWorkRecorder()
	}
}
