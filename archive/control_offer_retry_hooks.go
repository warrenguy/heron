//go:build testhooks

package archive

import "testing"

// controlOfferRetryDoWorkRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with controlOfferUntilPositive.
var controlOfferRetryDoWorkRecorder func()

// SetControlOfferRetryDoWorkRecorderForTest installs a callback invoked after each
// aeronClient.DoWork() inside controlOfferUntilPositive.
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
