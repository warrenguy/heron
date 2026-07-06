//go:build testhooks

package client

import "testing"

// spinOnceOpRecorder is unsynchronized.
//
// Tests must not set the recorder from t.Parallel() subtests or mutate it
// concurrently with SpinOnce.
var spinOnceOpRecorder func(op string)

// SetSpinOnceOpRecorderForTest installs a callback invoked at SpinOnce operation
// boundaries (DoWork and immediately before PollEgress).
func SetSpinOnceOpRecorderForTest(tb testing.TB, recorder func(op string)) {
	tb.Helper()
	spinOnceOpRecorder = recorder
	tb.Cleanup(func() { spinOnceOpRecorder = nil })
}

func recordSpinOnceOp(op string) {
	if spinOnceOpRecorder != nil {
		spinOnceOpRecorder(op)
	}
}
