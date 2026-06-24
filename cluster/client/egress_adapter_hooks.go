//go:build testhooks

package client

// egressAdapterOpRecorder is unsynchronized; tests must not set the recorder
// from t.Parallel() subtests or mutate it concurrently with PollEgress.
var egressAdapterOpRecorder func(string)

// SetEgressAdapterOpRecorderForTest installs a callback that records EgressAdapter
// teardown operations during tests. Pass nil to disable recording.
func SetEgressAdapterOpRecorderForTest(recorder func(string)) {
	egressAdapterOpRecorder = recorder
}

func recordEgressAdapterOp(name string) {
	if egressAdapterOpRecorder != nil {
		egressAdapterOpRecorder(name)
	}
}
