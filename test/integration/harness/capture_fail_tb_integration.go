//go:build integration

package harness

import "testing"

// failedCaptureTB reports whether t is a CaptureFailTB that recorded a Fail.
// Integration helpers use this after require paths that may not Goexit under
// CaptureFailTB.
func failedCaptureTB(t testing.TB) bool {
	capture, ok := t.(*CaptureFailTB)
	return ok && capture.failed
}

// skippedCaptureTB reports whether t is a CaptureFailTB that recorded a Skip.
// Real testing.T Skip ends the test goroutine. CaptureFailTB does not. Callers
// must return after resolve helpers Skip under CaptureFailTB.
func skippedCaptureTB(t testing.TB) bool {
	capture, ok := t.(*CaptureFailTB)
	return ok && capture.skipped
}
