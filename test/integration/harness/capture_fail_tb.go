package harness

import (
	"fmt"
	"strings"
	"testing"
)

// CaptureFailTB records testify failures without stopping the parent test run.
type CaptureFailTB struct {
	testing.TB
	failed  bool
	lastMsg string
}

// NewCaptureFailTB wraps t for negative assertion cases.
func NewCaptureFailTB(t testing.TB) *CaptureFailTB {
	return &CaptureFailTB{TB: t}
}

// Failed reports whether a captured assertion failed.
func (capture *CaptureFailTB) Failed() bool {
	return capture.failed
}

func (capture *CaptureFailTB) Errorf(format string, args ...any) {
	capture.lastMsg = lastCapturedErrorMessage(fmt.Sprintf(format, args...))
}

func (capture *CaptureFailTB) FailNow() {
	capture.failed = true
}

func (capture *CaptureFailTB) Fatalf(format string, args ...any) {
	capture.Errorf(format, args...)
	capture.FailNow()
}

//nolint:unused // referenced from integration-only pubsub.go.
func failedCaptureTB(t testing.TB) bool {
	capture, ok := t.(*CaptureFailTB)
	return ok && capture.failed
}

func lastCapturedErrorMessage(msg string) string {
	if !strings.Contains(msg, "Error Trace:") {
		return msg
	}
	for line := range strings.SplitSeq(msg, "\n") {
		if after, ok := strings.CutPrefix(strings.TrimSpace(line), "Error:"); ok {
			return strings.TrimSpace(after)
		}
	}
	return msg
}
