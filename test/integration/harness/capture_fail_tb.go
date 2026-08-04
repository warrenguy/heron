package harness

import (
	"fmt"
	"strings"
	"testing"
)

// CaptureFailTB records Fail and Skip without stopping the parent test run.
//
// Behaviour: FailNow, Fatalf, Skip, Skipf, and SkipNow do not call the embedded
// TB, so they do not Goexit the parent. Failed reports only Fail paths. A Skip
// leaves Failed false so negative tests can require a fail rather than a skip.
type CaptureFailTB struct {
	testing.TB
	failed  bool
	skipped bool
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

// Skipped reports whether a captured Skip path ran.
func (capture *CaptureFailTB) Skipped() bool {
	return capture.skipped
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

func (capture *CaptureFailTB) Skip(args ...any) {
	capture.markSkipped(strings.TrimSpace(fmt.Sprint(args...)))
}

func (capture *CaptureFailTB) Skipf(format string, args ...any) {
	capture.markSkipped(fmt.Sprintf(format, args...))
}

func (capture *CaptureFailTB) SkipNow() {
	capture.markSkipped("")
}

func (capture *CaptureFailTB) markSkipped(msg string) {
	if msg != "" {
		capture.lastMsg = msg
	}
	capture.skipped = true
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
