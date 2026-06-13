package harness

import (
	"fmt"
	"strings"
	"testing"
)

type captureFailTB struct {
	testing.TB
	failed  bool
	lastMsg string
}

func (capture *captureFailTB) Errorf(format string, args ...any) {
	capture.lastMsg = lastCapturedErrorMessage(fmt.Sprintf(format, args...))
}

func (capture *captureFailTB) FailNow() {
	capture.failed = true
}

//nolint:unused // referenced from integration-only pubsub.go.
func failedCaptureTB(t testing.TB) bool {
	capture, ok := t.(*captureFailTB)
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
