//go:build integration

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
	msg := fmt.Sprintf(format, args...)
	if !strings.Contains(msg, "Error Trace:") {
		capture.lastMsg = msg
		return
	}
	for line := range strings.SplitSeq(msg, "\n") {
		trimmed := strings.TrimSpace(line)
		after, ok := strings.CutPrefix(trimmed, "Error:")
		if ok {
			capture.lastMsg = strings.TrimSpace(after)
			return
		}
	}
	capture.lastMsg = msg
}

func (capture *captureFailTB) FailNow() {
	capture.failed = true
}

func failedCaptureTB(t testing.TB) bool {
	capture, ok := t.(*captureFailTB)
	return ok && capture.failed
}
