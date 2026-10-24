package testutil

import (
	"time"

	"github.com/stretchr/testify/require"
)

// RequireReturnsWithin runs fn on a new goroutine. Fails tb when fn does not
// return within timeout. A hung fn leaks its goroutine. fn runs off the test
// goroutine, so it must not call require or FailNow; a panic in fn ends the
// test binary.
func RequireReturnsWithin(tb TB, timeout time.Duration, fn func()) {
	tb.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		require.FailNowf(tb, "call did not return", "timeout %s", timeout)
	}
}
