//go:build rapid && testhooks

package archive

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"luas.fast/aeron"
	"luas.fast/idlestrategy"
)

const (
	awaitFaultRapidStartNs int64 = 1_000_000_000
	awaitFaultRapidTimeout       = time.Second
)

type awaitFaultKind int

const (
	awaitFaultKindMatch awaitFaultKind = iota
	awaitFaultKindFault
	awaitFaultKindTimeout
)

type awaitFaultOutcome struct {
	kind  awaitFaultKind
	polls int
}

// simulateAwaitFault walks one schedule in loop order. matchFrom is the first
// matchFn call that reports done. Call 0 runs before the loop. faultFrom and
// deadlineFrom are one-based loop iterations, or zero to disable.
func simulateAwaitFault(matchFrom, faultFrom, deadlineFrom int) awaitFaultOutcome {
	if matchFrom == 0 {
		return awaitFaultOutcome{kind: awaitFaultKindMatch}
	}

	for iter := 1; ; iter++ {
		switch iter {
		case deadlineFrom:
			return awaitFaultOutcome{kind: awaitFaultKindTimeout, polls: iter - 1}

		case faultFrom:
			return awaitFaultOutcome{kind: awaitFaultKindFault, polls: iter - 1}

		case matchFrom:
			return awaitFaultOutcome{kind: awaitFaultKindMatch, polls: iter}
		}
	}
}

func TestRunControlAwaitLoop_faultOrder_rapid(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	faultErr := errors.New("transport fault")
	matchErr := errors.New("latched match")

	rapid.Check(t, func(rt *rapid.T) {
		matchFrom := rapid.IntRange(0, 20).Draw(rt, "matchFrom")
		faultFrom := rapid.IntRange(0, 21).Draw(rt, "faultFrom")
		deadlineFrom := rapid.IntRange(0, 21).Draw(rt, "deadlineFrom")
		want := simulateAwaitFault(matchFrom, faultFrom, deadlineFrom)

		now := awaitFaultRapidStartNs
		var polls, checkCalls, matchCalls int
		err := runControlAwaitLoop(
			t.Context(),
			harness.Client,
			Config{
				IdleStrategy: idlestrategy.NoOp,
				AeronConfig: aeron.Config{
					NanoClock: func() int64 { return now },
				},
			},
			func() int {
				polls++
				return 0
			},
			func() error {
				checkCalls++
				if faultFrom > 0 && checkCalls >= faultFrom {
					return faultErr
				}

				return nil
			},
			awaitFaultRapidTimeout,
			42,
			ControlResponseAwaitOperation,
			func() (bool, error) {
				matchCalls++
				if matchCalls == deadlineFrom {
					now = awaitFaultRapidStartNs + int64(awaitFaultRapidTimeout)
				}

				return matchCalls > matchFrom, matchErr
			},
		)

		require.Equal(rt, want.polls, polls)

		switch want.kind {
		case awaitFaultKindMatch:
			require.ErrorIs(rt, err, matchErr)

		case awaitFaultKindFault:
			require.ErrorIs(rt, err, faultErr)
			var timeoutErr *ControlResponseTimeoutError
			require.NotErrorAs(rt, err, &timeoutErr)

		case awaitFaultKindTimeout:
			requireControlResponseTimeout(
				rt,
				err,
				ControlResponseAwaitOperation,
				42,
				awaitFaultRapidTimeout,
			)
		}
	})
}
