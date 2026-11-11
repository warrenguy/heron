//go:build rapid && testhooks

package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
	"github.com/warrenguy/luas/internal/testutil"
)

const (
	awaitRapidStartNs      int64 = 1_000_000_000
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

		now := awaitRapidStartNs
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
			awaitChecks{transportFault: func() error {
				checkCalls++
				if faultFrom > 0 && checkCalls >= faultFrom {
					return faultErr
				}

				return nil
			}},
			awaitFaultRapidTimeout,
			42,
			ControlResponseAwaitOperation,
			func() (bool, error) {
				matchCalls++
				if matchCalls == deadlineFrom {
					now = awaitRapidStartNs + int64(awaitFaultRapidTimeout)
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

type awaitDisconnectKind int

const (
	awaitDisconnectKindMatch awaitDisconnectKind = iota
	awaitDisconnectKindNotConnected
	awaitDisconnectKindCanceled
	awaitDisconnectKindTimeout
)

// awaitDisconnectSchedule is one drawn loop schedule. Iterations are
// one-based. An iteration past iterations disables its event.
type awaitDisconnectSchedule struct {
	// iterations is the deadline in loop iterations.
	iterations int
	// work[i-1] is the poll work count of iteration i.
	work []int
	// matchIter is the iteration that matches. 0 matches before the loop.
	matchIter int
	// disconnectIter is the first iteration whose check reports not connected.
	disconnectIter int
	// cancelIter is the iteration whose poll cancels ctx.
	cancelIter int
	// nilCheck passes a nil isConnected.
	nilCheck bool
}

type awaitDisconnectOutcome struct {
	kind   awaitDisconnectKind
	polls  int
	checks int
}

// simulateAwaitDisconnect walks the schedule in the runControlAwaitLoop exit
// order.
func simulateAwaitDisconnect(schedule awaitDisconnectSchedule) awaitDisconnectOutcome {
	if schedule.matchIter == 0 {
		return awaitDisconnectOutcome{kind: awaitDisconnectKindMatch}
	}

	var outcome awaitDisconnectOutcome
	for iter := 1; iter <= schedule.iterations; iter++ {
		outcome.polls++

		if iter == schedule.matchIter {
			outcome.kind = awaitDisconnectKindMatch
			return outcome
		}

		checkRuns := schedule.work[iter-1] == 0 && !schedule.nilCheck
		if checkRuns {
			outcome.checks++
		}

		if iter == schedule.cancelIter {
			outcome.kind = awaitDisconnectKindCanceled
			return outcome
		}

		if checkRuns && iter >= schedule.disconnectIter {
			outcome.kind = awaitDisconnectKindNotConnected
			return outcome
		}
	}

	outcome.kind = awaitDisconnectKindTimeout
	return outcome
}

func drawAwaitDisconnectSchedule(rt *rapid.T) awaitDisconnectSchedule {
	rt.Helper()

	iterations := rapid.IntRange(1, 20).Draw(rt, "iterations")
	return awaitDisconnectSchedule{
		iterations:     iterations,
		work:           rapid.SliceOfN(rapid.IntRange(0, 2), iterations, iterations).Draw(rt, "work"),
		matchIter:      rapid.IntRange(0, iterations+1).Draw(rt, "matchIter"),
		disconnectIter: rapid.IntRange(1, iterations+1).Draw(rt, "disconnectIter"),
		cancelIter:     rapid.IntRange(1, iterations+1).Draw(rt, "cancelIter"),
		nilCheck:       rapid.Bool().Draw(rt, "nilCheck"),
	}
}

func TestRunControlAwaitLoop_disconnectOrder_rapid(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	matchErr := errors.New("latched match")

	rapid.Check(t, func(rt *rapid.T) {
		tb := testutil.IterationTB(t, rt)
		schedule := drawAwaitDisconnectSchedule(rt)
		want := simulateAwaitDisconnect(schedule)
		timeout := time.Duration(schedule.iterations)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		now := awaitRapidStartNs
		var polls, checks, matchCalls int

		var isConnected func() bool
		if !schedule.nilCheck {
			isConnected = func() bool {
				checks++
				return polls < schedule.disconnectIter
			}
		}

		err := runControlAwaitLoop(
			ctx,
			harness.Client,
			Config{
				IdleStrategy: idlestrategy.NoOp,
				AeronConfig: aeron.Config{
					NanoClock: func() int64 { return now },
				},
			},
			func() int {
				polls++
				now++

				if polls == schedule.cancelIter {
					cancel()
				}
				return schedule.work[polls-1]
			},
			awaitChecks{isConnected: isConnected},
			timeout,
			notConnectedLoopCorrelationID,
			ControlResponseAwaitOperation,
			func() (bool, error) {
				matchCalls++
				return matchCalls-1 == schedule.matchIter, matchErr
			},
		)

		require.Equal(rt, want.polls, polls)
		require.Equal(rt, want.checks, checks)

		switch want.kind {
		case awaitDisconnectKindMatch:
			require.ErrorIs(rt, err, matchErr)

		case awaitDisconnectKindNotConnected:
			requireControlResponseNotConnected(
				tb,
				err,
				ControlResponseAwaitOperation,
				notConnectedLoopCorrelationID,
			)
			var timeoutErr *ControlResponseTimeoutError
			require.NotErrorAs(rt, err, &timeoutErr)

		case awaitDisconnectKindCanceled:
			require.ErrorIs(rt, err, context.Canceled)
			require.NotErrorIs(rt, err, ErrNotConnected)

		case awaitDisconnectKindTimeout:
			requireControlResponseTimeout(
				rt,
				err,
				ControlResponseAwaitOperation,
				notConnectedLoopCorrelationID,
				timeout,
			)
		}
	})
}
