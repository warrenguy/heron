//go:build rapid && testhooks

package archive

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"luas.fast/aeron"
	"luas.fast/idlestrategy"
)

const offerFaultRapidPositive int64 = 1

type offerFaultKind int

const (
	offerFaultKindSuccess offerFaultKind = iota
	offerFaultKindTerminal
	offerFaultKindFault
)

type offerFaultOutcome struct {
	kind   offerFaultKind
	status int64
	offers int
	polls  int
	checks int
}

func offerFaultPosition(seq []int64, index int) int64 {
	if index < len(seq) {
		return seq[index]
	}
	return offerFaultRapidPositive
}

// simulateOfferFault walks seq in controlOfferUntilPositive loop order. Each
// offer follows one fault check. Each BackPressured offer adds a pacing check.
// The fault fires at check number fireFrom.
func simulateOfferFault(seq []int64, fireFrom int) offerFaultOutcome {
	var out offerFaultOutcome

	for {
		out.checks++
		if out.checks >= fireFrom {
			out.kind = offerFaultKindFault
			return out
		}

		pos := offerFaultPosition(seq, out.offers)
		out.offers++

		if pos > 0 {
			out.kind = offerFaultKindSuccess
			out.status = pos
			return out
		}

		if !aeron.IsIngressRetriable(pos) {
			out.kind = offerFaultKindTerminal
			out.status = pos
			return out
		}

		out.polls++

		if pos == aeron.BackPressured {
			out.checks++
			if out.checks >= fireFrom {
				out.kind = offerFaultKindFault
				return out
			}
		}
	}
}

func TestControlOfferUntilPositive_faultOrder_rapid(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	faultErr := errors.New("transport fault")
	results := []int64{
		aeron.BackPressured,
		aeron.AdminAction,
		aeron.NotConnected,
		aeron.Closed,
		offerFaultRapidPositive,
	}

	rapid.Check(t, func(rt *rapid.T) {
		seq := rapid.SliceOfN(rapid.SampledFrom(results), 0, 20).Draw(rt, "offers")
		total := simulateOfferFault(seq, math.MaxInt).checks
		fireFrom := rapid.IntRange(1, total+1).Draw(rt, "fireFrom")
		want := simulateOfferFault(seq, fireFrom)

		var offers, polls, checks int
		pos, err := controlOfferUntilPositive(
			t.Context(),
			harness.Client,
			Config{
				MessageRetryAttempts:     len(seq) + 1,
				ControlOfferRetryTimeout: time.Hour,
				IdleStrategy:             idlestrategy.NoOp,
			},
			ControlOfferConfig{
				IdleStrategy: idlestrategy.NoOp,
				NanoClock:    func() int64 { return 0 },
				PollFn: func() int {
					polls++
					return 0
				},
				transportFault: func() error {
					checks++
					if checks >= fireFrom {
						return faultErr
					}
					return nil
				},
			},
			func() int64 {
				result := offerFaultPosition(seq, offers)
				offers++
				return result
			},
		)

		require.Equal(rt, want.offers, offers)
		require.Equal(rt, want.polls, polls)

		switch want.kind {
		case offerFaultKindSuccess:
			require.NoError(rt, err)
			require.Equal(rt, want.status, pos)

		case offerFaultKindTerminal:
			require.Zero(rt, pos)
			requireControlTerminal(rt, err, want.status)

		case offerFaultKindFault:
			require.Zero(rt, pos)
			require.ErrorIs(rt, err, faultErr)
		}
	})
}
