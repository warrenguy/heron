//go:build rapid

package consensus

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type serviceAckOpKind int

const (
	serviceAckOpOffer serviceAckOpKind = iota
	serviceAckOpHasReached
	serviceAckOpRemoveHead
	serviceAckOpDepth
)

// modelHasReached decides the walk outcome from two independent scans: the
// lowest empty queue and the lowest non-empty first entry that mismatches the
// pair. The lower index decides.
//
// Returns:
//   - (false, nil) when the first empty index is lower than any mismatch
//   - (false, ErrServiceAckOutOfSequence) when a mismatch wins the index race
//   - (true, nil) when neither empty nor mismatch is present
//
// Derived from index precedence. Does not replay the early-return loop under
// test.
func modelHasReached(
	queues [][]serviceAck,
	logPosition, ackID int64,
) (bool, error) {
	firstEmpty := slices.IndexFunc(queues, func(q []serviceAck) bool {
		return len(q) == 0
	})
	firstMismatch := slices.IndexFunc(queues, func(q []serviceAck) bool {
		return len(q) > 0 && (q[0].ackID != ackID || q[0].logPosition != logPosition)
	})
	switch {
	case firstEmpty >= 0 && (firstMismatch < 0 || firstEmpty < firstMismatch):
		return false, nil
	case firstMismatch >= 0:
		return false, ErrServiceAckOutOfSequence
	default:
		return true, nil
	}
}

// assertReachedOutcome asserts (ok, err) against the model pair. wantErr is
// nil or ErrServiceAckOutOfSequence.
func assertReachedOutcome(rt *rapid.T, wantOK bool, wantErr error, gotOK bool, gotErr error) {
	rt.Helper()
	require.Equal(rt, wantOK, gotOK)
	if wantErr != nil {
		require.ErrorIs(rt, gotErr, wantErr)
	} else {
		require.NoError(rt, gotErr)
	}
}

// Asserts offer, hasReached, removeServiceAckHead, and depth reads against a
// reference model that mirrors the Java walk order. The Agent comes from
// NewAgent. Offer and drawn-pair hasReached go white-box through the tracker.
// hasReachedServiceAcks, removeServiceAckHead, and getters go through the Agent.
func TestServiceAckTracker_sequencing_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		serviceCount := rapid.IntRange(0, 4).Draw(rt, "serviceCount")
		a := NewAgent(AgentConfig{ServiceCount: serviceCount})
		model := make([][]serviceAck, serviceCount)
		expectedID := int64(0)

		snapshotDepths := func() []int {
			rt.Helper()
			depths := make([]int, serviceCount)
			for i := range serviceCount {
				depths[i] = a.ServiceAckQueueDepth(int32(i))
			}
			return depths
		}

		assertQueueContents := func() {
			rt.Helper()
			require.Len(rt, a.serviceAcks.queues, serviceCount)
			require.Equal(rt, expectedID, a.ExpectedServiceAckID())
			for sid := range serviceCount {
				require.Equal(rt, model[sid], a.serviceAcks.queues[sid])
				require.Equal(rt, len(model[sid]), a.ServiceAckQueueDepth(int32(sid)))
			}
			// Out-of-range depth is always 0.
			require.Equal(rt, 0, a.ServiceAckQueueDepth(-1))
			require.Equal(rt, 0, a.ServiceAckQueueDepth(int32(serviceCount)))
		}

		checkHasReached := func(logPosition, ackID int64) {
			rt.Helper()
			before := snapshotDepths()
			wantOK, wantErr := modelHasReached(model, logPosition, ackID)
			gotOK, gotErr := a.serviceAcks.hasReached(logPosition, ackID)
			assertReachedOutcome(rt, wantOK, wantErr, gotOK, gotErr)
			// Check-only: depths unchanged.
			require.Equal(rt, before, snapshotDepths())

			// hasReachedServiceAcks uses expectedID only.
			if ackID == expectedID {
				reachedOK, reachedErr := a.hasReachedServiceAcks(logPosition)
				assertReachedOutcome(rt, wantOK, wantErr, reachedOK, reachedErr)
				require.Equal(rt, before, snapshotDepths())
			}
		}

		stepCount := rapid.IntRange(1, 64).Draw(rt, "stepCount")
		for range stepCount {
			// Bias offer and hasReached. Remove only when the model matches.
			ops := []serviceAckOpKind{
				serviceAckOpOffer,
				serviceAckOpOffer,
				serviceAckOpHasReached,
				serviceAckOpHasReached,
				serviceAckOpDepth,
			}
			// removeServiceAckHead only when every first entry matches in the model.
			canRemove := false
			var removePos int64
			if serviceCount == 0 {
				// Zero services: always matched at any position.
				canRemove = true
				removePos = rapid.Int64().Draw(rt, "zeroServiceRemovePos")
			} else if len(model[0]) > 0 {
				// Candidate position is first entry of service 0; walk confirms.
				cand := model[0][0].logPosition
				ok, _ := modelHasReached(model, cand, expectedID)
				if ok {
					canRemove = true
					removePos = cand
				}
			}
			if canRemove {
				ops = append(ops, serviceAckOpRemoveHead)
			}

			op := rapid.SampledFrom(ops).Draw(rt, "op")
			switch op {
			case serviceAckOpOffer:
				// serviceID may be out of range.
				sid := rapid.Int32Range(-2, int32(serviceCount)+1).Draw(rt, "offerServiceID")
				ack := serviceAck{
					ackID:       rapid.Int64().Draw(rt, "ackID"),
					logPosition: rapid.Int64().Draw(rt, "logPosition"),
					relevantID:  rapid.Int64().Draw(rt, "relevantID"),
				}
				// Bias toward the model expected next pair. Share logPosition with
				// any existing expected first entry so multi-service match and
				// remove can run. Independent free positions almost never align.
				if rapid.Bool().Draw(rt, "biasExpected") {
					ack.ackID = expectedID
					for i := range model {
						if len(model[i]) > 0 && model[i][0].ackID == expectedID {
							ack.logPosition = model[i][0].logPosition
							break
						}
					}
				}
				accepted := a.serviceAcks.offer(sid, ack)
				inRange := sid >= 0 && int(sid) < serviceCount
				require.Equal(rt, inRange, accepted)
				if inRange {
					model[sid] = append(model[sid], ack)
				}

			case serviceAckOpHasReached:
				logPos := rapid.Int64().Draw(rt, "drawnPos")
				ackID := rapid.Int64().Draw(rt, "drawnAckID")
				// Mix drawn pairs with the model expected next pair.
				if rapid.Bool().Draw(rt, "checkExpected") {
					ackID = expectedID
					if serviceCount > 0 && len(model[0]) > 0 {
						logPos = model[0][0].logPosition
					}
				}
				checkHasReached(logPos, ackID)

			case serviceAckOpRemoveHead:
				// Call only when the model says every first entry matches.
				ok, err := modelHasReached(model, removePos, expectedID)
				require.True(rt, ok)
				require.NoError(rt, err)
				reachedOK, reachedErr := a.hasReachedServiceAcks(removePos)
				require.True(rt, reachedOK)
				require.NoError(rt, reachedErr)

				a.removeServiceAckHead()
				for i := range model {
					if len(model[i]) > 0 {
						model[i] = model[i][1:]
					}
				}
				expectedID++

			case serviceAckOpDepth:
				sid := rapid.Int32Range(-1, int32(serviceCount)+1).Draw(rt, "depthServiceID")
				got := a.ServiceAckQueueDepth(sid)
				if sid < 0 || int(sid) >= serviceCount {
					require.Equal(rt, 0, got)
				} else {
					require.Equal(rt, len(model[sid]), got)
				}
			}

			// Every op holds the full-state invariant.
			assertQueueContents()
		}

		checkHasReached(
			rapid.Int64().Draw(rt, "finalPos"),
			rapid.Int64().Draw(rt, "finalAckID"),
		)
	})
}
