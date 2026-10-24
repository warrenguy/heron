//go:build rapid

package consensus

import (
	"maps"
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
	serviceAckOpNoteDropped
	serviceAckOpDropAt
)

// modelHasReached decides the walk outcome from two independent scans: the
// lowest empty queue and the lowest non-empty first entry that mismatches
// logPosition or the next id of its own queue in nextIDs. The lower index
// decides.
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
	nextIDs []int64,
	logPosition int64,
) (bool, error) {
	firstEmpty := slices.IndexFunc(queues, func(q []serviceAck) bool {
		return len(q) == 0
	})
	firstMismatch := -1
	for i, q := range queues {
		if len(q) > 0 && (q[0].ackID != nextIDs[i] || q[0].logPosition != logPosition) {
			firstMismatch = i
			break
		}
	}
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

// modelNoteDropped records ackID as dropped by service i in the set model.
// The next id then advances past every dropped id it reaches.
func modelNoteDropped(nextIDs []int64, dropped []map[int64]bool, i int, ackID int64) {
	if ackID < nextIDs[i] {
		return
	}
	dropped[i][ackID] = true
	modelSkipDropped(nextIDs, dropped, i)
}

// modelSkipDropped advances the next id of service i past each dropped id.
func modelSkipDropped(nextIDs []int64, dropped []map[int64]bool, i int) {
	for dropped[i][nextIDs[i]] {
		delete(dropped[i], nextIDs[i])
		nextIDs[i]++
	}
}

// Asserts offer, hasReached, removeServiceAckHead, noteDropped, dropAt and
// depth reads against a reference model. The model mirrors the Java walk order
// with one next id per queue. The Agent comes from NewAgent. Offer, hasReached,
// noteDropped and dropAt go white-box through the tracker.
// hasReachedServiceAcks, removeServiceAckHead, and getters go through the Agent.
func TestServiceAckTracker_sequencing_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		serviceCount := rapid.IntRange(0, 4).Draw(rt, "serviceCount")
		a := NewAgent(AgentConfig{ServiceCount: serviceCount})
		model := make([][]serviceAck, serviceCount)
		nextIDs := make([]int64, serviceCount)
		dropped := make([]map[int64]bool, serviceCount)
		for i := range dropped {
			dropped[i] = map[int64]bool{}
		}
		popCount := int64(0)

		modelExpectedID := func() int64 {
			if serviceCount == 0 {
				return popCount
			}
			return slices.Min(nextIDs)
		}

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
			require.Equal(rt, modelExpectedID(), a.ExpectedServiceAckID())
			require.Equal(rt, nextIDs, a.serviceAcks.nextAckIDs)
			for sid := range serviceCount {
				require.Equal(rt, model[sid], a.serviceAcks.queues[sid])
				require.Equal(rt, len(model[sid]), a.ServiceAckQueueDepth(int32(sid)))
				wantDropped := slices.Collect(maps.Keys(dropped[sid]))
				require.ElementsMatch(rt, wantDropped, a.serviceAcks.droppedAckIDs[sid])
			}
			// Out-of-range depth is always 0.
			require.Equal(rt, 0, a.ServiceAckQueueDepth(-1))
			require.Equal(rt, 0, a.ServiceAckQueueDepth(int32(serviceCount)))
		}

		checkHasReached := func(logPosition int64) {
			rt.Helper()
			before := snapshotDepths()
			wantOK, wantErr := modelHasReached(model, nextIDs, logPosition)
			gotOK, gotErr := a.serviceAcks.hasReached(logPosition)
			assertReachedOutcome(rt, wantOK, wantErr, gotOK, gotErr)
			// Check-only: depths unchanged.
			require.Equal(rt, before, snapshotDepths())

			reachedOK, reachedErr := a.hasReachedServiceAcks(logPosition)
			assertReachedOutcome(rt, wantOK, wantErr, reachedOK, reachedErr)
			require.Equal(rt, before, snapshotDepths())
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
				serviceAckOpNoteDropped,
				serviceAckOpDropAt,
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
				ok, _ := modelHasReached(model, nextIDs, cand)
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
				inRange := sid >= 0 && int(sid) < serviceCount
				// Bias toward the next unqueued id of the service. Share
				// logPosition with any first entry that matches its own next id,
				// so multi-service match and remove can run. Independent free
				// positions almost never align.
				if inRange && rapid.Bool().Draw(rt, "biasExpected") {
					ack.ackID = nextIDs[sid] + int64(len(model[sid]))
					for i := range model {
						if len(model[i]) > 0 && model[i][0].ackID == nextIDs[i] {
							ack.logPosition = model[i][0].logPosition
							break
						}
					}
				}
				accepted := a.serviceAcks.offer(sid, ack)
				require.Equal(rt, inRange, accepted)
				if inRange {
					model[sid] = append(model[sid], ack)
				}

			case serviceAckOpHasReached:
				logPos := rapid.Int64().Draw(rt, "drawnPos")
				// Mix drawn positions with the first entry position of queue 0.
				if rapid.Bool().Draw(rt, "checkExpected") && serviceCount > 0 && len(model[0]) > 0 {
					logPos = model[0][0].logPosition
				}
				checkHasReached(logPos)

			case serviceAckOpRemoveHead:
				// Call only when the model says every first entry matches.
				ok, err := modelHasReached(model, nextIDs, removePos)
				require.True(rt, ok)
				require.NoError(rt, err)
				reachedOK, reachedErr := a.hasReachedServiceAcks(removePos)
				require.True(rt, reachedOK)
				require.NoError(rt, reachedErr)

				a.removeServiceAckHead()
				for i := range model {
					if len(model[i]) > 0 {
						model[i] = model[i][1:]
						nextIDs[i]++
						modelSkipDropped(nextIDs, dropped, i)
					}
				}
				popCount++

			case serviceAckOpNoteDropped:
				sid := rapid.Int32Range(-2, int32(serviceCount)+1).Draw(rt, "dropServiceID")
				inRange := sid >= 0 && int(sid) < serviceCount
				ackID := rapid.Int64Range(-1, 8).Draw(rt, "dropAckID")
				if inRange {
					// Bias to ids around the next id of the service.
					ackID = nextIDs[sid] + rapid.Int64Range(-1, 2).Draw(rt, "dropAckIDOffset")
				}
				a.serviceAcks.noteDropped(sid, ackID)
				if inRange {
					modelNoteDropped(nextIDs, dropped, int(sid), ackID)
				}

			case serviceAckOpDropAt:
				pos := rapid.Int64().Draw(rt, "dropPos")
				if serviceCount > 0 {
					sid := rapid.IntRange(0, serviceCount-1).Draw(rt, "dropAtServiceID")
					if len(model[sid]) > 0 {
						pos = model[sid][0].logPosition
					}
				}
				a.serviceAcks.dropAt(pos, nil)
				for i := range model {
					kept := model[i][:0]
					for _, ack := range model[i] {
						if ack.logPosition == pos {
							modelNoteDropped(nextIDs, dropped, i, ack.ackID)
							continue
						}
						kept = append(kept, ack)
					}
					model[i] = kept
				}

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

		checkHasReached(rapid.Int64().Draw(rt, "finalPos"))
	})
}
