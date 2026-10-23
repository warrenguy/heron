//go:build rapid && testhooks

package consensus

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

type serviceControlOpKind int

const (
	serviceControlOpPublish serviceControlOpKind = iota
	serviceControlOpAckNext
	serviceControlOpWrongAck
	serviceControlOpWork
)

// Asserts wait sequencing against a model service that acks every published
// coordination point in publish order (every delivered frame). Ops: publish a
// point, advance one service cursor and inject its next ack, inject a wrong-pair
// ack, run one serviceControlWork tick.
//
// Invariants each step: waits complete in FIFO order only when every service
// has acked the first wait pair; ExpectedServiceAckID advances once per
// completed wait; PendingServiceAckWaits matches the model; after any wrong-pair
// injection the failure marks once and no later wait completes.
func TestServiceControl_waitSequencing_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		serviceCount := rapid.IntRange(1, 3).Draw(rt, "serviceCount")
		a := NewAgent(AgentConfig{ServiceCount: serviceCount})
		// Outer t: rapid.T is not testing.TB (missing ArtifactDir).
		p, _ := boundServiceProxy(t)
		a.SetServiceProxy(p)
		q := &QueueConsensusIngress{}
		a.SetServiceIngress(q)

		// modelWaits is the FIFO of published positions still unconsumed.
		modelWaits := make([]int64, 0)
		// delivered is the full publish-order stream each service walks.
		delivered := make([]int64, 0)
		// cursor[sid] is how many delivered frames service sid has acked.
		cursor := make([]int, serviceCount)
		expectedID := int64(0)
		// failed latches after ErrServiceAckOutOfSequence (marks once). Model
		// then expects no further wait progress.
		failed := false

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, len(modelWaits), a.PendingServiceAckWaits())
			require.Equal(rt, expectedID, a.ExpectedServiceAckID())
			if len(modelWaits) == 0 {
				require.Equal(rt, int64(nullValue), a.ExpectedAckPosition())
			} else {
				require.Equal(rt, modelWaits[0], a.ExpectedAckPosition())
			}
			if failed {
				require.ErrorIs(rt, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			} else {
				require.NoError(rt, a.ServiceAckFailure())
			}
		}

		// everyServiceAckedHead reports whether every service has acked index
		// expectedID (the first wait pair).
		everyServiceAckedHead := func() bool {
			if len(modelWaits) == 0 || failed {
				return false
			}
			for sid := range serviceCount {
				if cursor[sid] <= int(expectedID) {
					return false
				}
			}
			return true
		}

		nOps := rapid.IntRange(1, 40).Draw(rt, "nOps")
		for range nOps {
			kinds := []serviceControlOpKind{
				serviceControlOpPublish,
				serviceControlOpAckNext,
				serviceControlOpWork,
			}
			// Wrong-pair only until failure marks once.
			if !failed {
				kinds = append(kinds, serviceControlOpWrongAck)
			}
			kind := rapid.SampledFrom(kinds).Draw(rt, "kind")

			switch kind {
			case serviceControlOpPublish:
				pos := rapid.Int64Range(1, 1_000_000).Draw(rt, "pos")
				for slices.Contains(delivered, pos) {
					pos = rapid.Int64Range(1, 1_000_000).Draw(rt, "posRetry")
				}
				if failed {
					err := a.RequestServiceAck(pos)
					require.ErrorIs(rt, err, ErrServiceAckOutOfSequence)
				} else {
					require.NoError(rt, a.RequestServiceAck(pos))
					modelWaits = append(modelWaits, pos)
					delivered = append(delivered, pos)
				}

			case serviceControlOpAckNext:
				if failed || len(delivered) == 0 {
					assertInvariants()
					continue
				}
				sid := rapid.IntRange(0, serviceCount-1).Draw(rt, "sid")
				if cursor[sid] >= len(delivered) {
					assertInvariants()
					continue
				}
				idx := cursor[sid]
				q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
					LogPosition: delivered[idx],
					AckID:       int64(idx),
					RelevantID:  int64(sid + 1),
					ServiceID:   int32(sid),
				}))
				require.Equal(rt, 1, a.pollServiceControl())
				cursor[sid]++

			case serviceControlOpWrongAck:
				// Need a first wait and an empty service-0 queue so the wrong
				// pair becomes the first entry (not behind a matching ack).
				if len(modelWaits) == 0 || a.ServiceAckQueueDepth(0) != 0 {
					assertInvariants()
					continue
				}
				// Poison service 0 first entry so hasReached hits a mismatch
				// before any empty lower-id queue. Wait site marks once.
				wrongPos := rapid.Int64Range(1_000_001, 2_000_000).Draw(rt, "wrongPos")
				q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
					LogPosition: wrongPos,
					AckID:       expectedID,
					RelevantID:  99,
					ServiceID:   0,
				}))
				require.Equal(rt, 1, a.pollServiceControl())
				// Receive-time check uses first-entry position (wrongPos). With
				// empty higher services it returns partial, no mark. Wait site
				// checks the first wait position and marks.
				_ = a.serviceControlWork()
				require.ErrorIs(rt, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
				failed = true

			case serviceControlOpWork:
				beforeWaits := a.PendingServiceAckWaits()
				beforeID := a.ExpectedServiceAckID()
				work := a.serviceControlWork()
				if !failed && everyServiceAckedHead() {
					require.Equal(rt, 1, work)
					require.Equal(rt, beforeWaits-1, a.PendingServiceAckWaits())
					require.Equal(rt, beforeID+1, a.ExpectedServiceAckID())
					modelWaits = modelWaits[1:]
					expectedID++
				} else {
					// Marked failure or partial first entries: no wait completes.
					require.Equal(rt, beforeWaits, a.PendingServiceAckWaits())
					require.Equal(rt, beforeID, a.ExpectedServiceAckID())
					require.Equal(rt, 0, work)
				}
			}
			assertInvariants()
		}
	})
}

type replayJoinOpKind int

const (
	replayJoinOpRequestServiceAck replayJoinOpKind = iota
	replayJoinOpEarlierJoinLog
	replayJoinOpPublish
	replayJoinOpRetry
	replayJoinOpAckNext
	replayJoinOpWork
	replayJoinOpMarkFailure
	replayJoinOpUnbind
)

type replayJoinPublishOutcome int

const (
	replayJoinPublished replayJoinPublishOutcome = iota
	replayJoinRetriable
	replayJoinTerminal
)

type replayJoinModelState int

const (
	replayJoinModelNone replayJoinModelState = iota
	replayJoinModelUnpublished
	replayJoinModelPublished
)

// replayJoinRetiredStart selects how a snapshot wait retires before the ops.
type replayJoinRetiredStart int

const (
	replayJoinRetiredNone replayJoinRetiredStart = iota
	// replayJoinRetiredOwed retires the wait before the snapshot ack arrives.
	replayJoinRetiredOwed
	// replayJoinRetiredDropped retires the wait after the snapshot ack
	// reached the tracker.
	replayJoinRetiredDropped
)

// replayJoinModelWait is one queued wait in the model FIFO. replay marks the
// replay JoinLog wait.
type replayJoinModelWait struct {
	position int64
	replay   bool
}

// Asserts replay JoinLog tracking against a FIFO model with ServiceCount 1.
// The model service acks every published coordination point in publish
// order. serviceAckFailure marks at most once, late in the run.
//
// The setup may retire a snapshot wait at one of the op positions before the
// ops. The model service read that action first, so it sends its snapshot ack
// first. That ack either reached the tracker before the retire or arrives
// during the ops.
//
// Ops:
//   - queue a RequestServiceAck wait
//   - publish or store an earlier JoinLog
//   - publish the replay JoinLog with a drawn outcome
//   - retry a pending publish
//   - ack the next point
//   - run one serviceControlWork tick
//   - UnbindReplayLog
//
// Invariants each step:
//   - IsReplayLogJoined matches the model, or reports true after the
//     failure mark.
//   - IsReplayLogJoined stays false until the model service sends the
//     replay JoinLog ack, unless the failure mark is set. The check uses the
//     delivered index, not an ack id, so a skipped id at a retire does not
//     affect it.
//   - Queued wait positions match the model. Positions repeat, so a
//     position match alone does not cover the count.
//   - HasPendingJoinLog matches the model.
func TestReplayJoinTracking_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		a := NewAgent(AgentConfig{ServiceCount: 1})
		q := &QueueConsensusIngress{}
		a.SetServiceIngress(q)
		// Outer t: rapid.T is not testing.TB (missing ArtifactDir).
		connected, _ := boundServiceProxy(t)
		a.SetServiceProxy(connected)
		disconnected := boundServiceProxyWithLimit(t, 0, false)
		// A closed publication returns a terminal code on every offer.
		closed, closedPub := boundServiceProxy(t)
		require.NoError(rt, closedPub.Close())

		modelWaits := make([]replayJoinModelWait, 0)
		// delivered holds every point that queued a wait, in publish order.
		delivered := make([]int64, 0)
		cursor := 0
		expectedID := int64(0)
		state := replayJoinModelNone
		failed := false
		// hasPending and pendingPos model any stored pending JoinLog.
		hasPending := false
		pendingPos := int64(0)
		// replayIndex is the delivered index of the replay JoinLog wait, or -1.
		replayIndex := -1

		clearReplayMarks := func() {
			for i := range modelWaits {
				modelWaits[i].replay = false
			}
			replayIndex = -1
		}
		queueWait := func(position int64, replay bool) {
			if failed {
				return
			}
			modelWaits = append(modelWaits, replayJoinModelWait{position: position, replay: replay})
			delivered = append(delivered, position)
		}
		// modelPublished applies a successful replay JoinLog publish.
		modelPublished := func(position int64) {
			hasPending = false
			if failed {
				state = replayJoinModelNone
				return
			}
			state = replayJoinModelPublished
			replayIndex = len(delivered)
			queueWait(position, true)
		}
		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, failed, a.ServiceAckFailure() != nil)
			require.Equal(rt, state == replayJoinModelNone || failed, a.IsReplayLogJoined())
			replayAckUnsent := replayIndex >= 0 && cursor <= replayIndex
			if replayAckUnsent && !failed {
				require.False(rt, a.IsReplayLogJoined())
			}
			positions := make([]int64, 0, len(modelWaits))
			for _, w := range modelWaits {
				positions = append(positions, w.position)
			}
			require.Equal(rt, positions, append([]int64{}, a.serviceAckWaits...))
			require.Equal(rt, hasPending, a.HasPendingJoinLog())
			if hasPending {
				require.Equal(rt, pendingPos, a.pendingJoinLog.LogPosition)
			}
			require.Equal(rt, expectedID, a.ExpectedServiceAckID())
		}

		positions := []int64{64, 128, 192}

		retired := rapid.SampledFrom([]replayJoinRetiredStart{
			replayJoinRetiredNone,
			replayJoinRetiredOwed,
			replayJoinRetiredDropped,
		}).Draw(rt, "retired")
		if retired != replayJoinRetiredNone {
			retiredPos := rapid.SampledFrom(positions).Draw(rt, "retiredPos")
			a.queueSnapshotWait(retiredPos, 0, 0)
			// The snapshot ack takes delivered index 0 and ack id 0.
			delivered = append(delivered, retiredPos)
			if retired == replayJoinRetiredDropped {
				enqueueServiceAck(t, q, retiredPos, 0, 0)
				require.Equal(rt, 1, a.pollServiceControl())
				cursor = 1
			}
			_, prepared, err := a.PrepareForNewLeadership(0, 0)
			require.NoError(rt, err)
			require.True(rt, prepared)
			if retired == replayJoinRetiredDropped {
				// The retire drops the ack and skips its id.
				expectedID = 1
			}
			assertInvariants()
		}

		nOps := rapid.IntRange(1, 40).Draw(rt, "nOps")
		// The failure mark holds for the rest of the run, so failAt sits in
		// the second half. failAt == nOps marks no failure.
		failAt := rapid.IntRange(nOps/2, nOps).Draw(rt, "failAt")
		for i := range nOps {
			kind := replayJoinOpMarkFailure
			if i != failAt {
				kind = rapid.SampledFrom([]replayJoinOpKind{
					replayJoinOpRequestServiceAck,
					replayJoinOpEarlierJoinLog,
					replayJoinOpPublish,
					replayJoinOpRetry,
					replayJoinOpAckNext,
					replayJoinOpWork,
					replayJoinOpUnbind,
				}).Draw(rt, "kind")
			}

			switch kind {
			case replayJoinOpRequestServiceAck:
				pos := rapid.SampledFrom(positions).Draw(rt, "requestPos")
				a.SetServiceProxy(connected)
				err := a.RequestServiceAck(pos)
				if failed {
					require.ErrorIs(rt, err, ErrServiceAckOutOfSequence)
				} else {
					require.NoError(rt, err)
					queueWait(pos, false)
				}

			case replayJoinOpEarlierJoinLog:
				// No earlier JoinLog publishes while the replay JoinLog stays
				// unpublished.
				if state == replayJoinModelUnpublished {
					break
				}
				pos := rapid.SampledFrom(positions).Draw(rt, "joinPos")
				stored := rapid.Bool().Draw(rt, "joinStored")
				fields := sampleJoinLogFields()
				fields.LogPosition = pos
				if stored {
					a.SetServiceProxy(disconnected)
					require.Equal(rt, 0, a.publishOrQueueJoinLog(fields))
					hasPending = true
					pendingPos = pos
					break
				}
				a.SetServiceProxy(connected)
				require.Equal(rt, 1, a.publishOrQueueJoinLog(fields))
				hasPending = false
				queueWait(pos, false)

			case replayJoinOpPublish:
				pos := rapid.SampledFrom(positions).Draw(rt, "replayPos")
				outcome := rapid.SampledFrom([]replayJoinPublishOutcome{
					replayJoinPublished,
					replayJoinRetriable,
					replayJoinTerminal,
				}).Draw(rt, "outcome")
				switch outcome {
				case replayJoinPublished:
					a.SetServiceProxy(connected)
				case replayJoinRetriable:
					a.SetServiceProxy(disconnected)
				case replayJoinTerminal:
					a.SetServiceProxy(closed)
				}
				a.PublishReplayJoinLog(ReplayLogJoin{StartPosition: pos, StopPosition: pos + 4096})
				// PublishReplayJoinLog restarts tracking. No earlier wait stays
				// the replay JoinLog wait.
				clearReplayMarks()
				switch outcome {
				case replayJoinPublished:
					modelPublished(pos)
				case replayJoinRetriable:
					state = replayJoinModelUnpublished
					hasPending = true
					pendingPos = pos
				case replayJoinTerminal:
					require.Error(rt, a.ServicePublishFailure())
					state = replayJoinModelNone
					hasPending = false
				}

			case replayJoinOpRetry:
				a.SetServiceProxy(connected)
				if !hasPending {
					require.Equal(rt, 0, a.retryPendingJoinLog())
					break
				}
				require.Equal(rt, 1, a.retryPendingJoinLog())
				if state == replayJoinModelUnpublished {
					modelPublished(pendingPos)
					break
				}
				hasPending = false
				queueWait(pendingPos, false)

			case replayJoinOpAckNext:
				if failed || cursor >= len(delivered) {
					break
				}
				enqueueServiceAck(t, q, delivered[cursor], int64(cursor), 0)
				require.Equal(rt, 1, a.pollServiceControl())
				if retired == replayJoinRetiredOwed && cursor == 0 {
					// The owed snapshot ack drops, even with a wait queued at
					// its position. The drop skips its id.
					expectedID = 1
				}
				cursor++

			case replayJoinOpWork:
				work := a.serviceControlWork()
				if failed || len(modelWaits) == 0 || cursor <= int(expectedID) {
					require.Equal(rt, 0, work)
					break
				}
				require.Equal(rt, 1, work)
				if modelWaits[0].replay {
					state = replayJoinModelNone
				}
				modelWaits = modelWaits[1:]
				expectedID++

			case replayJoinOpMarkFailure:
				a.serviceAckFailure = ErrServiceAckOutOfSequence
				failed = true

			case replayJoinOpUnbind:
				a.UnbindReplayLog(nil)
				// Only an unpublished replay JoinLog drops. A stored earlier
				// JoinLog stays.
				if state == replayJoinModelUnpublished {
					hasPending = false
				}
				clearReplayMarks()
				state = replayJoinModelNone
			}
			assertInvariants()
		}
	})
}
