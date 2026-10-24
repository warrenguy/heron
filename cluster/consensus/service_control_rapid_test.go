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

type retiredAckOpKind int

const (
	retiredAckOpControl retiredAckOpKind = iota
	retiredAckOpSnapshot
	retiredAckOpAck
	retiredAckOpSwap
	retiredAckOpWork
	retiredAckOpPrepare
	retiredAckOpTruncate
	retiredAckOpFault
)

type retiredAckFaultShape int

const (
	// retiredAckFaultPosition sends the next id at a position that a frame
	// never uses.
	retiredAckFaultPosition retiredAckFaultShape = iota
	// retiredAckFaultPreviousID sends the previous id at the next frame
	// position: a repeated or stale id.
	retiredAckFaultPreviousID
	// retiredAckFaultSkippedID sends the next id plus one at the next frame
	// position.
	retiredAckFaultSkippedID
)

// retiredAckFrame is one coordination frame in the retired ack model.
type retiredAckFrame struct {
	position int64
	snapshot bool
}

// Asserts per-service ack ids over random control waits, snapshot waits,
// prepares that retire the snapshot wait, and per-service delivery orders.
// Ack id i of service s names frame order[s][i]. A late delivery swaps a
// retired snapshot frame with the control frame after it. A swap never pairs
// frames at one position. A control wait may reuse a retired position after
// the retired frame. A truncate below a retired position removes every
// unacked retired snapshot frame above it from each service's order. One
// optional fault sends a bad ack at a non-retired position.
//
// Invariants each step:
//   - Before a fault, a failure never marks.
//   - ExpectedServiceAckID never decreases. Before a fault, it never exceeds
//     the lowest cursor.
//   - Each expected id is at most the cursor of its service. Each dropped id
//     sits above the expected id of its service.
//   - A pending snapshot wait position sits once in the FIFO.
//
// A final drain queues one control wait and each service acks every frame in
// its order. Without a fault every wait pops and the expected id of each
// service reaches its own frame count. With a fault the failure marks.
func TestServiceControl_retiredSnapshotAcks_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		serviceCount := rapid.IntRange(1, 3).Draw(rt, "serviceCount")
		// Outer t: rapid.T is not testing.TB (missing ArtifactDir).
		a, q, _ := agentWithServiceControl(t, serviceCount)
		a.SetLogTruncateControl(&fakeLogTruncateControl{})
		a.SetLogRecordingID(77)

		var frames []retiredAckFrame
		order := make([][]int, serviceCount)
		cursor := make([]int, serviceCount)
		var retired []int64
		nextPosition := int64(0)
		faulted := false
		lastExpectedID := int64(0)

		freshPosition := func() int64 {
			nextPosition += 64
			return nextPosition
		}
		addFrame := func(frame retiredAckFrame) {
			frames = append(frames, frame)
			for sid := range order {
				order[sid] = append(order[sid], len(frames)-1)
			}
		}
		queueControl := func(position int64) {
			rt.Helper()
			fields := sampleJoinLogFields()
			fields.LogPosition = position
			require.Equal(rt, 1, a.publishOrQueueJoinLog(fields))
			addFrame(retiredAckFrame{position: position})
		}
		isRetired := func(frame retiredAckFrame) bool {
			return frame.snapshot && slices.Contains(retired, frame.position)
		}
		ack := func(sid int) {
			rt.Helper()
			frame := frames[order[sid][cursor[sid]]]
			enqueueServiceAck(t, q, frame.position, int64(cursor[sid]), int32(sid))
			require.Equal(rt, 1, a.pollServiceControl())
			cursor[sid]++
		}
		requireInvariants := func(step string) {
			rt.Helper()
			expectedID := a.ExpectedServiceAckID()
			require.GreaterOrEqual(rt, expectedID, lastExpectedID, step)
			lastExpectedID = expectedID
			if !faulted {
				require.NoError(rt, a.ServiceAckFailure(), step)
				require.LessOrEqual(rt, expectedID, int64(slices.Min(cursor)), step)
			}
			for sid := range serviceCount {
				next := a.serviceAcks.nextAckIDs[sid]
				require.LessOrEqual(rt, next, int64(cursor[sid]), step)
				for _, id := range a.serviceAcks.droppedAckIDs[sid] {
					require.Greater(rt, id, next, step)
				}
			}
			if wait := a.snapshotWaitPosition; wait != 0 {
				count := 0
				for _, w := range a.serviceAckWaits {
					if w == wait {
						count++
					}
				}
				require.Equal(rt, 1, count, step)
			}
		}

		opNames := []string{"control", "snapshot", "ack", "swap", "work", "prepare", "truncate", "fault"}
		steps := rapid.IntRange(1, 60).Draw(rt, "steps")
		for range steps {
			ops := []retiredAckOpKind{
				retiredAckOpControl,
				retiredAckOpSnapshot,
				retiredAckOpAck,
				retiredAckOpAck,
				retiredAckOpAck,
				retiredAckOpSwap,
				retiredAckOpWork,
				retiredAckOpPrepare,
				retiredAckOpPrepare,
				retiredAckOpTruncate,
			}
			if !faulted {
				ops = append(ops, retiredAckOpFault)
			}
			op := rapid.SampledFrom(ops).Draw(rt, "op")
			sid := rapid.IntRange(0, serviceCount-1).Draw(rt, "serviceID")
			switch op {
			case retiredAckOpControl:
				position := freshPosition()
				reuse := len(a.serviceAckWaits) == 0 && len(retired) > 0 &&
					rapid.Bool().Draw(rt, "reuseRetired")
				if reuse {
					position = rapid.SampledFrom(retired).Draw(rt, "retiredPosition")
				}
				queueControl(position)

			case retiredAckOpSnapshot:
				// handleClusterAction does not queue a wait after the failure
				// mark.
				if a.snapshotWaitPosition != 0 || a.ServiceAckFailure() != nil {
					break
				}
				position := freshPosition()
				a.queueSnapshotWait(position, 0, 0)
				addFrame(retiredAckFrame{position: position, snapshot: true})

			case retiredAckOpAck:
				if cursor[sid] < len(order[sid]) {
					ack(sid)
				}

			case retiredAckOpSwap:
				k := cursor[sid]
				if k+1 >= len(order[sid]) {
					break
				}
				first, second := frames[order[sid][k]], frames[order[sid][k+1]]
				if isRetired(first) && !second.snapshot && second.position != first.position {
					order[sid][k], order[sid][k+1] = order[sid][k+1], order[sid][k]
				}

			case retiredAckOpWork:
				a.serviceControlWork()

			case retiredAckOpPrepare:
				if pending := a.snapshotWaitPosition; pending != 0 {
					retired = append(retired, pending)
				}
				_, prepared, err := a.PrepareForNewLeadership(0, 1)
				require.NoError(rt, err)
				require.True(rt, prepared)

			case retiredAckOpTruncate:
				if len(retired) == 0 {
					break
				}
				above := rapid.SampledFrom(retired).Draw(rt, "truncateAbove")
				position := rapid.Int64Range(0, above/64-1).Draw(rt, "truncateSlot") * 64
				require.NoError(rt, a.TruncateLogEntry(0, position))
				// The service never reads a truncated frame, so it never
				// acks one. Later ack ids of the service shift down.
				for service := range order {
					acked := order[service][:cursor[service]]
					unacked := slices.DeleteFunc(
						slices.Clone(order[service][cursor[service]:]),
						func(frameIndex int) bool {
							frame := frames[frameIndex]
							return isRetired(frame) && frame.position > position
						},
					)
					order[service] = slices.Concat(acked, unacked)
				}

			case retiredAckOpFault:
				shape := rapid.SampledFrom([]retiredAckFaultShape{
					retiredAckFaultPosition,
					retiredAckFaultPreviousID,
					retiredAckFaultSkippedID,
				}).Draw(rt, "faultShape")
				ackID := int64(cursor[sid])
				// An odd position is never a frame position.
				position := nextPosition + 1
				if shape != retiredAckFaultPosition {
					// The drop path accepts any ack at a retired position.
					// Id faults need a control frame at a non-retired
					// position.
					if cursor[sid] >= len(order[sid]) {
						break
					}
					frame := frames[order[sid][cursor[sid]]]
					if frame.snapshot || slices.Contains(retired, frame.position) {
						break
					}
					position = frame.position
					if shape == retiredAckFaultPreviousID {
						if ackID == 0 {
							break
						}
						ackID--
					} else {
						ackID++
					}
				}
				enqueueServiceAck(t, q, position, ackID, int32(sid))
				require.Equal(rt, 1, a.pollServiceControl())
				faulted = true
			}
			requireInvariants(opNames[op])
		}

		// Final drain: a closing control wait makes the walk reach any faulty
		// entry. Each service acks every remaining frame.
		queueControl(freshPosition())
		for sid := range serviceCount {
			for cursor[sid] < len(order[sid]) {
				ack(sid)
				a.serviceControlWork()
				requireInvariants("final drain")
			}
		}
		for range len(a.serviceAckWaits) + 1 {
			a.serviceControlWork()
		}
		requireInvariants("final drain")

		if faulted {
			require.ErrorIs(rt, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			return
		}
		require.Empty(rt, a.serviceAckWaits)
		lowestFrameCount := int64(len(order[0]))
		for sid := range serviceCount {
			frameCount := int64(len(order[sid]))
			lowestFrameCount = min(lowestFrameCount, frameCount)
			require.Equal(rt, 0, a.ServiceAckQueueDepth(int32(sid)))
			require.Equal(rt, frameCount, a.serviceAcks.nextAckIDs[sid])
			require.Empty(rt, a.serviceAcks.droppedAckIDs[sid])
		}
		require.Equal(rt, lowestFrameCount, a.ExpectedServiceAckID())
		require.NoError(rt, a.ServiceAckFailure())
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
