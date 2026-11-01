//go:build testhooks

package consensus

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func sampleReplayLogJoin(startPosition, stopPosition int64) ReplayLogJoin {
	return ReplayLogJoin{
		StartPosition: startPosition,
		StopPosition:  stopPosition,
		LogSessionID:  -17,
		LogStreamID:   103,
		LogChannel:    "aeron:ipc?session-id=-17",
	}
}

// ackAndPopWait queues one ack at logPosition from service 0. It asserts that
// the next serviceControlWork call pops a wait. One snapshotTakeWork call
// then resolves a module take that the pop built. The fixtures do not wire
// a recording control.
func ackAndPopWait(
	t *testing.T,
	a *Agent,
	q *QueueConsensusIngress,
	logPosition, ackID int64,
) {
	t.Helper()
	enqueueServiceAck(t, q, logPosition, ackID, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	a.snapshotTakeWork(0)
}

// retiredReplayAgent returns a one-service agent with a connected service
// proxy whose snapshot wait at position retired in PrepareForNewLeadership.
// ackBeforePrepare queues the snapshot ack before the prepare, so the retire
// drops the ack.
func retiredReplayAgent(
	t *testing.T,
	position int64,
	ackBeforePrepare bool,
) (*Agent, *QueueConsensusIngress) {
	t.Helper()
	a, q, _ := agentWithServiceControl(t, 1)
	a.queueSnapshotWait(position, 0, 0)
	if ackBeforePrepare {
		enqueueServiceAck(t, q, position, 0, 0)
		require.Equal(t, 1, a.pollServiceControl())
	}
	_, prepared, err := a.PrepareForNewLeadership(position, 0)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 0, a.PendingServiceAckWaits())
	return a, q
}

func TestPublishReplayJoinLog(t *testing.T) {
	t.Parallel()

	const (
		startPosition int64 = 4096
		stopPosition  int64 = 8192
	)
	// retiredStartLateAck publishes at a retired start. The service sends its
	// snapshot ack and its join ack at the start, in either order. The module
	// sees two acks with ids in send order. Only a snapshot ack sent first can
	// arrive before the publish. firstBeforePublish sends it there.
	retiredStartLateAck := func(t *testing.T, firstBeforePublish bool) {
		a, q := retiredReplayAgent(t, startPosition, false)
		startID := a.ExpectedServiceAckID()
		sendFirst := func() {
			enqueueServiceAck(t, q, startPosition, startID, 0)
			require.Equal(t, 1, a.pollServiceControl())
		}
		if firstBeforePublish {
			sendFirst()
		}
		a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
		if !firstBeforePublish {
			sendFirst()
		}

		// The first ack drops. The replay JoinLog wait stays.
		require.Equal(t, 0, a.serviceControlWork())
		require.Equal(t, 1, a.PendingServiceAckWaits())
		require.False(t, a.IsReplayLogJoined())

		// The second ack pops the replay JoinLog wait.
		ackAndPopWait(t, a, q, startPosition, startID+1)
		require.True(t, a.IsReplayLogJoined())
		require.Equal(t, startID+2, a.ExpectedServiceAckID())
		require.NoError(t, a.ServiceAckFailure())
	}
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "nil proxy",
			run: func(t *testing.T) {
				a, _ := agentWithServiceIngress(1)
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.False(t, a.HasPendingJoinLog())
			},
		},
		{
			name: "published at once with an empty FIFO",
			run: func(t *testing.T) {
				a, q, pub := agentWithServiceControl(t, 1)
				a.thisMemberID = 2
				join := sampleReplayLogJoin(startPosition, stopPosition)
				a.PublishReplayJoinLog(join)

				require.Equal(t, codecs.JoinLogFields{
					LogPosition:    startPosition,
					MaxLogPosition: stopPosition,
					MemberID:       2,
					LogSessionID:   join.LogSessionID,
					LogStreamID:    join.LogStreamID,
					IsStartup:      clusterwire.BooleanType_TRUE,
					Role:           RoleFollower.Code(),
					IsStandby:      clusterwire.BooleanType_FALSE,
					LogChannel:     []byte(join.LogChannel),
				}, decodeJoinLogFromPub(t, pub))
				require.Equal(t, 1, a.PendingServiceAckWaits())
				require.Equal(t, startPosition, a.ExpectedAckPosition())
				require.False(t, a.IsReplayLogJoined())

				ackAndPopWait(t, a, q, startPosition, 0)
				require.True(t, a.IsReplayLogJoined())
			},
		},
		{
			name: "an earlier wait queued",
			run: func(t *testing.T) {
				// An earlier replay attempt leaves a wait at the same position.
				a, q, _ := agentWithServiceControl(t, 1)
				require.NoError(t, a.RequestServiceAck(startPosition))
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.Equal(t, 2, a.PendingServiceAckWaits())
				require.False(t, a.IsReplayLogJoined())

				// Phase 1: the earlier wait pops.
				ackAndPopWait(t, a, q, startPosition, 0)
				require.False(t, a.IsReplayLogJoined())

				// Phase 2: the replay JoinLog wait pops.
				ackAndPopWait(t, a, q, startPosition, 1)
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, 0, a.PendingServiceAckWaits())
			},
		},
		{
			name: "retriable then published by retryPendingJoinLog",
			run: func(t *testing.T) {
				a, q, _ := agentWithServiceControl(t, 1)
				require.NoError(t, a.RequestServiceAck(100))

				// Phase 1: a disconnected proxy stores the replay JoinLog.
				a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.True(t, a.HasPendingJoinLog())
				require.Equal(t, 1, a.PendingServiceAckWaits())
				require.False(t, a.IsReplayLogJoined())

				// Phase 2: the earlier wait pops before the publish.
				ackAndPopWait(t, a, q, 100, 0)
				require.False(t, a.IsReplayLogJoined())

				// Phase 3: the retry publishes. Tracking starts here.
				connected, pub := boundServiceProxy(t)
				a.SetServiceProxy(connected)
				require.Equal(t, 1, a.retryPendingJoinLog())
				require.False(t, a.HasPendingJoinLog())
				require.Equal(t, startPosition, decodeJoinLogFromPub(t, pub).LogPosition)
				require.Equal(t, 1, a.PendingServiceAckWaits())
				require.False(t, a.IsReplayLogJoined())

				ackAndPopWait(t, a, q, startPosition, 1)
				require.True(t, a.IsReplayLogJoined())
			},
		},
		{
			name: "terminal code",
			run: func(t *testing.T) {
				a, _, pub := agentWithServiceControl(t, 1)
				require.NoError(t, pub.Close())
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.Error(t, a.ServicePublishFailure())
				require.False(t, a.HasPendingJoinLog())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.True(t, a.IsReplayLogJoined())
			},
		},
		{
			name: "serviceAckFailure marked",
			run: func(t *testing.T) {
				a, _, pub := agentWithServiceControl(t, 1)
				require.NoError(t, a.RequestServiceAck(100))
				a.serviceAckFailure = ErrServiceAckOutOfSequence
				before := pub.Position()
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))

				// The JoinLog frame publishes after the request frame.
				require.Greater(t, pub.Position(), before)
				require.Equal(t, 0, a.replayJoinWaitsAhead)
				require.False(t, a.HasPendingJoinLog())
				require.Equal(t, 1, a.PendingServiceAckWaits())
				require.True(t, a.IsReplayLogJoined())
			},
		},
		{
			name: "UnbindReplayLog with the JoinLog still pending",
			run: func(t *testing.T) {
				a, _ := agentWithServiceIngress(1)
				a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.True(t, a.HasPendingJoinLog())
				require.False(t, a.IsReplayLogJoined())

				a.UnbindReplayLog(nil)
				require.False(t, a.HasPendingJoinLog())
				require.True(t, a.IsReplayLogJoined())

				// A connected proxy receives no frame after the unbind.
				connected, pub := boundServiceProxy(t)
				a.SetServiceProxy(connected)
				require.Equal(t, 0, a.retryPendingJoinLog())
				require.Equal(t, int64(0), pub.Position())
				require.Equal(t, 0, a.PendingServiceAckWaits())
			},
		},
		{
			name: "UnbindReplayLog after the publish keeps the wait",
			run: func(t *testing.T) {
				a, q, _ := agentWithServiceControl(t, 1)
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.False(t, a.IsReplayLogJoined())

				a.UnbindReplayLog(nil)
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, 1, a.PendingServiceAckWaits())

				// The services still ack the published JoinLog once.
				ackAndPopWait(t, a, q, startPosition, 0)
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "PrepareForNewLeadership with the JoinLog still pending",
			run: func(t *testing.T) {
				// The election re-enters INIT before the replay runner closes.
				a, _ := agentWithServiceIngress(1)
				a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.True(t, a.HasPendingJoinLog())
				require.False(t, a.IsReplayLogJoined())

				_, prepared, err := a.PrepareForNewLeadership(startPosition, 0)
				require.NoError(t, err)
				require.True(t, prepared)
				require.False(t, a.HasPendingJoinLog())
				require.True(t, a.IsReplayLogJoined())

				// A connected proxy receives no frame after the prepare.
				connected, pub := boundServiceProxy(t)
				a.SetServiceProxy(connected)
				require.Equal(t, 0, a.retryPendingJoinLog())
				require.Equal(t, int64(0), pub.Position())
			},
		},
		{
			name: "PrepareForNewLeadership after the publish keeps the wait",
			run: func(t *testing.T) {
				a, q, _ := agentWithServiceControl(t, 1)
				require.NoError(t, a.RequestServiceAck(100))
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.Equal(t, 2, a.PendingServiceAckWaits())
				require.False(t, a.IsReplayLogJoined())

				_, prepared, err := a.PrepareForNewLeadership(startPosition, 0)
				require.NoError(t, err)
				require.True(t, prepared)
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, 2, a.PendingServiceAckWaits())

				// Both waits still pop. No pop lowers the count below 0.
				ackAndPopWait(t, a, q, 100, 0)
				ackAndPopWait(t, a, q, startPosition, 1)
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, 0, a.replayJoinWaitsAhead)
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "retired start with the snapshot ack before the publish",
			run: func(t *testing.T) {
				retiredStartLateAck(t, true)
			},
		},
		{
			name: "retired start with both acks after the publish",
			run: func(t *testing.T) {
				retiredStartLateAck(t, false)
			},
		},
		{
			name: "retired start with the ack dropped at retire",
			run: func(t *testing.T) {
				a, q := retiredReplayAgent(t, startPosition, true)
				startID := a.ExpectedServiceAckID()
				a.PublishReplayJoinLog(sampleReplayLogJoin(startPosition, stopPosition))
				require.False(t, a.IsReplayLogJoined())

				// The join ack alone pops the replay JoinLog wait.
				ackAndPopWait(t, a, q, startPosition, startID)
				require.True(t, a.IsReplayLogJoined())
				require.Equal(t, startID+1, a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// replayLogFollower returns a follower with serviceCount services, a module
// adapter with no image, and an unbound replay image.
func replayLogFollower(
	t *testing.T,
	serviceCount int,
) (*Agent, *QueueConsensusIngress, *aeron.Image) {
	t.Helper()
	a, q := newFollowerActionAgent(0, NewLogAdapter(0), serviceCount)
	sub := aeron.NewTestSubscriptionForHooks(t)
	return a, q, aeron.NewTestImageForHooks(t, sub, 0)
}

func TestReplayLogPoll_moduleState(t *testing.T) {
	t.Parallel()

	const nowNs int64 = 555
	for _, state := range allModuleStates {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()

			a, _, image := replayLogFollower(t, 0)
			messageEnd := writeSessionMessageFrame(t, image, 0, 9)
			a.BindReplayLog(image)
			a.setModuleState(state, "test")

			got := a.ReplayLogPoll(messageEnd, nowNs)
			if state == ModuleStateActive || state == ModuleStateSuspended {
				require.Equal(t, 1, got)
				require.Equal(t, messageEnd, a.LogAdapter().Position())
				require.Equal(t, messageEnd, a.CommitPosition())
				return
			}
			require.Equal(t, 0, got)
			require.Equal(t, int64(0), a.LogAdapter().Position())
			require.Equal(t, int64(0), a.CommitPosition())
			require.Zero(t, a.logPollNowNs)
		})
	}
}

func TestReplayLogPoll(t *testing.T) {
	t.Parallel()

	const nowNs int64 = 555
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "nil adapter",
			run: func(t *testing.T) {
				a := newFollowerLogPollAgent(0, nil)
				require.Nil(t, a.LogAdapter())
				a.BindReplayLog(nil)
				require.Equal(t, 0, a.ReplayLogPoll(4096, nowNs))
				require.Equal(t, int64(0), a.CommitPosition())
				a.UnbindReplayLog(nil)
			},
		},
		{
			name: "no image",
			run: func(t *testing.T) {
				a, _, _ := replayLogFollower(t, 0)
				require.NotNil(t, a.LogAdapter())
				require.Equal(t, 0, a.ReplayLogPoll(4096, nowNs))
				require.Equal(t, int64(0), a.CommitPosition())
				require.Equal(t, int64(0), a.logPollNowNs)
			},
		},
		{
			name: "bound lowered by a pending wait",
			run: func(t *testing.T) {
				a, q, image := replayLogFollower(t, 1)
				actionEnd, messageEnd := writeSnapshotActionThenMessage(t, image, 0)
				a.BindReplayLog(image)

				// Phase 1: the poll consumes the action and queues its wait.
				require.Equal(t, 1, a.ReplayLogPoll(messageEnd, nowNs))
				require.True(t, a.HasPendingSnapshotAction())
				require.Equal(t, actionEnd, a.LogAdapter().Position())
				require.Equal(t, actionEnd, a.CommitPosition())

				// Phase 2: the wait holds the bound at the action end.
				require.Equal(t, 0, a.ReplayLogPoll(messageEnd, nowNs))
				require.Equal(t, actionEnd, a.LogAdapter().Position())
				require.Equal(t, actionEnd, a.CommitPosition())

				// Phase 3: the pop releases the bound.
				ackAndPopWait(t, a, q, actionEnd, 0)
				require.Equal(t, 1, a.ReplayLogPoll(messageEnd, nowNs))
				require.Equal(t, messageEnd, a.LogAdapter().Position())
				require.Equal(t, messageEnd, a.CommitPosition())
			},
		},
		{
			name: "pending replay JoinLog holds the bound at its start",
			run: func(t *testing.T) {
				a, _, image := replayLogFollower(t, 1)
				messageEnd := writeSessionMessageFrame(t, image, 0, 9)
				a.BindReplayLog(image)
				a.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
				a.PublishReplayJoinLog(sampleReplayLogJoin(0, messageEnd))
				require.True(t, a.HasPendingJoinLog())

				require.Equal(t, 0, a.ReplayLogPoll(messageEnd, nowNs))
				require.Equal(t, int64(0), a.LogAdapter().Position())
				require.Equal(t, int64(0), a.CommitPosition())
			},
		},
		{
			name: "commit raised to the adapter position",
			run: func(t *testing.T) {
				a, _, image := replayLogFollower(t, 0)
				firstEnd := writeSessionMessageFrame(t, image, 0, 9)
				secondEnd := writeSessionMessageFrame(t, image, int(firstEnd), 9)
				a.BindReplayLog(image)

				// The stop is the second frame start. Only the first frame
				// starts before it.
				require.Equal(t, 1, a.ReplayLogPoll(firstEnd, nowNs))
				require.Equal(t, firstEnd, a.LogAdapter().Position())
				require.Equal(t, firstEnd, a.CommitPosition())

				require.Equal(t, 1, a.ReplayLogPoll(secondEnd, nowNs))
				require.Equal(t, secondEnd, a.CommitPosition())
			},
		},
		{
			name: "logPollNowNs set for session learn",
			run: func(t *testing.T) {
				a, _, image := replayLogFollower(t, 0)
				fields := sampleLearnOpenFields(12)
				payload := encodeLogAdapterSessionOpenPayload(t, fields)
				span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
				writeLogAdapterUnfragmentedFrame(t, image, 0, payload)
				a.BindReplayLog(image)

				require.Equal(t, 1, a.ReplayLogPoll(span, nowNs))
				requireLearnedOpen(t, a, fields, nowNs, span)
				require.Equal(t, span, a.CommitPosition())
			},
		},
		{
			name: "UnbindReplayLog clears only its own image",
			run: func(t *testing.T) {
				a, _, image := replayLogFollower(t, 0)
				messageEnd := writeSessionMessageFrame(t, image, 0, 9)
				a.BindReplayLog(image)
				require.Equal(t, 1, a.ReplayLogPoll(messageEnd, nowNs))

				// Phase 1: another image does not clear the binding.
				other := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
				a.UnbindReplayLog(other)
				require.Same(t, image, a.LogAdapter().Image())

				// Phase 2: the bound image clears. The position stays.
				a.UnbindReplayLog(image)
				require.Nil(t, a.LogAdapter().Image())
				require.Equal(t, messageEnd, a.LogAdapter().Position())
				require.Equal(t, 0, a.ReplayLogPoll(messageEnd+4096, nowNs))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestJoinLogAsLeader_unbindsLogImage(t *testing.T) {
	t.Parallel()

	const leadershipTermID int64 = 6
	adapter, image := boundLogAdapter(t)
	messageEnd := writeSessionMessageFrame(t, image, 0, 9)
	a := newFollowerLogPollAgent(messageEnd, adapter)
	a.notifiedCommitPosition = messageEnd
	a.followerConsensusWork(0)
	require.Equal(t, messageEnd, adapter.Position())

	require.NoError(t, a.JoinLogAsLeader(leadershipTermID, messageEnd, 0, false))
	require.Nil(t, adapter.Image())
	require.False(t, image.IsClosed())
	require.Equal(t, messageEnd, adapter.Position())
	require.False(t, a.isFollowerLogImageClosed())
}

// copyLogFrames writes each frame of pub below end into image at the same
// term offset, so each frame keeps its log position.
func copyLogFrames(t *testing.T, pub *aeron.ExclusivePublication, image *aeron.Image, end int64) {
	t.Helper()
	for offset := 0; int64(offset) < end; {
		payload, next := termWireAt(t, pub, offset)
		writeLogAdapterUnfragmentedFrame(t, image, offset, payload)
		offset = next
	}
}

// replayLogFrom binds image at from, replays it to stop, then unbinds it.
func replayLogFrom(
	t fragmentRequireT,
	a *Agent,
	image *aeron.Image,
	from, stop, nowNs int64,
) {
	t.Helper()
	aeron.ImageSetSubscriberPositionForHooks(image, from)
	a.BindReplayLog(image)
	for a.LogAdapter().Position() < stop {
		require.Positive(t, a.ReplayLogPoll(stop, nowNs))
	}
	a.UnbindReplayLog(image)
	require.Equal(t, stop, a.CommitPosition())
}

// countLogEntries counts the TimerEvents for correlationID and the
// SessionCloseEvents for sessionID in pub that end above from and below end.
func countLogEntries(
	t *testing.T,
	pub *aeron.ExclusivePublication,
	from, end, correlationID, sessionID int64,
) (timerEvents, sessionCloses int) {
	t.Helper()
	for _, frame := range scanLogEntryFrames(t, pub, end) {
		if frame.endPosition <= from {
			continue
		}
		switch frame.templateID {
		case clusterwire.TimerEventSbeTemplateID:
			if frame.id == correlationID {
				timerEvents++
			}
		case clusterwire.SessionCloseEventSbeTemplateID:
			if frame.id == sessionID {
				sessionCloses++
			}
		}
	}
	return timerEvents, sessionCloses
}

func TestUncommittedEntryRollback_replay(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 3
		correlationID    int64 = 21
		closedID         int64 = 1
		clearedID        int64 = 2
		nowNs            int64 = 0
	)

	// formerLeader opens session 1 and commits the open. Above commit it
	// fires a timer, closes session 1 and opens session 2. Then it prepares
	// at commit. Returns the commit and the publication end.
	formerLeader := func(t *testing.T) (*Agent, *aeron.ExclusivePublication, int64, int64) {
		t.Helper()
		a, q, pub := leaderWithTimerIngress(t, AgentConfig{
			LeadershipTermID: leadershipTermID,
			ThisMemberID:     1,
			Commit:           new(atomic.Int64),
			LogAdapter:       NewLogAdapter(0),
		})
		wireImmediateEgress(t, a)
		clientIngress := attachQueueClientIngress(a)

		a.acceptSessionConnect(validConnectFields(1), nowNs)
		require.Equal(t, 2, a.sessionWork(nowNs))
		commitPosition := pub.Position()
		require.Equal(t, 1, a.UpdateLeaderPosition(nowNs, commitPosition))

		requireScheduled(t, a, q, correlationID, nowNs, 1)
		require.Equal(t, 1, a.timerWork(nowNs))
		clientIngress.Enqueue(encodeSessionCloseRequestWire(t, leadershipTermID, closedID))
		require.Equal(t, 1, a.pollClientIngress(nowNs))
		a.acceptSessionConnect(validConnectFields(2), nowNs)
		require.Equal(t, 2, a.sessionWork(nowNs))
		end := pub.Position()

		_, prepared, err := a.PrepareForNewLeadership(commitPosition, nowNs)
		require.NoError(t, err)
		require.True(t, prepared)
		require.Equal(t, commitPosition, a.CommitPosition())
		requireAgentTimers(t, a, map[int64]int64{correlationID: 0}, nil)
		requireSessionIDs(t, a, closedID)
		require.Equal(t, 1, a.ClosingSessionCount())
		return a, pub, commitPosition, end
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "re-elected and its own frames survive",
			run: func(t *testing.T) {
				a, pub, commitPosition, end := formerLeader(t)
				image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
				copyLogFrames(t, pub, image, end)

				replayLogFrom(t, a, image, commitPosition, end, nowNs)
				requireAgentTimers(t, a, nil, nil)
				require.Equal(t, 0, a.ClosingSessionCount())
				requireSessionIDs(t, a, clearedID)
				cleared, ok := a.sessions.sessionByID(clearedID)
				require.True(t, ok)
				require.Equal(t, sessionOpen, cleared.state)
			},
		},
		{
			name: "re-elected after its frames were truncated",
			run: func(t *testing.T) {
				a, pub, commitPosition, end := formerLeader(t)
				image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
				copyLogFrames(t, pub, image, commitPosition)

				replayLogFrom(t, a, image, commitPosition, commitPosition, nowNs)
				requireAgentTimers(t, a, map[int64]int64{correlationID: 0}, nil)
				requireSessionIDs(t, a, closedID)
				require.Equal(t, 1, a.ClosingSessionCount())

				require.NoError(t, a.JoinLogAsLeader(leadershipTermID+1, commitPosition, 0, false))
				a.ElectionComplete(nowNs)
				for tick := int64(1); tick <= timerWheelTicksPerWheel; tick++ {
					tickNs := nowNs + tick*timerWheelTickResolutionNs
					a.timerWork(tickNs)
					a.sessionWork(tickNs)
				}
				requireAgentTimers(t, a, nil, nil)
				require.Equal(t, 0, a.ClosingSessionCount())
				requireSessionIDs(t, a)

				timerEvents, sessionCloses := countLogEntries(
					t, pub, end, pub.Position(), correlationID, closedID,
				)
				require.Equal(t, 1, timerEvents)
				require.Equal(t, 1, sessionCloses)
			},
		},
		{
			name: "follower reads the new leader frames",
			run: func(t *testing.T) {
				a, _, commitPosition, _ := formerLeader(t)
				image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
				offset := int(commitPosition)
				timerEvent := encodeLogAdapterTimerEventPayload(t, codecs.TimerEventFields{
					LeadershipTermID: leadershipTermID + 1,
					CorrelationID:    correlationID,
				})
				writeLogAdapterUnfragmentedFrame(t, image, offset, timerEvent)
				offset += logbuffer.AlignFragmentChunkLength(len(timerEvent))
				sessionClose := encodeLogAdapterSessionClosePayload(t, codecs.SessionCloseEventFields{
					LeadershipTermID: leadershipTermID + 1,
					ClusterSessionID: closedID,
					CloseReason:      clusterwire.CloseReason_TIMEOUT,
				})
				writeLogAdapterUnfragmentedFrame(t, image, offset, sessionClose)
				offset += logbuffer.AlignFragmentChunkLength(len(sessionClose))

				replayLogFrom(t, a, image, commitPosition, int64(offset), nowNs)
				requireAgentTimers(t, a, nil, nil)
				require.Equal(t, 0, a.ClosingSessionCount())
				requireSessionIDs(t, a)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func newLeadershipTermReplay(
	tb testing.TB,
	adapter *LogAdapter,
	image *aeron.Image,
	payload []byte,
) *Agent {
	tb.Helper()
	agent := NewAgent(AgentConfig{Commit: new(atomic.Int64), LogAdapter: adapter})
	writeLogAdapterUnfragmentedFrame(tb, image, 0, payload)
	return agent
}

func requireAdapterCommit(tb testing.TB, agent *Agent, position int64) {
	tb.Helper()
	require.Equal(tb, position, agent.logAdapter.Position())
	require.Equal(tb, position, agent.CommitPosition())
}

func TestAgent_newLeadershipTermEventReachesElection(t *testing.T) {
	fields := sampleLogAdapterNLTFields()
	payload := encodeLogAdapterNLTPayload(t, fields)
	eventEnd := int64(logbuffer.AlignFragmentChunkLength(len(payload)))

	t.Run("runner_set", func(t *testing.T) {
		adapter, image := boundLogAdapter(t)
		agent := newLeadershipTermReplay(t, adapter, image, payload)
		runner := &recordingRunner{}
		agent.SetElection(runner)

		require.Positive(t, agent.ReplayLogPoll(eventEnd+32, 1))
		requireAdapterCommit(t, agent, eventEnd)
		require.Equal(t, []codecs.NewLeadershipTermEventFields{fields}, runner.termEvents)
	})

	t.Run("runner_nil", func(t *testing.T) {
		adapter, image := boundLogAdapter(t)
		agent := newLeadershipTermReplay(t, adapter, image, payload)

		require.NotPanics(t, func() {
			require.Positive(t, agent.ReplayLogPoll(eventEnd+32, 1))
		})
		requireAdapterCommit(t, agent, eventEnd)
	})

	t.Run("runner_error", func(t *testing.T) {
		adapter, image, handler := boundLogAdapterWithHandler(t)
		agent := newLeadershipTermReplay(t, adapter, image, payload)
		runner := &recordingRunner{termErr: errors.New("append refused")}
		agent.SetElection(runner)

		messageEnd := writeSessionMessageFrame(t, image, int(eventEnd), 9)

		agent.ReplayLogPoll(messageEnd+32, 1)
		requireAdapterCommit(t, agent, 0)
		require.Equal(t, []codecs.NewLeadershipTermEventFields{fields}, runner.termEvents)
		requireHandlerIdle(t, handler)

		agent.ReplayLogPoll(messageEnd+32, 1)
		requireAdapterCommit(t, agent, 0)
		require.Equal(t, []codecs.NewLeadershipTermEventFields{fields, fields}, runner.termEvents)
		require.Equal(t, 0, handler.sessionMessages)
	})
}
