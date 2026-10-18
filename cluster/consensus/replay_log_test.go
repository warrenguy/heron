//go:build testhooks

package consensus

import (
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
// the next serviceControlWork call pops a wait.
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
}

func TestPublishReplayJoinLog(t *testing.T) {
	t.Parallel()

	const (
		startPosition int64 = 4096
		stopPosition  int64 = 8192
	)
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
