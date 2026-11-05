//go:build testhooks

package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func TestAgent_Start_once(t *testing.T) {
	svc := &recordingService{}
	agent := NewAgent(AgentConfig{Service: svc})

	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 1, svc.startCount)
	require.NoError(t, agent.ServiceFailure())

	// Later ticks do not re-call Start.
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, 1, svc.startCount)
}

func TestAgent_Start_errorMarksSticky(t *testing.T) {
	startErr := errors.New("start boom")
	svc := &recordingService{startErr: startErr}
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	agent := NewAgent(AgentConfig{
		Service:       svc,
		Subscriptions: &stubAdder{sub: sub},
	})
	// Join is ready to bind. Start failure must return before joinWork.
	agent.handleJoinLog(defaultJoinEvent(0))

	require.Equal(t, 1, agent.DoWork(0))
	require.ErrorIs(t, agent.ServiceFailure(), startErr)
	require.Equal(t, 1, svc.startCount)
	require.True(t, agent.HasPendingJoin())
	require.False(t, agent.IsLogBound())

	// serviceFailure early return. Without that check, joinWork would bind.
	require.Equal(t, 0, agent.DoWork(0))
	require.False(t, agent.IsLogBound())
	require.True(t, agent.HasPendingJoin())
	require.Equal(t, 0, agent.ReadyCoordinationAcks())
	require.Equal(t, 1, svc.startCount)
}

func TestAgent_CallbackOrderGolden(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	frames := [][]byte{
		encodeSessionOpen(t, codecs.SessionOpenEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        1,
			ResponseChannel:  []byte("c"),
		}),
		encodeSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: 1,
				Timestamp:        2,
			},
			[]byte("m1"),
		),
		encodeSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: 1,
				Timestamp:        3,
			},
			[]byte("m2"),
		),
		encodeTimerEvent(t, codecs.TimerEventFields{
			LeadershipTermID: 1,
			CorrelationID:    99,
			Timestamp:        4,
		}),
		encodeSessionClose(t, codecs.SessionCloseEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        5,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		}),
	}

	offset := 0
	for _, payload := range frames {
		offset += writeAgentFrame(t, image, offset, payload)
	}
	end := int64(offset)
	*committed = end

	// Drain until all frames delivered (fragment limit may chunk).
	for agent.LogPosition() < end && agent.ServiceFailure() == nil {
		agent.DoWork(0)
	}
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, []string{"open", "message", "message", "timer", "close"}, svc.order)
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, int64(5), agent.ClusterTime())
	require.Len(t, svc.messages, 2)
}

func TestAgent_PositionAndTime_boundLimited(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	p1 := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        100,
	})
	p2 := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		Timestamp:        200,
	})
	span1 := writeAgentFrame(t, image, 0, p1)
	span2 := writeAgentFrame(t, image, span1, p2)

	// Bound between frames.
	*committed = int64(span1)
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, int64(span1), agent.LogPosition())
	require.Equal(t, int64(100), agent.ClusterTime())
	require.Len(t, svc.timers, 1)

	*committed = int64(span1 + span2)
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, int64(span1+span2), agent.LogPosition())
	require.Equal(t, int64(200), agent.ClusterTime())
	require.Len(t, svc.timers, 2)
}

func TestAgent_NLT_optionalHandler(t *testing.T) {
	t.Run("implements", func(t *testing.T) {
		svc := &nltRecordingService{}
		agent, image, committed := bindAgent(t, svc, 0)
		fields := codecs.NewLeadershipTermEventFields{
			LeadershipTermID:    7,
			LogPosition:         128,
			Timestamp:           999,
			TermBaseLogPosition: 64,
			LeaderMemberID:      1,
			LogSessionID:        3,
			TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:          42,
		}
		span := writeAgentFrame(t, image, 0, encodeNLT(t, fields))
		*committed = int64(span)
		require.Equal(t, 1, agent.DoWork(0))
		require.Len(t, svc.nlt, 1)
		require.Equal(
			t,
			nltRec{
				leadershipTermID:    fields.LeadershipTermID,
				logPosition:         fields.LogPosition,
				timestamp:           fields.Timestamp,
				termBaseLogPosition: fields.TermBaseLogPosition,
				leaderMemberID:      fields.LeaderMemberID,
				logSessionID:        fields.LogSessionID,
				timeUnit:            fields.TimeUnit,
				appVersion:          fields.AppVersion,
			},
			svc.nlt[0],
		)
		require.Equal(t, int64(span), agent.LogPosition())
		require.Equal(t, int64(999), agent.ClusterTime())
		require.Equal(t, fields.LeadershipTermID, agent.egressLeadershipTermID)
	})

	t.Run("doesNotImplement", func(t *testing.T) {
		svc := &recordingService{}
		agent, image, committed := bindAgent(t, svc, 0)
		fields := codecs.NewLeadershipTermEventFields{
			LeadershipTermID: 1,
			LogPosition:      1,
			Timestamp:        50,
		}
		span := writeAgentFrame(t, image, 0, encodeNLT(t, fields))
		*committed = int64(span)
		require.Equal(t, 1, agent.DoWork(0))
		require.Equal(t, int64(span), agent.LogPosition())
		require.Equal(t, int64(50), agent.ClusterTime())
		require.Empty(t, svc.order)
		require.Equal(t, fields.LeadershipTermID, agent.egressLeadershipTermID)
	})
}

func TestAgent_NilService(t *testing.T) {
	agent, image, committed := bindAgent(t, nil, 0)

	openPayload := encodeSessionOpen(t, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 3,
		Timestamp:        1,
		ResponseChannel:  []byte("c"),
	})
	span := writeAgentFrame(t, image, 0, openPayload)
	*committed = int64(span)

	require.NotPanics(t, func() {
		require.Equal(t, 1, agent.DoWork(0))
	})
	require.True(t, agent.HasSession(3))
	require.Equal(t, int64(span), agent.LogPosition())
}

func TestAgent_ScheduleTimer(t *testing.T) {
	fields := sampleScheduleTimerFields()
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{ModuleProxy: proxy})
	require.True(t, agent.ScheduleTimer(fields.CorrelationID, fields.Deadline))
	require.NoError(t, agent.ServiceFailure())
	requireDecodedFrame(
		t,
		pub,
		codecs.ScheduleTimerEncodedLen(),
		codecs.TryDecodeScheduleTimerAt,
		fields,
	)
}

func TestAgent_CancelTimer(t *testing.T) {
	fields := sampleCancelTimerFields()
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{ModuleProxy: proxy})
	require.True(t, agent.CancelTimer(fields.CorrelationID))
	require.NoError(t, agent.ServiceFailure())
	requireDecodedFrame(t, pub, codecs.CancelTimerEncodedLen(), codecs.TryDecodeCancelTimerAt, fields)
}

func TestAgent_TimerPublish_failurePaths(t *testing.T) {
	schedule := sampleScheduleTimerFields()
	cancel := sampleCancelTimerFields()
	ops := []struct {
		name    string
		publish func(*Agent) bool
	}{
		{
			name: "schedule",
			publish: func(a *Agent) bool {
				return a.ScheduleTimer(schedule.CorrelationID, schedule.Deadline)
			},
		},
		{
			name: "cancel",
			publish: func(a *Agent) bool {
				return a.CancelTimer(cancel.CorrelationID)
			},
		},
	}
	for _, tc := range moduleProxyFailureCases(ErrTimerPublish) {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range ops {
				t.Run(op.name, func(t *testing.T) {
					agent := NewAgent(AgentConfig{ModuleProxy: tc.setup(t)})
					require.False(t, op.publish(agent))
					if tc.wantFailure != nil {
						require.ErrorIs(t, agent.ServiceFailure(), tc.wantFailure)
						require.False(t, op.publish(agent))
						require.ErrorIs(t, agent.ServiceFailure(), tc.wantFailure)
					} else {
						require.NoError(t, agent.ServiceFailure())
					}
				})
			}
		})
	}
}

// Asserts a closed Agent sends no module request. Each request returns
// false, the session stays open with IsClosing false, and no failure marks.
func TestAgent_Close_rejectsModuleRequests(t *testing.T) {
	const openID int64 = 7
	log, _, pub := bindCloseAgent(t, nil, codecs.RoleLeader)
	agent := log.agent
	log.open(openID, 1)
	agent.Close()
	sent := pub.Position()

	require.False(t, agent.ScheduleTimer(1, 1))
	require.False(t, agent.CancelTimer(1))
	require.False(t, agent.CloseClientSession(openID))
	session, ok := agent.ClientSession(openID)
	require.True(t, ok)
	require.False(t, session.Close())
	require.False(t, session.IsClosing())
	require.Equal(t, sent, pub.Position())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_NilCommittedPosition_skipsPoll(t *testing.T) {
	svc := &recordingService{}
	sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)

	agent := NewAgent(AgentConfig{
		Service:       svc,
		Subscriptions: &stubAdder{sub: sub},
		// CommittedPosition nil
	})
	agent.handleJoinLog(defaultJoinEvent(0))
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())

	payload := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        1,
	})
	writeAgentFrame(t, image, 0, payload)

	require.Equal(t, 0, agent.DoWork(0))
	require.Empty(t, svc.timers)
	require.Equal(t, int64(0), agent.LogPosition())
}

func TestAgent_Close_idempotent(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent

	// Session 1 holds a publication. Session 2 has an add in flight.
	log.open(1, 1)
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	log.open(2, 1)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())

	payload := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        1,
	})
	log.write(t, payload)

	agent.Close()
	require.False(t, agent.IsLogBound())
	require.True(t, agent.logSubscription == nil || agent.logSubscription.IsClosed())
	require.True(t, pub.IsClosed())
	require.Equal(t, []int64{2}, adder.cancels)
	require.Empty(t, adder.inFlight)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	require.Equal(t, 2, agent.SessionCount())
	for session := range agent.ClientSessions() {
		require.Equal(t, aeron.NotConnected, session.Offer([]byte("late")))
	}

	// Idempotent.
	agent.Close()
	require.Equal(t, []int64{2}, adder.cancels)

	// Inert after close.
	require.Equal(t, 0, agent.DoWork(0))
	require.Empty(t, svc.timers)
}

func TestAgent_NewAgent_sentinels(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	require.Equal(t, int32(-1), agent.MemberID())
	require.Equal(t, int64(0), agent.LogPosition())
	require.Equal(t, codecs.RoleFollower, agent.Role())
	require.False(t, agent.IsLogBound())
	require.False(t, agent.HasPendingJoin())
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, nullValue, agent.egressLeadershipTermID)
	require.Equal(t, 0, agent.ResponsePublicationCount())
}

func TestAgent_DoWork_pollsArchive(t *testing.T) {
	svc := &recordingService{}
	agent := NewAgent(AgentConfig{Service: svc})
	archiveControl := &countingArchiveControl{pollWork: 2}
	agent.archive = archiveControl

	// Tick 1: Start plus the archive Poll count.
	require.Equal(t, 3, agent.DoWork(0))
	require.Equal(t, 1, archiveControl.polls)

	// Tick 2: the archive Poll count alone.
	require.Equal(t, 2, agent.DoWork(0))
	require.Equal(t, 2, archiveControl.polls)

	// After Close: no Poll.
	agent.Close()
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, 2, archiveControl.polls)
}

func TestAgent_NewAgent_noArchive(t *testing.T) {
	agent := NewAgent(AgentConfig{})

	// A typed-nil client in the interface field would panic in Poll.
	require.Equal(t, snapshotArchiveControl(nil), agent.archive)
	require.Equal(t, 1, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_DispatchFailure_sticky(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	// Wrong schema aborts and marks failure.
	payload := encodeMessageHeader(t, 0, 1, 112, 0)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)

	require.Equal(t, 0, agent.DoWork(0))
	require.ErrorIs(t, agent.ServiceFailure(), ErrLogDispatchSchema)
	require.Equal(t, 0, agent.DoWork(0))
}
