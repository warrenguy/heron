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

type errorSiteRun struct {
	agent *Agent
	check func(t *testing.T)
}

func bindHandledLeader(
	t *testing.T,
	adder ResponsePublicationAdder,
	handler func(error),
) *sessionLog {
	t.Helper()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	log.agent.errorHandler = handler
	return log
}

func deliverControlJoin(t *testing.T, handler func(error)) *controlAgentHarness {
	t.Helper()
	h := newControlAgentHarness(t, 0)
	h.agent.errorHandler = handler
	h.control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))
	h.agent.DoWork(0)
	return h
}

func driveControlReject(t *testing.T, handler func(error), payload []byte) errorSiteRun {
	t.Helper()
	control := &QueueControlIngress{}
	agent := NewAgent(AgentConfig{
		Service:      &recordingService{},
		Control:      control,
		ErrorHandler: handler,
	})
	control.Enqueue(payload)
	agent.DoWork(0)
	return errorSiteRun{agent: agent, check: func(t *testing.T) {
		t.Helper()
		require.False(t, agent.HasPendingJoin())
		require.Equal(t, 0, agent.PendingCoordinationAcks())
	}}
}

func queuePollError(adder *fakeResponseAdder, err error) {
	adder.next(1, responsePoll{})
	adder.next(1, responsePoll{err: err})
}

func requireSessionWithoutPublication(t *testing.T, agent *Agent, sessionID int64) {
	t.Helper()
	require.True(t, agent.HasSession(sessionID))
	require.Equal(t, 0, agent.ResponsePublicationCount())
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
}

func requireAddCancelled(t *testing.T, agent *Agent, adder *fakeResponseAdder) {
	t.Helper()
	require.Equal(t, []int64{1}, adder.cancels)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
}

func requireSnapshotActionAbort(
	t *testing.T,
	agent *Agent,
	svc *snapshotRecordingService,
	span int,
) {
	t.Helper()
	require.Equal(t, int64(span), agent.LogPosition())
	require.Empty(t, svc.pubs)
	require.Equal(t, 0, agent.PendingCoordinationAcks())
}

func TestAgent_ErrorHandler_everySite(t *testing.T) {
	startErr := errors.New("start boom")
	addSubErr := errors.New("add sub failed")
	offerErr := errors.New("offer add boom")
	pollErr := errors.New("poll add boom")

	tests := []struct {
		name     string
		fatal    bool
		sentinel error
		wrapped  error
		drive    func(t *testing.T, handler func(error)) errorSiteRun
	}{
		{
			name:     "Start",
			fatal:    true,
			sentinel: ErrStart,
			wrapped:  startErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				agent := NewAgent(AgentConfig{
					Service:      &recordingService{startErr: startErr},
					ErrorHandler: handler,
				})
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, 0, agent.PendingCoordinationAcks())
					require.ErrorIs(t, agent.ServiceFailure(), startErr)
				}}
			},
		},
		{
			name:     "LogSubscriptionAdd",
			fatal:    true,
			sentinel: ErrLogSubscriptionAdd,
			wrapped:  addSubErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				agent := NewAgent(AgentConfig{
					Service:       &recordingService{},
					Subscriptions: &stubAdder{err: addSubErr},
					ErrorHandler:  handler,
				})
				agent.handleJoinLog(defaultJoinEvent(0))
				require.Equal(t, 1, agent.DoWork(0))
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, agent.IsLogBound())
					require.True(t, agent.HasPendingJoin())
				}}
			},
		},
		{
			name:     "LogNotContiguous",
			fatal:    true,
			sentinel: ErrLogNotContiguous,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
				agent := NewAgent(AgentConfig{
					Service:       &recordingService{},
					Subscriptions: &stubAdder{sub: sub},
					ErrorHandler:  handler,
				})
				agent.handleJoinLog(defaultJoinEvent(128))
				require.Equal(t, 1, agent.DoWork(0))
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, agent.IsLogBound())
					require.Equal(t, int64(0), agent.LogPosition())
				}}
			},
		},
		{
			name:     "SessionClash",
			fatal:    true,
			sentinel: ErrSessionClash,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				log := bindHandledLeader(t, nil, handler)
				log.open(7, 50)
				frozen := log.agent.LogPosition()
				log.deliver(encodeSessionOpen(t, codecs.SessionOpenEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: 7,
					Timestamp:        80,
					ResponseStreamID: testResponseStreamID,
					ResponseChannel:  []byte(testResponseChannel),
				}))
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, frozen, log.agent.LogPosition())
					require.Equal(t, 1, log.agent.SessionCount())
				}}
			},
		},
		{
			name:     "TimerPublish",
			fatal:    true,
			sentinel: ErrTimerPublish,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				proxy, pub := boundModuleProxy(t)
				require.NoError(t, pub.Close())
				agent := NewAgent(AgentConfig{
					ModuleProxy:  proxy,
					ErrorHandler: handler,
				})
				sent := agent.ScheduleTimer(1, 2)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, sent)
				}}
			},
		},
		{
			name:     "CloseSessionPublish",
			fatal:    true,
			sentinel: ErrCloseSessionPublish,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				proxy, pub := boundModuleProxy(t)
				agent := NewAgent(AgentConfig{
					ModuleProxy:  proxy,
					ErrorHandler: handler,
				})
				agent.handleSessionOpen(64, codecs.SessionOpenEventFields{
					ClusterSessionID: 7,
					Timestamp:        64,
					ResponseStreamID: testResponseStreamID,
					ResponseChannel:  []byte(testResponseChannel),
				})
				require.NoError(t, pub.Close())
				closed := agent.CloseClientSession(7)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, closed)
					require.True(t, agent.HasSession(7))
					session, ok := agent.ClientSession(7)
					require.True(t, ok)
					require.False(t, session.IsClosing())
				}}
			},
		},
		{
			name:     "AckPublish",
			fatal:    true,
			sentinel: ErrAckPublish,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				h := deliverControlJoin(t, handler)
				require.True(t, h.agent.IsLogBound())
				require.NoError(t, h.pub.Close())
				h.agent.DoWork(0)
				return errorSiteRun{agent: h.agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, 1, h.agent.PendingCoordinationAcks())
					require.Equal(t, int64(0), h.agent.NextAckID())
				}}
			},
		},
		{
			name:     "AckRequestPassed",
			fatal:    true,
			sentinel: ErrAckRequestPassed,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				h := deliverControlJoin(t, handler)
				h.agent.DoWork(0)
				span := h.writeTimer(t, 0, 1, 1)
				*h.committed = int64(span)
				h.agent.DoWork(0)
				require.Equal(t, int64(span), h.agent.LogPosition())
				h.control.Enqueue(encodeRequestServiceAckControl(t, 0))
				h.agent.DoWork(0)
				return errorSiteRun{agent: h.agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, 1, h.agent.PendingCoordinationAcks())
					require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)
				}}
			},
		},
		{
			name:     "LogDispatchHeader",
			fatal:    true,
			sentinel: ErrLogDispatchHeader,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				return driveLogAbort(t, handler, []byte{0x01, 0x02, 0x03, 0x04})
			},
		},
		{
			name:     "LogDispatchSchema",
			fatal:    true,
			sentinel: ErrLogDispatchSchema,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				payload := encodeMessageHeader(t, 0, 1, 112, 0)
				return driveLogAbort(t, handler, payload)
			},
		},
		{
			name:     "LogDispatchDecode",
			fatal:    true,
			sentinel: ErrLogDispatchDecode,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				payload := encodeMessageHeader(
					t,
					clusterwire.SessionMessageHeaderSbeBlockLength,
					clusterwire.SessionMessageHeaderSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.SessionMessageHeaderSbeSchemaVersion,
				)
				return driveLogAbort(t, handler, payload)
			},
		},
		{
			name:     "ActionPositionMismatch",
			fatal:    true,
			sentinel: ErrActionPositionMismatch,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				svc := &snapshotRecordingService{}
				agent, image, committed := bindImage(t, AgentConfig{
					Service:      svc,
					ErrorHandler: handler,
				}, 0)
				span := writeClusterAction(t, image, committed, snapshotActionFields(1, 12))
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					requireSnapshotActionAbort(t, agent, svc, span)
				}}
			},
		},
		{
			name:     "UnknownClusterAction",
			fatal:    true,
			sentinel: ErrUnknownClusterAction,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				svc := &snapshotRecordingService{}
				agent, image, committed := bindImage(t, AgentConfig{
					Service:      svc,
					ErrorHandler: handler,
				}, 0)
				span := writeAlignedAction(
					t,
					image,
					committed,
					9,
					clusterwire.ClusterAction_NULL_VALUE,
					0,
				)
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					requireSnapshotActionAbort(t, agent, svc, span)
				}}
			},
		},
		{
			name:     "ResponseAddOffer",
			sentinel: ErrResponsePublicationAdd,
			wrapped:  offerErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				adder := newFakeResponseAdder()
				adder.offerErrs = map[int32]error{testResponseStreamID: offerErr}
				log := bindHandledLeader(t, adder, handler)
				log.open(7, 50)
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					requireSessionWithoutPublication(t, log.agent, 7)
				}}
			},
		},
		{
			name:     "ResponseAddPoll",
			sentinel: ErrResponsePublicationAdd,
			wrapped:  pollErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				adder := newFakeResponseAdder()
				log := bindHandledLeader(t, adder, handler)
				log.open(7, 50)
				adder.next(1, responsePoll{err: pollErr})
				log.agent.DoWork(0)
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					requireSessionWithoutPublication(t, log.agent, 7)
				}}
			},
		},
		{
			name:     "UnknownSessionClose",
			sentinel: ErrUnknownSessionClose,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				svc := &recordingService{}
				agent, image, committed := bindImage(t, AgentConfig{
					Service:      svc,
					ErrorHandler: handler,
				}, 0)
				closeSpan := writeAgentFrame(t, image, 0, encodeSessionClose(t, codecs.SessionCloseEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: 999,
					Timestamp:        3,
					CloseReason:      clusterwire.CloseReason_TIMEOUT,
				}))
				timerSpan := writeAgentFrame(t, image, closeSpan, encodeTimerEvent(t, codecs.TimerEventFields{
					LeadershipTermID: 1,
					CorrelationID:    1,
					Timestamp:        4,
				}))
				*committed = int64(closeSpan + timerSpan)
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, 0, agent.SessionCount())
					require.Len(t, svc.timers, 1)
					require.Equal(t, int64(4), agent.ClusterTime())
				}}
			},
		},
		{
			name:     "ControlSchema",
			sentinel: ErrControlMessageSchema,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				return driveControlReject(t, handler, encodeMessageHeader(
					t,
					8,
					clusterwire.JoinLogSbeTemplateID,
					999,
					16,
				))
			},
		},
		{
			name:     "CloseUnknownSession",
			sentinel: ErrCloseUnknownSession,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				agent, _, _ := bindImage(t, AgentConfig{ErrorHandler: handler}, 0)
				agent.DoWork(0)
				require.True(t, agent.IsLogBound())
				closed := agent.CloseClientSession(99)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, closed)
					require.Equal(t, 0, agent.SessionCount())
				}}
			},
		},
		{
			name:     "SessionCloseEndsAdd",
			sentinel: ErrResponsePublicationAdd,
			wrapped:  pollErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				adder := newFakeResponseAdder()
				log := bindHandledLeader(t, adder, handler)
				log.open(7, 1)
				queuePollError(adder, pollErr)
				log.close(7, 2)
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, log.agent.HasSession(7))
					requireAddCancelled(t, log.agent, adder)
				}}
			},
		},
		{
			name:     "ImageCloseEndsAdd",
			sentinel: ErrResponsePublicationAdd,
			wrapped:  pollErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				adder := newFakeResponseAdder()
				log := bindHandledLeader(t, adder, handler)
				log.open(7, 1)
				queuePollError(adder, pollErr)
				aeron.ImageInternalCloseForHooks(log.image)
				log.agent.DoWork(0)
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					require.False(t, log.agent.IsLogBound())
					require.Equal(t, codecs.RoleFollower, log.agent.Role())
					require.True(t, log.agent.HasSession(7))
					requireAddCancelled(t, log.agent, adder)
				}}
			},
		},
		{
			name:     "FollowerJoinEndsAdd",
			sentinel: ErrResponsePublicationAdd,
			wrapped:  pollErr,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				adder := newFakeResponseAdder()
				log := bindHandledLeader(t, adder, handler)
				log.open(7, 1)
				log.agent.adapter.SetImage(nil)
				pos := log.agent.LogPosition()
				replaceLogSubscription(t, log.agent, testLogSessionID, testLogStreamID, pos)
				log.agent.logSubscription = nil
				event := defaultJoinEvent(pos)
				event.role = codecs.RoleFollower
				log.agent.handleJoinLog(event)
				queuePollError(adder, pollErr)
				log.agent.DoWork(0)
				return errorSiteRun{agent: log.agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, codecs.RoleFollower, log.agent.Role())
					require.True(t, log.agent.IsLogBound())
					require.True(t, log.agent.HasSession(7))
					requireAddCancelled(t, log.agent, adder)
				}}
			},
		},
		{
			name:     "ControlDecode",
			sentinel: ErrControlMessageDecode,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				return driveControlReject(t, handler, []byte{0x01, 0x02, 0x03, 0x04})
			},
		},
		{
			name:     "UnknownTimeUnit",
			sentinel: ErrUnknownTimeUnit,
			drive: func(t *testing.T, handler func(error)) errorSiteRun {
				t.Helper()
				const unknown = clusterwire.ClusterTimeUnit(99)
				svc := &nltRecordingService{}
				agent, image, committed := bindImage(t, AgentConfig{
					Service:      svc,
					ErrorHandler: handler,
				}, 0)
				span := writeAgentFrame(t, image, 0, encodeNLT(t, codecs.NewLeadershipTermEventFields{
					LeadershipTermID: 7,
					LogPosition:      128,
					Timestamp:        999,
					LeaderMemberID:   1,
					LogSessionID:     3,
					TimeUnit:         unknown,
					AppVersion:       42,
				}))
				*committed = int64(span)
				agent.DoWork(0)
				return errorSiteRun{agent: agent, check: func(t *testing.T) {
					t.Helper()
					require.Equal(t, unknown, agent.timeUnit)
					require.Equal(t, int32(42), agent.appVersion)
					require.Equal(t, int64(span), agent.LogPosition())
					require.Equal(t, int64(999), agent.ClusterTime())
					require.Len(t, svc.nlt, 1)
					require.Equal(t, unknown, svc.nlt[0].timeUnit)
				}}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			finish := func(t *testing.T, run errorSiteRun) {
				t.Helper()
				run.check(t)
				if tc.fatal {
					require.Equal(t, 0, run.agent.DoWork(1))
				}
			}
			t.Run("records", func(t *testing.T) {
				var got []error
				run := tc.drive(t, collectErrors(&got))
				require.Len(t, got, 1)
				require.ErrorIs(t, got[0], tc.sentinel)
				if tc.wrapped != nil {
					require.ErrorIs(t, got[0], tc.wrapped)
				}
				require.Equal(t, tc.fatal, errors.Is(got[0], ErrFailed))
				if tc.fatal {
					require.Same(t, got[0], run.agent.ServiceFailure())
				} else {
					require.NoError(t, run.agent.ServiceFailure())
				}
				finish(t, run)
				if tc.fatal {
					require.False(t, run.agent.CloseClientSession(99))
					run.agent.Close()
					run.agent.Close()
					require.Len(t, got, 1)
					require.Same(t, got[0], run.agent.ServiceFailure())
				}
			})
			t.Run("nilHandler", func(t *testing.T) {
				run := tc.drive(t, nil)
				if tc.fatal {
					require.ErrorIs(t, run.agent.ServiceFailure(), ErrFailed)
					require.ErrorIs(t, run.agent.ServiceFailure(), tc.sentinel)
					if tc.wrapped != nil {
						require.ErrorIs(t, run.agent.ServiceFailure(), tc.wrapped)
					}
				} else {
					require.NoError(t, run.agent.ServiceFailure())
				}
				finish(t, run)
			})
		})
	}
}

func driveLogAbort(t *testing.T, handler func(error), payload []byte) errorSiteRun {
	t.Helper()
	agent, image, committed := bindImage(t, AgentConfig{
		Service:      &recordingService{},
		ErrorHandler: handler,
	}, 0)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)
	agent.DoWork(0)
	return errorSiteRun{agent: agent, check: func(t *testing.T) {
		t.Helper()
		require.Equal(t, int64(0), agent.LogPosition())
	}}
}

func TestAgent_ErrorHandler_closeDropsTeardownErrors(t *testing.T) {
	var got []error
	adder := newFakeResponseAdder()
	log := bindHandledLeader(t, adder, collectErrors(&got))
	log.open(1, 1)
	log.open(2, 2)
	require.Equal(t, 2, log.agent.PendingResponsePublicationAdds())
	require.Empty(t, got)

	adder.next(1, responsePoll{err: errors.New("close poll 1")})
	adder.next(2, responsePoll{err: errors.New("close poll 2")})
	log.agent.Close()

	require.Empty(t, got)
	require.NoError(t, log.agent.ServiceFailure())
	require.Equal(t, []int64{1, 2}, adder.cancels)
	require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
	require.Equal(t, 2, log.agent.SessionCount())

	require.False(t, log.agent.CloseClientSession(99))
	require.Empty(t, got)
	require.NoError(t, log.agent.ServiceFailure())
}
