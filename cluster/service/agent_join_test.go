//go:build testhooks

package service

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs"
)

func TestAgent_JoinLifecycle(t *testing.T) {
	t.Run("cold start", func(t *testing.T) {
		svc := &recordingService{}
		agent, _, _ := bindAgent(t, svc, 0)
		// Bind marks the join ack ready. Bind does not drain the ack.
		require.Equal(t, 1, svc.startCount)
		require.Equal(t, 1, agent.PendingCoordinationAcks())
	})

	t.Run("recovery seed binds at the seeded position", func(t *testing.T) {
		const seeded int64 = 640
		sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, seeded)
		agent := NewAgent(AgentConfig{
			Service:       &recordingService{},
			Subscriptions: &stubAdder{sub: sub},
		})
		agent.seedRecovery(codecs.RecoveryStateKeyFields{
			LeadershipTermID: 5,
			LogPosition:      seeded,
		})
		agent.handleJoinLog(defaultJoinEvent(seeded))

		require.Equal(t, 2, agent.DoWork(0), "start and bind")
		require.NoError(t, agent.ServiceFailure())
		require.True(t, agent.IsLogBound())
		require.Equal(t, seeded, agent.LogPosition())
		require.Equal(t, 1, agent.ReadyCoordinationAcks())
	})
}

// TestAgent_Join_doesNotBind asserts every path that leaves a delivered join
// unbound.
//
// Each row runs two ticks. The first does start work only. The second does not
// make progress. The join stays pending. The join ack stays unready.
func TestAgent_Join_doesNotBind(t *testing.T) {
	tests := []struct {
		name         string
		adder        func(tb testing.TB) SubscriptionAdder
		joinPosition int64
		seedPosition int64
		wantFailure  error
	}{
		{
			name:  "no adder wired",
			adder: func(testing.TB) SubscriptionAdder { return nil },
		},
		{
			name:  "adder returns no subscription",
			adder: func(testing.TB) SubscriptionAdder { return &stubAdder{} },
		},
		{
			name: "subscription has no image",
			adder: func(testing.TB) SubscriptionAdder {
				sub := aeron.NewSubscriptionForHooks(9002, testLogStreamID, testLogChannel)
				return &stubAdder{sub: sub}
			},
		},
		{
			name: "image closed",
			adder: func(tb testing.TB) SubscriptionAdder {
				sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, 0)
				aeron.ImageInternalCloseForHooks(image)
				return &stubAdder{sub: sub}
			},
		},
		{
			// Agent logPosition is 0; event at 128 fails contiguity.
			name: "event position ahead of consumed position",
			adder: func(tb testing.TB) SubscriptionAdder {
				sub, _ := newLogImage(tb, testLogSessionID, testLogStreamID, 0)
				return &stubAdder{sub: sub}
			},
			joinPosition: 128,
			wantFailure:  ErrLogNotContiguous,
		},
		{
			// Image join position 64; event position 0.
			name: "image join position ahead of event position",
			adder: func(tb testing.TB) SubscriptionAdder {
				sub, _ := newLogImage(tb, testLogSessionID, testLogStreamID, 64)
				return &stubAdder{sub: sub}
			},
			wantFailure: ErrLogNotContiguous,
		},
		{
			name: "subscription add fails",
			adder: func(testing.TB) SubscriptionAdder {
				return &stubAdder{err: errors.New("add sub failed")}
			},
		},
		{
			name: "event position behind the recovery seed",
			adder: func(tb testing.TB) SubscriptionAdder {
				sub, _ := newLogImage(tb, testLogSessionID, testLogStreamID, 0)
				return &stubAdder{sub: sub}
			},
			seedPosition: 128,
			wantFailure:  ErrLogNotContiguous,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{}
			var got []error
			agent := NewAgent(AgentConfig{
				Service:       svc,
				Subscriptions: tc.adder(t),
				ErrorHandler:  collectErrors(&got),
			})
			if tc.seedPosition != 0 {
				agent.seedRecovery(codecs.RecoveryStateKeyFields{
					LeadershipTermID: nullValue,
					LogPosition:      tc.seedPosition,
				})
			}
			agent.handleJoinLog(defaultJoinEvent(tc.joinPosition))

			require.Equal(t, 1, agent.DoWork(0)) // start only
			if tc.wantFailure != nil {
				require.ErrorIs(t, agent.ServiceFailure(), tc.wantFailure)
			} else {
				require.NoError(t, agent.ServiceFailure())
			}

			require.False(t, agent.IsLogBound())
			require.True(t, agent.HasPendingJoin())
			require.Equal(t, 1, agent.PendingCoordinationAcks())
			require.Equal(t, 0, agent.ReadyCoordinationAcks())

			require.Equal(t, 0, agent.DoWork(0))
			require.True(t, agent.HasPendingJoin())
			require.False(t, agent.IsLogBound())
		})
	}
}

// TestAgent_Join_addRetryWaitsForInterval starts the wait on the tick after the
// failed add. A newer join does not wait.
func TestAgent_Join_addRetryWaitsForInterval(t *testing.T) {
	var got []error
	agent, adder := joinWithFailedSubscriptionAdd(
		t,
		collectErrors(&got),
		errors.New("add sub failed"),
	)
	require.Equal(t, 1, adder.calls, "first add")
	require.Len(t, got, 1, "first add reports")

	// A failed add can block past the interval.
	now := 10 * joinAddRetryIntervalNs
	agent.DoWork(now)
	require.Equal(t, 1, adder.calls, "tick after a blocking add")

	now += joinAddRetryIntervalNs - 1
	agent.DoWork(now)
	require.Equal(t, 1, adder.calls, "inside interval")
	require.Len(t, got, 1, "inside interval reports")

	now++
	agent.DoWork(now)
	require.Equal(t, 2, adder.calls, "at interval")
	require.Len(t, got, 2, "at interval reports")

	agent.DoWork(now)
	now += joinAddRetryIntervalNs - 1
	agent.DoWork(now)
	require.Equal(t, 2, adder.calls, "second wait")

	agent.handleJoinLog(defaultJoinEvent(0))
	agent.DoWork(now)
	require.Equal(t, 3, adder.calls, "newer join")

	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	adder.err, adder.sub = nil, sub
	agent.DoWork(now)
	now += joinAddRetryIntervalNs
	agent.DoWork(now)
	require.True(t, agent.IsLogBound())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_Join_supersession(t *testing.T) {
	svc := &recordingService{}
	// Image ready at join position 0. Deliver both joins before DoWork so the
	// second supersedes the first before bind.
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)

	agent := NewAgent(AgentConfig{
		Service:           svc,
		CommittedPosition: func() int64 { return 1 << 20 },
		Subscriptions:     &stubAdder{sub: sub},
	})

	// First join is pending and not bound.
	agent.handleJoinLog(defaultJoinEvent(0))
	require.Equal(t, 1, agent.PendingCoordinationAcks())
	require.Equal(t, 0, agent.ReadyCoordinationAcks())

	// Second join supersedes. First ack becomes ready without bind.
	// ReadyCoordinationAcks counts the ready prefix (one superseded join).
	second := defaultJoinEvent(0)
	second.memberID = 9
	second.role = codecs.RoleFollower
	agent.handleJoinLog(second)
	require.Equal(t, 2, agent.PendingCoordinationAcks())
	require.Equal(t, 1, agent.ReadyCoordinationAcks())
	require.True(t, agent.HasPendingJoin())

	// Bind the second join.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.True(t, agent.IsLogBound())
	require.False(t, agent.HasPendingJoin())
	require.Equal(t, 2, agent.ReadyCoordinationAcks())
	require.Equal(t, int32(9), agent.MemberID())
	require.Equal(t, codecs.RoleFollower, agent.Role())
}

func TestAgent_Join_handoff(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	// One frame under the first image.
	payload := encodeSessionMessage(
		t,
		codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        10,
		},
		[]byte("a"),
	)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)

	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, int64(span), agent.LogPosition())
	require.True(t, agent.IsLogBound())

	// New join at handoff position span. Changed stream forces close and re-add.
	const newSessionID int32 = 99
	const newStream int32 = 101
	replaceLogSubscription(t, agent, newSessionID, newStream, int64(span))

	next := defaultJoinEvent(int64(span))
	next.logSessionID = newSessionID
	next.logStreamID = newStream
	next.memberID = 11
	next.role = codecs.RoleCandidate
	agent.handleJoinLog(next)

	require.Equal(t, int64(span), agent.adapter.MaxLogPosition())
	require.True(t, agent.HasPendingJoin())
	require.True(t, agent.IsLogBound())

	// Position is already at the cap. Poll returns 0 and unbind runs.
	require.Equal(t, 0, agent.DoWork(0))
	require.False(t, agent.IsLogBound())
	require.Equal(t, codecs.RoleFollower, agent.Role())
	require.Equal(t, int64(span), agent.LogPosition())

	// Next DoWork binds the new join.
	work := agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 1)
	require.True(t, agent.IsLogBound())
	require.Equal(t, int32(11), agent.MemberID())
	require.Equal(t, codecs.RoleCandidate, agent.Role())
	require.False(t, agent.HasPendingJoin())
}

func TestAgent_Join_leaderBindClearsClosing(t *testing.T) {
	log, _, pub := bindCloseAgent(t, nil, codecs.RoleFollower)
	agent := log.agent
	ids := []int64{1, 2}
	for _, id := range ids {
		log.open(id, id)
		require.True(t, agent.CloseClientSession(id))
	}
	position := agent.LogPosition()
	replaceLogSubscription(t, agent, testLogSessionID, testLogStreamID, position)

	// rebind caps the image at position, unbinds it, then binds a JoinLog
	// with role.
	rebind := func(role codecs.Role) {
		t.Helper()
		event := defaultJoinEvent(position)
		event.role = role
		agent.handleJoinLog(event)
		agent.DoWork(0)
		require.False(t, agent.IsLogBound())
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.True(t, agent.IsLogBound())
		require.Equal(t, role, agent.Role())
	}
	// requireClosing asserts IsClosing per id, in ids order.
	requireClosing := func(want ...bool) {
		t.Helper()
		for i, id := range ids {
			session, ok := agent.ClientSession(id)
			require.True(t, ok)
			require.Equal(t, want[i], session.IsClosing())
		}
	}

	// Phase 1: a follower bind keeps both flags.
	rebind(codecs.RoleFollower)
	requireClosing(true, true)

	// Phase 2: a leader bind clears both flags.
	rebind(codecs.RoleLeader)
	requireClosing(false, false)

	// Phase 3: the service can repeat the close.
	offset := int(pub.Position())
	require.True(t, agent.CloseClientSession(1))
	fields, ok := codecs.TryDecodeCloseSessionAt(termWireAt(t, pub, offset), 0)
	require.True(t, ok)
	require.Equal(t, int64(1), fields.ClusterSessionID)
	requireClosing(true, false)
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_Join_replayChain(t *testing.T) {
	const (
		replaySessionID int32 = -17
		replayStreamID  int32 = 103
	)
	// replayJoinEvent is the replay JoinLog at startPosition with cap
	// stopPosition.
	replayJoinEvent := func(startPosition, stopPosition int64) logJoinEvent {
		event := defaultJoinEvent(startPosition)
		event.maxLogPosition = stopPosition
		event.logSessionID = replaySessionID
		event.logStreamID = replayStreamID
		event.role = codecs.RoleFollower
		return event
	}
	// bindReplay binds a fresh agent to a replay image at 0 with cap
	// stopPosition. writeFrames fills the image and returns the stop.
	bindReplay := func(
		t *testing.T,
		svc *recordingService,
		writeFrames func(image *aeron.Image) int64,
	) (*Agent, *int64, int64) {
		t.Helper()
		sub, image := newLogImage(t, replaySessionID, replayStreamID, 0)
		stop := writeFrames(image)
		committed := int64(0)
		agent := NewAgent(AgentConfig{
			Service:           svc,
			CommittedPosition: func() int64 { return committed },
			Subscriptions:     &stubAdder{sub: sub},
		})
		agent.handleJoinLog(replayJoinEvent(0, stop))
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.True(t, agent.IsLogBound())
		require.Equal(t, stop, agent.adapter.MaxLogPosition())
		return agent, &committed, stop
	}
	message := func(t *testing.T, sessionID int64) []byte {
		return encodeSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: sessionID,
				Timestamp:        10,
			},
			[]byte("m"),
		)
	}
	// requireLiveBound drives the agent until the live join at position
	// binds.
	requireLiveBound := func(t *testing.T, agent *Agent, position int64) {
		t.Helper()
		for range 4 {
			agent.DoWork(0)
			if agent.IsLogBound() && agent.adapter.MaxLogPosition() == math.MaxInt64 {
				break
			}
		}
		require.NoError(t, agent.ServiceFailure())
		require.True(t, agent.IsLogBound())
		require.False(t, agent.HasPendingJoin())
		require.Equal(t, int64(math.MaxInt64), agent.adapter.MaxLogPosition())
		require.Equal(t, position, agent.LogPosition())
		require.Equal(t, codecs.RoleLeader, agent.Role())
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "replay to the cap then the live join",
			run: func(t *testing.T) {
				svc := &recordingService{}
				agent, committed, stop := bindReplay(t, svc, func(image *aeron.Image) int64 {
					first := writeAgentFrame(t, image, 0, message(t, 1))
					second := writeAgentFrame(t, image, first, message(t, 2))
					return int64(first + second)
				})

				// Phase 1: consume to the cap, then unbind.
				*committed = stop
				for range 4 {
					agent.DoWork(0)
				}
				require.NoError(t, agent.ServiceFailure())
				require.False(t, agent.IsLogBound())
				require.Equal(t, stop, agent.LogPosition())
				require.Len(t, svc.messages, 2)

				// Phase 2: the live join at the cap binds.
				replaceLogSubscription(t, agent, testLogSessionID, testLogStreamID, stop)
				agent.handleJoinLog(defaultJoinEvent(stop))
				requireLiveBound(t, agent, stop)
			},
		},
		{
			name: "live join while the replay image is below the cap",
			run: func(t *testing.T) {
				svc := &recordingService{}
				var firstEnd int64
				agent, committed, stop := bindReplay(t, svc, func(image *aeron.Image) int64 {
					first := writeAgentFrame(t, image, 0, message(t, 1))
					second := writeAgentFrame(t, image, first, message(t, 2))
					firstEnd = int64(first)
					return int64(first + second)
				})

				// Phase 1: consume below the cap.
				*committed = firstEnd
				agent.DoWork(0)
				require.Equal(t, firstEnd, agent.LogPosition())
				require.True(t, agent.IsLogBound())

				// Phase 2: the live join waits as a pending join.
				replaceLogSubscription(t, agent, testLogSessionID, testLogStreamID, stop)
				agent.handleJoinLog(defaultJoinEvent(stop))
				require.True(t, agent.HasPendingJoin())
				require.Equal(t, stop, agent.adapter.MaxLogPosition())

				// Phase 3: the replay image consumes to the cap, unbinds, and
				// the live join binds.
				*committed = stop
				requireLiveBound(t, agent, stop)
				require.Len(t, svc.messages, 2)
			},
		},
		{
			name: "live image at the start then the replay join",
			run: func(t *testing.T) {
				svc := &recordingService{}
				agent, _, _ := bindAgent(t, svc, 0)
				const stop int64 = 4096

				replaceLogSubscription(t, agent, replaySessionID, replayStreamID, 0)
				agent.handleJoinLog(replayJoinEvent(0, stop))
				require.True(t, agent.HasPendingJoin())

				// The live image sits at the cap. It unbinds, then the replay
				// image binds.
				for range 4 {
					agent.DoWork(0)
					if agent.IsLogBound() && !agent.HasPendingJoin() {
						break
					}
				}
				require.NoError(t, agent.ServiceFailure())
				require.True(t, agent.IsLogBound())
				require.False(t, agent.HasPendingJoin())
				require.Equal(t, stop, agent.adapter.MaxLogPosition())
				require.Equal(t, int64(0), agent.LogPosition())
				require.Equal(t, codecs.RoleFollower, agent.Role())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestAgent_Join_paddingHandoff(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	// Message then padding at the handoff cap.
	payload := encodeSessionMessage(
		t,
		codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        5,
		},
		[]byte("p"),
	)
	msgSpan := writeAgentFrame(t, image, 0, payload)
	const padLen int32 = 64
	writeAgentPadding(t, image, msgSpan, padLen)
	handoff := int64(msgSpan) + int64(padLen)
	*committed = handoff

	// Consume the message. Padding may also advance position in the same poll.
	agent.DoWork(0)
	require.Equal(t, int64(msgSpan), agent.LogPosition())
	// Drive until image position reaches handoff.
	for image.Position() < handoff && agent.ServiceFailure() == nil {
		agent.DoWork(0)
	}
	// Still bound: maxLogPosition is still MaxInt64 until the handoff join caps it.
	require.True(t, agent.IsLogBound())

	const newSessionID int32 = 55
	const newStream int32 = 102
	replaceLogSubscription(t, agent, newSessionID, newStream, handoff)

	next := defaultJoinEvent(handoff)
	next.logSessionID = newSessionID
	next.logStreamID = newStream
	agent.handleJoinLog(next)

	// Cap forces unbind. Padding adoption must lift LogPosition to handoff.
	agent.DoWork(0)
	require.False(t, agent.IsLogBound())
	require.Equal(t, handoff, agent.LogPosition())

	// Next join at handoff binds cleanly. No spurious contiguity failure.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.True(t, agent.IsLogBound())
	require.Equal(t, handoff, agent.LogPosition())
}

func TestAgent_Unbind_endOfStream(t *testing.T) {
	tests := []struct {
		name      string
		padLen    int32
		wordAbove int64
		wordFirst bool
		wantBound bool
	}{
		{name: "word at the consumed position"},
		{name: "word set before the agent polls the last frame", wordFirst: true},
		{name: "word above the consumed position", wordAbove: 64, wantBound: true},
		{name: "trailing padding up to the word", padLen: 64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{}
			agent, image, committed := bindAgent(t, svc, 0)

			span := int64(writeAgentFrame(t, image, 0, encodeTrivialSessionMessage(t)))
			streamEnd := span + int64(tc.padLen)
			if tc.padLen > 0 {
				writeAgentPadding(t, image, int(span), tc.padLen)
			}
			word := streamEnd + tc.wordAbove

			// Phase 1: deliver the frame.
			*committed = span
			if tc.wordFirst {
				aeron.ImageSetEndOfStreamPositionForHooks(image, word)
			}
			require.Equal(t, 1, agent.DoWork(0))
			require.True(t, agent.IsLogBound())
			require.Equal(t, span, agent.LogPosition())
			require.Equal(t, span, image.Position())

			// Phase 2: a zero-fragment tick unbinds at end of stream.
			if !tc.wordFirst {
				aeron.ImageSetEndOfStreamPositionForHooks(image, word)
			}
			*committed = streamEnd
			require.Equal(t, 0, agent.DoWork(0))
			require.NoError(t, agent.ServiceFailure())
			require.Equal(t, int64(math.MaxInt64), agent.adapter.MaxLogPosition())
			require.False(t, image.IsClosed())
			require.Equal(t, tc.wantBound, agent.IsLogBound())
			if tc.wantBound {
				require.Equal(t, codecs.RoleLeader, agent.Role())
				require.Equal(t, span, agent.LogPosition())
				require.Equal(t, 0, agent.DoWork(0))
				require.True(t, agent.IsLogBound())
				return
			}
			require.Equal(t, codecs.RoleFollower, agent.Role())
			require.Equal(t, streamEnd, agent.LogPosition())

			// Phase 3: rejoin on the reused subscription.
			const (
				newSessionID           int32 = 77
				newImageRegistrationID int64 = 9102
			)
			sub := agent.logSubscription
			next := aeron.NewTestImageForSubscriptionHooksWithSessionID(
				t,
				sub,
				newSessionID,
				newImageRegistrationID,
				streamEnd,
			)
			require.True(t, aeron.SubscriptionAddImageForHooks(sub, next))

			join := defaultJoinEvent(streamEnd)
			join.logSessionID = newSessionID
			join.memberID = 11
			join.role = codecs.RoleCandidate
			agent.handleJoinLog(join)
			require.True(t, agent.HasPendingJoin())

			require.GreaterOrEqual(t, agent.DoWork(0), 1)
			require.NoError(t, agent.ServiceFailure())
			require.True(t, agent.IsLogBound())
			require.Same(t, next, agent.adapter.Image())
			require.Same(t, sub, agent.logSubscription)
			require.False(t, agent.HasPendingJoin())
			require.Equal(t, streamEnd, agent.LogPosition())
			require.Equal(t, int32(11), agent.MemberID())
			require.Equal(t, codecs.RoleCandidate, agent.Role())
		})
	}
}

func TestAgent_Unbind_imageClosedWhileBound(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent

	// Session 1 holds a publication. Sessions 2 and 3 have an add in flight.
	held := aeron.NewTestPublicationForHooksWithConnected(t, true)
	log.open(1, 1)
	adder.next(1, responsePoll{publication: held, done: true})
	log.open(2, 1)
	log.open(3, 1)
	require.Equal(t, 1, agent.ResponsePublicationCount())
	require.Equal(t, 2, agent.PendingResponsePublicationAdds())

	// Both polls of add 2 stay pending. The add work poll of add 3 stays
	// pending. Its disconnect poll returns a publication.
	returned := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(3, responsePoll{})
	adder.next(3, responsePoll{publication: returned, done: true})
	polls := len(adder.polls)

	// The image has no cap. Unbind uses IsClosed. Add work counts its two
	// pending polls. The disconnect polls do not count.
	aeron.ImageInternalCloseForHooks(log.image)
	require.Equal(t, 2, agent.DoWork(0))
	require.False(t, agent.IsLogBound())
	require.Equal(t, codecs.RoleFollower, agent.Role())
	require.Equal(t, []int64{2, 3, 2, 3}, adder.polls[polls:])
	require.True(t, held.IsClosed())
	require.True(t, returned.IsClosed())
	require.Equal(t, []int64{2}, adder.cancels)
	require.Empty(t, adder.inFlight)
	require.Equal(t, 0, agent.ResponsePublicationCount())
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	requireOffers(t, agent, MockedOffer, 1, 2, 3)
	require.Equal(t, 3, agent.SessionCount())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_Unbind_fatalAddPollEndsEveryAdd(t *testing.T) {
	adder := newFakeResponseAdder()
	svc := &lifecycleRecordingService{}
	var got []error
	log := bindSessionLog(t, AgentConfig{
		Service:              svc,
		ResponsePublications: adder,
		ErrorHandler:         collectErrors(&got),
	}, codecs.RoleLeader)
	log.open(1, 1)
	log.open(2, 2)
	adder.next(1, responsePoll{})
	adder.next(2, responsePoll{})
	adder.next(1, responsePoll{err: aeron.ErrClientClosed})
	adder.next(2, responsePoll{err: errors.New("registration boom")})
	polls := len(adder.polls)
	roles := len(svc.roles)
	background := len(svc.backgroundNows)

	aeron.ImageInternalCloseForHooks(log.image)
	log.agent.DoWork(0)
	require.Equal(t, []int64{1, 2, 1, 2}, adder.polls[polls:])
	require.Equal(t, []int64{1, 2}, adder.cancels)
	require.False(t, log.agent.IsLogBound())
	require.Empty(t, adder.inFlight)
	requireFailureReported(t, log.agent, got, ErrResponsePublicationAdd, aeron.ErrClientClosed)
	require.Len(t, svc.roles, roles)
	require.Len(t, svc.backgroundNows, background)
	require.Equal(t, codecs.RoleLeader, log.agent.Role())
}

func TestAgent_JoinEgress_fatalAddStopsLoop(t *testing.T) {
	adder := newFakeResponseAdder()
	adder.offerErrs = map[int32]error{11: aeron.ErrClientClosed}
	svc := &lifecycleRecordingService{}
	var got []error
	sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	committed := int64(0)
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		Service:              svc,
		CommittedPosition:    func() int64 { return committed },
		Subscriptions:        &stubAdder{sub: sub},
		ResponsePublications: adder,
		ModuleProxy:          proxy,
		ErrorHandler:         collectErrors(&got),
	})
	require.NoError(t, agent.restoreClientSession(sampleClientSession(1, testResponseChannel)))
	require.NoError(t, agent.restoreClientSession(sampleClientSession(2, testResponseChannel)))
	log := &takeLog{image: image, committed: &committed}
	log.timer(t, 11)

	event := defaultJoinEvent(0)
	event.isStartup = false
	agent.handleJoinLog(event)
	agent.DoWork(0)

	requireFailureReported(t, agent, got, ErrResponsePublicationAdd, aeron.ErrClientClosed)
	require.Len(t, adder.offers, 1)
	require.Equal(t, int32(11), adder.offers[0].streamID)
	require.Equal(t, nullValue, agent.sessions[2].addCorrelationID)
	require.Empty(t, svc.roles)
	require.Empty(t, svc.backgroundNows)
	require.Empty(t, svc.timers)
	require.Equal(t, int64(0), agent.LogPosition())
	require.Equal(t, int64(0), image.Position())
	require.Equal(t, int64(0), agent.NextAckID())
	require.Empty(t, decodeServiceAcksFromPub(t, pub))

	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, int64(0), agent.NextAckID())
	require.Empty(t, decodeServiceAcksFromPub(t, pub))
}
