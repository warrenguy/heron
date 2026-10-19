//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

// recordingService records ClusteredService callbacks for agent unit tests.
type recordingService struct {
	startCount int
	startErr   error

	opens    []sessionOpenRec
	closes   []sessionCloseRec
	messages []sessionMessageRec
	timers   []timerRec
	order    []string
}

type sessionOpenRec struct {
	sessionID int64
	timestamp int64
}

type sessionCloseRec struct {
	sessionID int64
	timestamp int64
	reason    clusterwire.CloseReason
}

type sessionMessageRec struct {
	sessionID int64
	timestamp int64
	payload   []byte
}

type timerRec struct {
	correlationID int64
	timestamp     int64
}

func (s *recordingService) Start(Cluster) error {
	s.startCount++
	return s.startErr
}

func (s *recordingService) SessionOpen(sessionID, timestamp int64) {
	s.opens = append(s.opens, sessionOpenRec{sessionID: sessionID, timestamp: timestamp})
	s.order = append(s.order, "open")
}

func (s *recordingService) SessionClose(
	sessionID, timestamp int64,
	reason clusterwire.CloseReason,
) {
	s.closes = append(s.closes, sessionCloseRec{
		sessionID: sessionID,
		timestamp: timestamp,
		reason:    reason,
	})
	s.order = append(s.order, "close")
}

func (s *recordingService) SessionMessage(sessionID, timestamp int64, payload []byte) {
	s.messages = append(s.messages, sessionMessageRec{
		sessionID: sessionID,
		timestamp: timestamp,
		payload:   append([]byte(nil), payload...),
	})
	s.order = append(s.order, "message")
}

func (s *recordingService) TimerEvent(correlationID, timestamp int64) {
	s.timers = append(s.timers, timerRec{
		correlationID: correlationID,
		timestamp:     timestamp,
	})
	s.order = append(s.order, "timer")
}

// nltRecordingService adds NewLeadershipTermEventHandler to recordingService.
type nltRecordingService struct {
	recordingService
	nlt []nltRec
}

type nltRec struct {
	leadershipTermID    int64
	logPosition         int64
	timestamp           int64
	termBaseLogPosition int64
	leaderMemberID      int32
	logSessionID        int32
	timeUnit            clusterwire.ClusterTimeUnit
	appVersion          int32
}

func (s *nltRecordingService) NewLeadershipTermEvent(
	leadershipTermID int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberID int32,
	logSessionID int32,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) {
	s.nlt = append(s.nlt, nltRec{
		leadershipTermID:    leadershipTermID,
		logPosition:         logPosition,
		timestamp:           timestamp,
		termBaseLogPosition: termBaseLogPosition,
		leaderMemberID:      leaderMemberID,
		logSessionID:        logSessionID,
		timeUnit:            timeUnit,
		appVersion:          appVersion,
	})
	s.order = append(s.order, "nlt")
}

// snapshotRecordingService adds TakeSnapshotHandler to recordingService.
// Each TakeSnapshot call pops the first hookErrs entry. An empty queue falls
// back to hookErr.
type snapshotRecordingService struct {
	recordingService
	pubs             []*aeron.ExclusivePublication
	hookErrs         []error
	hookErr          error
	write            []byte
	closePublication bool
}

func (s *snapshotRecordingService) TakeSnapshot(
	publication *aeron.ExclusivePublication,
) error {
	s.pubs = append(s.pubs, publication)
	s.order = append(s.order, "snapshot")
	err := s.hookErr
	if len(s.hookErrs) > 0 {
		err = s.hookErrs[0]
		s.hookErrs = s.hookErrs[1:]
	}
	if err != nil {
		return err
	}
	if s.write != nil {
		if publication == nil {
			return errors.New("nil publication")
		}
		if pos := publication.Offer(s.write); pos <= 0 {
			return fmt.Errorf("offer failed: %d", pos)
		}
	}
	if s.closePublication {
		return publication.Close()
	}
	return nil
}

// stubAdder returns a fixed subscription (or error) from AddSubscription.
type stubAdder struct {
	sub *aeron.Subscription
	err error
}

func (s *stubAdder) AddSubscription(int32, string) (*aeron.Subscription, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.sub, nil
}

const (
	testLogChannel   = "aeron:ipc"
	testLogStreamID  = int32(100)
	testLogSessionID = int32(42)
	testMemberID     = int32(7)
)

func defaultJoinEvent(logPosition int64) logJoinEvent {
	return logJoinEvent{
		logPosition:    logPosition,
		maxLogPosition: math.MaxInt64,
		memberID:       testMemberID,
		logSessionID:   testLogSessionID,
		logStreamID:    testLogStreamID,
		isStartup:      true,
		role:           codecs.RoleLeader,
		logChannel:     testLogChannel,
	}
}

// newLogImage builds a subscription with one image on testLogChannel.
//
// Registration ids are fixed. Each call builds an independent subscription and
// image; no assertion reads either id.
func newLogImage(
	tb testing.TB,
	sessionID, streamID int32,
	joinPosition int64,
) (*aeron.Subscription, *aeron.Image) {
	tb.Helper()
	const (
		subRegistrationID   int64 = 9001
		imageRegistrationID int64 = 9101
	)
	sub := aeron.NewSubscriptionForHooks(subRegistrationID, streamID, testLogChannel)
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		tb,
		sub,
		sessionID,
		imageRegistrationID,
		joinPosition,
	)
	require.True(tb, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, image
}

// bindAgent joins and binds an image at logPosition with the test session.
//
// Returns the agent, bound image, and committed-position variable.
func bindAgent(
	tb testing.TB,
	svc ClusteredService,
	joinPosition int64,
) (*Agent, *aeron.Image, *int64) {
	tb.Helper()

	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, joinPosition)

	committed := joinPosition
	agent := NewAgent(AgentConfig{
		Service:           svc,
		CommittedPosition: func() int64 { return committed },
		Subscriptions:     &stubAdder{sub: sub},
	})
	agent.handleJoinLog(defaultJoinEvent(joinPosition))
	require.Equal(tb, 1, agent.PendingCoordinationAcks())
	require.Equal(tb, 0, agent.ReadyCoordinationAcks())
	require.True(tb, agent.HasPendingJoin())

	work := agent.DoWork(0)
	require.NoError(tb, agent.ServiceFailure())
	// Start (1) + bind (1)
	require.GreaterOrEqual(tb, work, 2)
	require.True(tb, agent.IsLogBound())
	require.False(tb, agent.HasPendingJoin())
	require.Equal(tb, 1, agent.ReadyCoordinationAcks())
	require.Equal(tb, testMemberID, agent.MemberID())
	require.Equal(tb, codecs.RoleLeader, agent.Role())
	require.Equal(tb, joinPosition, agent.LogPosition())

	return agent, image, &committed
}

// bindImage binds cfg onto a hooks image at joinPosition without a JoinLog
// entry. Action-hook tests use this so the action is the first queued ack.
func bindImage(
	tb testing.TB,
	cfg AgentConfig,
	joinPosition int64,
) (*Agent, *aeron.Image, *int64) {
	tb.Helper()
	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, joinPosition)
	committed := joinPosition
	cfg.CommittedPosition = func() int64 { return committed }
	if cfg.Subscriptions == nil {
		cfg.Subscriptions = &stubAdder{sub: sub}
	}
	agent := NewAgent(cfg)
	agent.adapter.SetImage(image)
	return agent, image, &committed
}

func writeAgentFrame(
	tb assertTB,
	image *aeron.Image,
	frameOffset int,
	payload []byte,
) int {
	tb.Helper()
	writeUnfragmentedFrame(tb, image, frameOffset, payload)
	return logbuffer.AlignFragmentChunkLength(len(payload))
}

func writeAgentPadding(
	tb assertTB,
	image *aeron.Image,
	frameOffset int,
	paddingLength int32,
) {
	tb.Helper()
	aeron.WriteCommittedPaddingFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		frameOffset,
		hookTermID,
		int32(frameOffset),
		paddingLength,
	)
}

// replaceLogSubscription swaps the stub adder for a new image at joinPosition.
func replaceLogSubscription(
	tb testing.TB,
	agent *Agent,
	sessionID, streamID int32,
	joinPosition int64,
) {
	tb.Helper()
	sub, _ := newLogImage(tb, sessionID, streamID, joinPosition)
	agent.subscriptions = &stubAdder{sub: sub}
}

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

func TestAgent_JoinLifecycle(t *testing.T) {
	svc := &recordingService{}
	agent, _, _ := bindAgent(t, svc, 0)
	// Start ran. Join ack remains queued after bind (ready, not drained).
	require.Equal(t, 1, svc.startCount)
	require.Equal(t, 1, agent.PendingCoordinationAcks())
}

// TestAgent_Join_doesNotBind asserts every path that leaves a delivered join
// unbound: no adder, no image, closed image, both contiguity mismatches, and a
// failed subscription add.
//
// Each row runs two ticks. The first does start work only. The second makes no
// progress. The join stays pending and the join ack stays unready throughout.
func TestAgent_Join_doesNotBind(t *testing.T) {
	addErr := errors.New("add sub failed")

	tests := []struct {
		name         string
		adder        func(tb testing.TB) SubscriptionAdder
		joinPosition int64
		wantFailure  error
	}{
		{
			name:  "no adder wired",
			adder: func(testing.TB) SubscriptionAdder { return nil },
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
			wantFailure:  errLogNotContiguous,
		},
		{
			// Image join position 64; event position 0.
			name: "image join position ahead of event position",
			adder: func(tb testing.TB) SubscriptionAdder {
				sub, _ := newLogImage(tb, testLogSessionID, testLogStreamID, 64)
				return &stubAdder{sub: sub}
			},
			wantFailure: errLogNotContiguous,
		},
		{
			name:        "subscription add fails",
			adder:       func(testing.TB) SubscriptionAdder { return &stubAdder{err: addErr} },
			wantFailure: addErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{}
			agent := NewAgent(AgentConfig{
				Service:       svc,
				Subscriptions: tc.adder(t),
			})
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

func TestAgent_SessionRegistry(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	openFields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		ClusterSessionID: 100,
		Timestamp:        50,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0x01},
	}
	openPayload := encodeSessionOpen(t, openFields)
	openSpan := writeAgentFrame(t, image, 0, openPayload)
	*committed = int64(openSpan)

	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 1, agent.SessionCount())
	require.True(t, agent.HasSession(100))
	require.Len(t, svc.opens, 1)
	require.Equal(t, int64(100), svc.opens[0].sessionID)
	require.Equal(t, int64(50), agent.ClusterTime())
	require.Equal(t, int64(openSpan), agent.LogPosition())

	session, ok := agent.ClientSession(100)
	require.True(t, ok)
	require.Equal(t, int64(100), session.ID())
	require.Equal(t, int32(102), session.ResponseStreamID())
	require.Equal(t, "aeron:ipc", session.ResponseChannel())
	require.Equal(t, []byte{0x01}, session.EncodedPrincipal())

	// Opens for 9, 2 and 5 arrive after 100. The walk sorts by id. Upstream
	// walks in open order.
	offset := openSpan
	for _, id := range []int64{9, 2, 5} {
		fields := openFields
		fields.ClusterSessionID = id
		offset += writeAgentFrame(t, image, offset, encodeSessionOpen(t, fields))
	}
	*committed = int64(offset)
	require.Equal(t, 3, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())

	var walked []int64
	for session := range agent.ClientSessions() {
		walked = append(walked, session.ID())
	}
	require.Equal(t, []int64{2, 5, 9, 100}, walked)

	// An early break stops the walk.
	walked = walked[:0]
	for session := range agent.ClientSessions() {
		walked = append(walked, session.ID())
		if len(walked) == 2 {
			break
		}
	}
	require.Equal(t, []int64{2, 5}, walked)

	session, ok = agent.ClientSession(404)
	require.False(t, ok)
	require.Nil(t, session)

	// Clash: second open with same id marks sticky failure, aborts, no callback.
	clashFields := openFields
	clashFields.Timestamp = 51
	clashPayload := encodeSessionOpen(t, clashFields)
	clashSpan := writeAgentFrame(t, image, offset, clashPayload)
	*committed = int64(offset + clashSpan)

	require.Equal(t, 0, agent.DoWork(0)) // ABORT does not count fragment
	require.ErrorIs(t, agent.ServiceFailure(), errSessionClash)
	require.Len(t, svc.opens, 4)
	require.Equal(t, 4, agent.SessionCount())
	// Position frozen before clash frame.
	require.Equal(t, int64(offset), agent.LogPosition())
	require.Equal(t, 0, agent.DoWork(0))
}

func TestAgent_SessionClose_knownAndUnknown(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	openPayload := encodeSessionOpen(t, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 7,
		Timestamp:        1,
		ResponseChannel:  []byte("c"),
	})
	openSpan := writeAgentFrame(t, image, 0, openPayload)

	closeKnown := encodeSessionClose(t, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 7,
		Timestamp:        2,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	closeSpan := writeAgentFrame(t, image, openSpan, closeKnown)

	closeUnknown := encodeSessionClose(t, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 999,
		Timestamp:        3,
		CloseReason:      clusterwire.CloseReason_TIMEOUT,
	})
	unkSpan := writeAgentFrame(t, image, openSpan+closeSpan, closeUnknown)

	*committed = int64(openSpan + closeSpan + unkSpan)
	require.Equal(t, 3, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 0, agent.SessionCount())
	require.False(t, agent.HasSession(7))
	require.Len(t, svc.closes, 1)
	require.Equal(t, int64(7), svc.closes[0].sessionID)
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, svc.closes[0].reason)
	require.Equal(t, int64(3), agent.ClusterTime())

	egressTests := []struct {
		name string

		// setup scripts the add of session 7 (correlation id 1) after the
		// open. Returns the publication the close must close, or nil. Nil
		// leaves the add in flight.
		setup func(t *testing.T, log *sessionLog, adder *fakeResponseAdder) *aeron.Publication

		wantPolls   []int64
		wantCancels []int64
	}{
		{
			name: "held publication",
			setup: func(t *testing.T, log *sessionLog, adder *fakeResponseAdder) *aeron.Publication {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				adder.next(1, responsePoll{publication: pub, done: true})
				log.agent.DoWork(0)
				require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
				return pub
			},
			wantPolls: []int64{1},
		},
		{
			// The tick poll of the close DoWork stays pending. The close
			// polls once more, then cancels.
			name:        "add in flight stays pending",
			wantPolls:   []int64{1, 1},
			wantCancels: []int64{1},
		},
		{
			// The tick poll stays pending. The close poll returns the ready
			// publication, as after a conductor ready mark.
			name: "add in flight ready at close",
			setup: func(t *testing.T, _ *sessionLog, adder *fakeResponseAdder) *aeron.Publication {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				adder.next(1, responsePoll{})
				adder.next(1, responsePoll{publication: pub, done: true})
				return pub
			},
			wantPolls: []int64{1, 1},
		},
	}
	for _, tc := range egressTests {
		t.Run(tc.name, func(t *testing.T) {
			adder := newFakeResponseAdder()
			log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
			log.open(7, 1)
			var pub *aeron.Publication
			if tc.setup != nil {
				pub = tc.setup(t, log, adder)
			}
			session, ok := log.agent.ClientSession(7)
			require.True(t, ok)

			log.close(7, 2)
			require.Len(t, svc.closes, 1)
			_, ok = log.agent.ClientSession(7)
			require.False(t, ok)
			require.Equal(t, aeron.NotConnected, session.Offer([]byte("late")))
			require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
			require.Empty(t, adder.inFlight)
			require.Equal(t, tc.wantPolls, adder.polls)
			require.Equal(t, tc.wantCancels, adder.cancels)
			if pub != nil {
				require.True(t, pub.IsClosed())
			}
			require.NoError(t, log.agent.ServiceFailure())
		})
	}
}

func TestAgent_SessionMessage_unknownSession(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	payload := encodeSessionMessage(
		t,
		codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: 404,
			Timestamp:        9,
		},
		[]byte("body"),
	)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)

	require.Equal(t, 1, agent.DoWork(0))
	require.Len(t, svc.messages, 1)
	require.Equal(t, []byte("body"), svc.messages[0].payload)
	require.Equal(t, int64(404), svc.messages[0].sessionID)
	require.Equal(t, 0, agent.SessionCount())
}

// responsePoll is one scripted PollAddPublication result.
type responsePoll struct {
	publication *aeron.Publication
	done        bool
	err         error
}

// fakeResponseAdder scripts ResponsePublicationAdder results. Each poll pops
// the first scripted result for its correlation id. An empty script polls
// pending. inFlight holds each started add until a done poll, an error poll,
// or a cancel.
type fakeResponseAdder struct {
	offers   []transportAdd
	offerErr error
	nextID   int64
	script   map[int64][]responsePoll
	inFlight map[int64]bool
	polls    []int64
	cancels  []int64
}

func newFakeResponseAdder() *fakeResponseAdder {
	return &fakeResponseAdder{
		nextID:   1,
		script:   make(map[int64][]responsePoll),
		inFlight: make(map[int64]bool),
	}
}

func (f *fakeResponseAdder) OfferAddPublication(streamID int32, channel string) (int64, error) {
	f.offers = append(f.offers, transportAdd{streamID: streamID, channel: channel})
	if f.offerErr != nil {
		return 0, f.offerErr
	}
	id := f.nextID
	f.nextID++
	f.inFlight[id] = true
	return id, nil
}

func (f *fakeResponseAdder) PollAddPublication(
	correlationID int64,
) (*aeron.Publication, bool, error) {
	f.polls = append(f.polls, correlationID)
	queue := f.script[correlationID]
	if len(queue) == 0 {
		return nil, false, nil
	}
	result := queue[0]
	f.script[correlationID] = queue[1:]
	if result.done || result.err != nil {
		delete(f.inFlight, correlationID)
	}
	return result.publication, result.done, result.err
}

func (f *fakeResponseAdder) CancelPending(correlationID int64) {
	f.cancels = append(f.cancels, correlationID)
	delete(f.inFlight, correlationID)
}

// next scripts the next poll result for correlationID.
func (f *fakeResponseAdder) next(correlationID int64, result responsePoll) {
	f.script[correlationID] = append(f.script[correlationID], result)
}

// sessionLog writes log frames to a bound agent image and delivers them.
type sessionLog struct {
	*takeLog
	tb    testing.TB
	agent *Agent
}

// bindSessionAgent binds a recording agent with adder and sets role. A nil
// adder leaves ResponsePublications unset.
func bindSessionAgent(
	tb testing.TB,
	adder ResponsePublicationAdder,
	role codecs.Role,
) (*sessionLog, *recordingService) {
	tb.Helper()
	svc := &recordingService{}
	cfg := AgentConfig{Service: svc, ResponsePublications: adder}
	agent, image, committed := bindImage(tb, cfg, 0)
	agent.role = role
	log := &sessionLog{
		takeLog: &takeLog{image: image, committed: committed},
		tb:      tb,
		agent:   agent,
	}
	return log, svc
}

// deliver writes payload, commits it, and runs one DoWork. Returns the work
// count.
func (l *sessionLog) deliver(payload []byte) int {
	l.tb.Helper()
	l.write(l.tb, payload)
	return l.agent.DoWork(0)
}

func (l *sessionLog) open(id, timestamp int64) {
	l.tb.Helper()
	l.deliver(encodeSessionOpen(l.tb, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        timestamp,
		ResponseStreamID: testResponseStreamID,
		ResponseChannel:  []byte(testResponseChannel),
	}))
	require.NoError(l.tb, l.agent.ServiceFailure())
	require.True(l.tb, l.agent.HasSession(id))
}

func (l *sessionLog) close(id, timestamp int64) {
	l.tb.Helper()
	l.deliver(encodeSessionClose(l.tb, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        timestamp,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	}))
	require.NoError(l.tb, l.agent.ServiceFailure())
	require.False(l.tb, l.agent.HasSession(id))
}

const (
	testResponseStreamID = int32(102)
	testResponseChannel  = "aeron:udp?endpoint=localhost:20121"
)

// requireEgressFrame asserts the frame at stream position start on pub
// decodes to want and payload.
func requireEgressFrame(
	tb assertTB,
	pub *aeron.Publication,
	start int64,
	want codecs.SessionMessageHeaderFields,
	payload []byte,
) {
	tb.Helper()
	termLength := int64(logbuffer.TermMinLength)
	termIndex := int((start / termLength) % logbuffer.PartitionCount)
	termOffset := int(start % termLength)
	termBuffer := pub.TermBuffer(termIndex)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuffer, termOffset))
	headerLength := codecs.SessionMessageHeaderEncodedLen()
	require.Equal(tb, logbuffer.DataHeaderLength+headerLength+len(payload), frameLength)
	wire := termBuffer.Bytes()[termOffset+logbuffer.OffsetFramePayload : termOffset+frameLength]
	got, ok := codecs.TryDecodeSessionMessageHeaderAt(wire, 0)
	require.True(tb, ok)
	require.Equal(tb, want, got)
	require.Equal(tb, payload, wire[headerLength:])
}

func TestAgent_ClientSession_openAsLeader(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent

	// Phase 1: the open starts one add. The session holds no publication.
	log.open(7, 50)
	require.Len(t, svc.opens, 1)
	require.Equal(t, []transportAdd{{
		streamID: testResponseStreamID,
		channel:  testResponseChannel,
	}}, adder.offers)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())
	session, ok := agent.ClientSession(7)
	require.True(t, ok)
	require.Equal(t, int64(7), session.ID())
	require.Equal(t, testResponseStreamID, session.ResponseStreamID())
	require.Equal(t, testResponseChannel, session.ResponseChannel())
	require.Empty(t, session.EncodedPrincipal())
	require.Equal(t, aeron.NotConnected, session.Offer([]byte("early")))

	// Phase 2: one DoWork resolves the add.
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())

	// Phase 3: the offer frames the payload behind one SessionMessageHeader.
	payload := []byte("echo")
	require.Positive(t, session.Offer(payload))
	requireEgressFrame(t, pub, 0, codecs.SessionMessageHeaderFields{
		LeadershipTermID: nullValue,
		ClusterSessionID: 7,
		Timestamp:        agent.ClusterTime(),
	}, payload)
	require.Equal(t, int64(50), agent.ClusterTime())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_ClientSession_openAsFollower(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleFollower)

	log.open(7, 50)
	require.Len(t, svc.opens, 1)
	require.Empty(t, adder.offers)
	require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
	session, ok := log.agent.ClientSession(7)
	require.True(t, ok)
	require.Equal(t, MockedOffer, session.Offer([]byte("m")))
	require.NoError(t, log.agent.ServiceFailure())
}

// TestAgent_ClientSession_offerCodes asserts each Offer result. No code
// marks a failure or closes the session.
func TestAgent_ClientSession_offerCodes(t *testing.T) {
	addErr := errors.New("add publication boom")

	tests := []struct {
		name string
		role codecs.Role

		// configure returns the adder the agent gets. Nil passes the fake.
		configure func(adder *fakeResponseAdder) ResponsePublicationAdder

		// poll scripts the next poll of add 1, then runs one DoWork. Nil
		// leaves the add in flight.
		poll func(t *testing.T) responsePoll

		closePublication bool
		oversize         bool
		wantPending      int
		want             int64
	}{
		{name: "follower", role: codecs.RoleFollower, want: MockedOffer},
		{
			name:      "leader with no adder",
			role:      codecs.RoleLeader,
			configure: func(*fakeResponseAdder) ResponsePublicationAdder { return nil },
			want:      aeron.NotConnected,
		},
		{
			name: "leader with a typed-nil adder",
			role: codecs.RoleLeader,
			configure: func(*fakeResponseAdder) ResponsePublicationAdder {
				return (*fakeResponseAdder)(nil)
			},
			want: aeron.NotConnected,
		},
		{
			name: "add offer error",
			role: codecs.RoleLeader,
			configure: func(adder *fakeResponseAdder) ResponsePublicationAdder {
				adder.offerErr = addErr
				return adder
			},
			want: aeron.NotConnected,
		},
		{name: "add in flight", role: codecs.RoleLeader, wantPending: 1, want: aeron.NotConnected},
		{
			name: "terminal add",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll {
				return responsePoll{done: true, err: addErr}
			},
			want: aeron.NotConnected,
		},
		{
			name: "vanished add",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll { return responsePoll{err: addErr} },
			want: aeron.NotConnected,
		},
		{
			name: "nil publication from a done poll",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll { return responsePoll{done: true} },
			want: aeron.NotConnected,
		},
		{
			name: "publication not connected",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithLimit(t, 0, false)
				return responsePoll{publication: pub, done: true}
			},
			want: aeron.NotConnected,
		},
		{
			name: "back-pressured publication",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
				return responsePoll{publication: pub, done: true}
			},
			want: aeron.BackPressured,
		},
		{
			name: "oversize",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				return responsePoll{publication: pub, done: true}
			},
			oversize: true,
			want:     aeron.UnsupportedMessageLength,
		},
		{
			name: "closed publication",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				return responsePoll{publication: pub, done: true}
			},
			closePublication: true,
			want:             aeron.Closed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adder := newFakeResponseAdder()
			var configured ResponsePublicationAdder = adder
			if tc.configure != nil {
				configured = tc.configure(adder)
			}
			log, _ := bindSessionAgent(t, configured, tc.role)
			log.open(7, 1)
			if tc.poll != nil {
				adder.next(1, tc.poll(t))
				log.agent.DoWork(0)
			}
			state := log.agent.sessions[7]
			if tc.closePublication {
				require.NoError(t, state.publication.Close())
			}
			payload := []byte("m")
			if tc.oversize {
				headerLength := codecs.SessionMessageHeaderEncodedLen()
				payload = make([]byte, state.publication.MaxMessageLength()-headerLength+1)
			}

			session, ok := log.agent.ClientSession(7)
			require.True(t, ok)
			require.Equal(t, tc.want, session.Offer(payload))
			require.Equal(t, tc.wantPending, log.agent.PendingResponsePublicationAdds())
			require.Len(t, adder.inFlight, tc.wantPending)
			require.NoError(t, log.agent.ServiceFailure())
			require.True(t, log.agent.HasSession(7))
		})
	}
}

func TestAgent_ClientSession_connectIdempotent(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent
	log.open(7, 1)
	session := agent.sessions[7]
	require.Len(t, adder.offers, 1)

	// An add in flight starts no second add.
	agent.connectClientSession(session)
	require.Len(t, adder.offers, 1)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())

	// A held publication starts no add.
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	require.Equal(t, 1, agent.DoWork(0))
	agent.connectClientSession(session)
	require.Len(t, adder.offers, 1)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	require.Same(t, pub, session.publication)
}

func TestAgent_ClientSessions_zeroAllocs(t *testing.T) {
	log, _ := bindSessionAgent(t, nil, codecs.RoleFollower)
	for _, id := range []int64{3, 1, 2} {
		log.open(id, 1)
	}
	agent := log.agent

	var sequence iter.Seq[ClientSession]
	testutil.AssertZeroAllocs(t, "ClientSessions", 100, func() {
		sequence = agent.ClientSessions()
	})

	walked := 0
	yield := func(ClientSession) bool {
		walked++
		return true
	}
	testutil.AssertZeroAllocs(t, "ClientSessions stored yield walk", 100, func() {
		sequence(yield)
	})
	require.Positive(t, walked)
}

func TestAgent_ClientSession_offerZeroAllocs(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	log.open(7, 1)
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	log.agent.DoWork(0)
	session, ok := log.agent.ClientSession(7)
	require.True(t, ok)

	payload := []byte("zero-alloc")
	require.Positive(t, session.Offer(payload))
	testutil.AssertZeroAllocs(t, "ClientSession.Offer", 100, func() {
		if session.Offer(payload) <= 0 {
			t.Fatal("offer rejected")
		}
	})
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
	})
}

func writeClusterAction(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	fields codecs.ClusterActionRequestFields,
) int {
	tb.Helper()
	payload := encodeClusterAction(tb, fields)
	span := writeAgentFrame(tb, image, 0, payload)
	*committed = int64(span)
	return span
}

func writeAlignedAction(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
	action clusterwire.ClusterAction,
	flags int32,
) int {
	tb.Helper()
	span := clusterActionFrameSpan()
	got := writeClusterAction(tb, image, committed, codecs.ClusterActionRequestFields{
		LeadershipTermID: 1,
		LogPosition:      int64(span),
		Timestamp:        timestamp,
		Action:           action,
		Flags:            flags,
	})
	require.Equal(tb, span, got, "encoded logPosition vs frame span")
	return span
}

func writeAlignedSnapshot(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
) int {
	tb.Helper()
	return writeAlignedAction(tb, image, committed, timestamp, clusterwire.ClusterAction_SNAPSHOT, 0)
}

// writeTermEventAndSnapshot writes a NewLeadershipTermEvent with NANOS at
// position 0, then a SNAPSHOT action. Returns the action frame end.
func writeTermEventAndSnapshot(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
) int64 {
	tb.Helper()
	log := &takeLog{image: image, committed: committed}
	log.termEvent(tb, clusterwire.ClusterTimeUnit_NANOS, 0)
	return log.snapshot(tb, timestamp)
}

func TestAgent_ClusterAction_snapshotHook(t *testing.T) {
	svc := &snapshotRecordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 555
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, ackKindSnapshot, agent.coordinationAcks[0].kind)
	require.Equal(t, end, agent.coordinationAcks[0].logPosition)

	// Tick 2: the hook runs on the fake publication. The take waits for STOP.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Len(t, svc.pubs, 1, "hook calls")
	require.Same(t, fake.LastPublication(), svc.pubs[0])
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack before STOP")

	// Tick 3: STOP resolves the take.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  fake.RecordingID(),
		ServiceID:   1,
	}, acks[0])
	require.NotEqual(t, snapshotTakeFakePreProofID, acks[0].RelevantID)
	require.Len(t, svc.pubs, 1, "hook calls after the ack")
}

func TestAgent_ClusterAction_noHandlerAcks(t *testing.T) {
	svc := &recordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 444
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.order, "callbacks")
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")

	// Tick 2: the rows record.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack before STOP")

	// Tick 3: STOP resolves the take.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  fake.RecordingID(),
		ServiceID:   1,
	}, acks[0])
}

func TestAgent_ClusterAction_minimalWrite(t *testing.T) {
	body := []byte("snap-bytes")
	svc := &snapshotRecordingService{write: body}
	agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	end := writeTermEventAndSnapshot(t, image, committed, 1)
	agent.DoWork(0)
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Len(t, svc.pubs, 1, "hook calls")

	payloads, _ := snapshotTermPayloads(t, fake.LastPublication())
	begin, sessions, _, rest := decodeSnapshotRows(t, payloads)
	require.Equal(t, end, begin.LogPosition)
	require.Empty(t, sessions)
	require.Equal(t, [][]byte{body}, rest)
}

func TestAgent_ClusterAction_hookError(t *testing.T) {
	hookErr := errors.New("snap boom")
	svc := &snapshotRecordingService{hookErr: hookErr}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 321
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack on consume tick")

	// Tick 2: the hook fails. The -1 ack sends, then the mark follows.
	agent.DoWork(0)
	require.Len(t, svc.pubs, 1, "hook calls")
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])
	require.ErrorIs(t, agent.ServiceFailure(), errTakeSnapshot)
	require.ErrorIs(t, agent.ServiceFailure(), hookErr)
	require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks after send")

	// Tick 3: the mark freezes the duty cycle.
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, end, agent.adapter.Position(), "poll position later tick")
	require.Len(t, decodeServiceAcksFromPub(t, pub), 1, "later tick acks")
	require.Len(t, svc.pubs, 1, "later tick hook calls")
}

func TestAgent_ClusterAction_hookErrorTerminalAck(t *testing.T) {
	hookErr := errors.New("snap boom")
	svc := &snapshotRecordingService{hookErr: hookErr}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	InstallSnapshotTakeFakeForTest(t, agent)
	require.NoError(t, pub.Close())

	end := writeTermEventAndSnapshot(t, image, committed, 321)

	// Tick 1: no mark, one unready entry.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")

	// Tick 2: the hook fails. The terminal code marks the take cause first,
	// with no send.
	agent.DoWork(0)
	require.Len(t, svc.pubs, 1, "hook calls")
	require.ErrorIs(t, agent.ServiceFailure(), errTakeSnapshot)
	require.ErrorIs(t, agent.ServiceFailure(), hookErr)
	require.NotErrorIs(t, agent.ServiceFailure(), errAckPublish)
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, int64(0), agent.NextAckID())

	// Tick 3: the mark freezes the duty cycle.
	require.Equal(t, 0, agent.DoWork(0))
}

func TestAgent_ClusterAction_hookErrorRetriableAck(t *testing.T) {
	t.Run("retriableDefersMark", func(t *testing.T) {
		hookErr := errors.New("snap boom")
		svc := &snapshotRecordingService{hookErr: hookErr}
		// Limit 0 + disconnected: Offer returns NotConnected.
		proxy := boundModuleProxyWithLimit(t, 0, false)
		agent, image, committed := bindImage(t, AgentConfig{
			ServiceID:   1,
			Service:     svc,
			ModuleProxy: proxy,
		}, 0)
		InstallSnapshotTakeFakeForTest(t, agent)

		const timestamp int64 = 321
		end := writeTermEventAndSnapshot(t, image, committed, timestamp)

		// Consume tick, hook tick, then retriable drain ticks: the entry
		// stays, no mark.
		for tick := range 4 {
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure(), "tick %d", tick)
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "tick %d pending acks", tick)
			require.Equal(t, int64(0), agent.NextAckID(), "tick %d ack id", tick)
		}
		require.Len(t, svc.pubs, 1, "hook calls")
		require.Equal(t, end, agent.adapter.Position(), "poll position")

		// Connected publication: the -1 ack sends, then the mark follows.
		connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
		proxy.SetPublication(connected)
		require.Equal(t, 1, agent.DoWork(0))
		acks := decodeServiceAcksFromPub(t, connected)
		require.Len(t, acks, 1)
		require.Equal(t, codecs.ServiceAckFields{
			LogPosition: end,
			Timestamp:   timestamp,
			AckID:       0,
			RelevantID:  nullValue,
			ServiceID:   1,
		}, acks[0])
		require.ErrorIs(t, agent.ServiceFailure(), errTakeSnapshot)
		require.ErrorIs(t, agent.ServiceFailure(), hookErr)
		require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks after send")
	})

	t.Run("laterDispatchFailureKeepsCause", func(t *testing.T) {
		svc := &snapshotRecordingService{hookErr: errors.New("snap boom")}
		proxy := boundModuleProxyWithLimit(t, 0, false)
		agent, image, committed := bindImage(t, AgentConfig{
			ServiceID:   1,
			Service:     svc,
			ModuleProxy: proxy,
		}, 0)
		InstallSnapshotTakeFakeForTest(t, agent)

		end := writeTermEventAndSnapshot(t, image, committed, 321)
		// Second SNAPSHOT frame: its LogPosition does not match its frame end.
		payload := encodeClusterAction(t, snapshotActionFields(1, 322))
		second := writeAgentFrame(t, image, int(end), payload)
		*committed = end + int64(second)

		// Tick 1: the take holds the log. The second frame stays unread.
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.Empty(t, svc.pubs, "hook calls")
		require.Equal(t, end, agent.adapter.Position(), "poll position after break")

		// Tick 2: the hook fails. NotConnected holds the entry. The log poll
		// resumes, so the second frame marks the position mismatch.
		agent.DoWork(0)
		require.Len(t, svc.pubs, 1, "hook calls")
		require.ErrorIs(t, agent.ServiceFailure(), errActionPositionMismatch)

		// A later bind sends no -1 ack.
		connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
		proxy.SetPublication(connected)
		require.Equal(t, 0, agent.DoWork(0))
		require.Empty(t, decodeServiceAcksFromPub(t, connected), "acks after bind")
		require.ErrorIs(t, agent.ServiceFailure(), errActionPositionMismatch)
		require.NotErrorIs(t, agent.ServiceFailure(), errTakeSnapshot)
		require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
		require.Len(t, svc.pubs, 1, "hook calls after bind")
	})
}

func TestAgent_ClusterAction_hookErrorNoModuleProxy(t *testing.T) {
	tests := []struct {
		name  string
		proxy *ConsensusModuleProxy
	}{
		{name: "nilProxy"},
		{name: "unboundProxy", proxy: NewConsensusModuleProxy()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hookErr := errors.New("snap boom")
			svc := &snapshotRecordingService{hookErr: hookErr}
			agent, image, committed := bindImage(t, AgentConfig{
				ServiceID:   1,
				Service:     svc,
				ModuleProxy: tt.proxy,
			}, 0)
			fake := InstallSnapshotTakeFakeForTest(t, agent)

			end := writeTermEventAndSnapshot(t, image, committed, 321)

			// Tick 1: the action stores the take.
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure())
			require.Equal(t, end, agent.adapter.Position(), "poll position after break")

			// Tick 2: the hook fails. No sender, so the take cause marks at
			// once.
			agent.DoWork(0)
			require.ErrorIs(t, agent.ServiceFailure(), errTakeSnapshot)
			require.ErrorIs(t, agent.ServiceFailure(), hookErr)
			require.Len(t, svc.pubs, 1, "hook calls")
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
			require.True(t, fake.LastPublication().IsClosed(), "publication closed")
			require.Nil(t, fake.handler, "session handler")

			// Tick 3: the mark freezes the duty cycle.
			require.Equal(t, 0, agent.DoWork(0))

			if tt.proxy == nil {
				return
			}
			// A later bind sends no -1 ack.
			pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
			tt.proxy.SetPublication(pub)
			require.Equal(t, 0, agent.DoWork(0))
			require.Empty(t, decodeServiceAcksFromPub(t, pub), "acks after bind")
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks after bind")
		})
	}
}

func TestAgent_ClusterAction_noArchive(t *testing.T) {
	svc := &snapshotRecordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)

	const timestamp int64 = 88
	span := writeAlignedSnapshot(t, image, committed, timestamp)

	// Tick 1: no archive fails the take on the action.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, int64(span), agent.adapter.Position(), "poll position after break")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 1, agent.ReadyCoordinationAcks(), "ready acks")

	// Tick 2: the -1 ack sends, then the mark follows.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: int64(span),
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])
	require.ErrorIs(t, agent.ServiceFailure(), errSnapshotNoArchive)
	require.Equal(t, nullValue, agent.LastSnapshotRecordingID())
}

func TestAgent_ClusterAction_consumedWithoutEntry(t *testing.T) {
	tests := []struct {
		name   string
		action clusterwire.ClusterAction
		flags  int32
	}{
		{name: "SUSPEND", action: clusterwire.ClusterAction_SUSPEND},
		{name: "RESUME", action: clusterwire.ClusterAction_RESUME},
		{name: "flags1", action: clusterwire.ClusterAction_SNAPSHOT, flags: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &snapshotRecordingService{}
			agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
			const timestamp int64 = 77
			span := writeAlignedAction(t, image, committed, timestamp, tt.action, tt.flags)
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure())
			require.Empty(t, svc.pubs, "hook calls")
			require.Equal(t, int64(span), agent.LogPosition())
			require.Equal(t, timestamp, agent.ClusterTime())
			require.Equal(t, int64(span), agent.adapter.Position(), "poll position after continue")
			require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
		})
	}
}

func TestAgent_ClusterAction_unknownAction(t *testing.T) {
	tests := []struct {
		name   string
		action clusterwire.ClusterAction
	}{
		{name: "nullValue", action: clusterwire.ClusterAction_NULL_VALUE},
		{name: "outOfRange", action: clusterwire.ClusterAction(99)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &snapshotRecordingService{}
			agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
			const timestamp int64 = 9
			span := writeAlignedAction(t, image, committed, timestamp, tt.action, 0)
			join := image.Position()
			agent.DoWork(0)
			require.ErrorIs(t, agent.ServiceFailure(), errUnknownClusterAction)
			require.Empty(t, svc.pubs, "hook calls")
			require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
			require.Equal(t, int64(span), agent.LogPosition())
			require.Equal(t, timestamp, agent.ClusterTime())
			require.Equal(t, join, agent.adapter.Position(), "poll position after abort")
		})
	}
}

func TestAgent_ClusterAction_positionMismatch(t *testing.T) {
	svc := &snapshotRecordingService{}
	agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
	const timestamp int64 = 12
	span := writeClusterAction(t, image, committed, snapshotActionFields(1, timestamp))
	join := image.Position()

	agent.DoWork(0)
	require.ErrorIs(t, agent.ServiceFailure(), errActionPositionMismatch)
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, int64(span), agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, join, agent.adapter.Position(), "poll position after abort")
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

// Asserts ScheduleTimer and CancelTimer failure mapping. Unbound and retriable
// paths leave ServiceFailure nil. Terminal close marks errTimerPublish once. A
// second call keeps that failure.
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
	tests := []struct {
		name        string
		setup       func(*testing.T) *ConsensusModuleProxy
		wantFailure error
	}{
		{
			name:  "nilProxy",
			setup: func(*testing.T) *ConsensusModuleProxy { return nil },
		},
		{
			name:  "unboundProxy",
			setup: func(*testing.T) *ConsensusModuleProxy { return NewConsensusModuleProxy() },
		},
		{
			name: "retriableNotConnected",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, false)
			},
		},
		{
			name: "retriableBackPressured",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, true)
			},
		},
		{
			name: "terminalClosed",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				proxy, pub := boundModuleProxy(t)
				require.NoError(t, pub.Close())
				return proxy
			},
			wantFailure: errTimerPublish,
		},
	}

	for _, tc := range tests {
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

func TestAgent_Unbind_imageClosedWhileBound(t *testing.T) {
	svc := &recordingService{}
	agent, image, _ := bindAgent(t, svc, 0)

	// defaultJoinEvent leaves maxLogPosition at MaxInt64. Unbind uses IsClosed.
	aeron.ImageInternalCloseForHooks(image)
	require.Equal(t, 0, agent.DoWork(0))
	require.False(t, agent.IsLogBound())
	require.Equal(t, codecs.RoleFollower, agent.Role())
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
}

// countingArchiveControl counts Poll calls. Each Poll reports pollWork.
type countingArchiveControl struct {
	polls    int
	pollWork int
}

func (c *countingArchiveControl) Poll() int {
	c.polls++
	return c.pollWork
}

func (*countingArchiveControl) ControlSessionID() int64 { return 0 }

func (*countingArchiveControl) Config() archive.Config { return archive.Config{} }

func (*countingArchiveControl) SetRecordingSignalHandler(archive.RecordingSignalHandler) {}

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
	require.ErrorIs(t, agent.ServiceFailure(), errLogDispatchSchema)
	require.Equal(t, 0, agent.DoWork(0))
}
