//go:build testhooks

package consensus

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func addLogJoinImage(
	t *testing.T,
	sub *aeron.Subscription,
	sessionID int32,
	correlationID int64,
	joinPosition int64,
) *aeron.Image {
	t.Helper()

	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		sessionID,
		correlationID,
		joinPosition,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

// TestSubscriptionLogJoinSource_orderIndependence selects each session's image
// and join params from two subscriptions holding the same images in opposite
// order.
func TestSubscriptionLogJoinSource_orderIndependence(t *testing.T) {
	t.Parallel()

	const (
		sessionA int32 = 10
		sessionB int32 = 20
		sessionC int32 = 30
		joinA    int64 = 0
		joinB    int64 = 96
		joinC    int64 = 192
	)

	forward := aeron.NewTestSubscriptionForHooks(t)
	forwardA := addLogJoinImage(t, forward, sessionA, 200, joinA)
	forwardB := addLogJoinImage(t, forward, sessionB, 201, joinB)
	forwardC := addLogJoinImage(t, forward, sessionC, 202, joinC)

	reverse := aeron.NewTestSubscriptionForHooks(t)
	reverseC := addLogJoinImage(t, reverse, sessionC, 300, joinC)
	reverseB := addLogJoinImage(t, reverse, sessionB, 301, joinB)
	reverseA := addLogJoinImage(t, reverse, sessionA, 302, joinA)

	srcForward := NewSubscriptionLogJoinSource(forward)
	srcReverse := NewSubscriptionLogJoinSource(reverse)

	for _, row := range []struct {
		sessionID    int32
		joinPosition int64
		wantForward  *aeron.Image
		wantReverse  *aeron.Image
	}{
		{sessionA, joinA, forwardA, reverseA},
		{sessionB, joinB, forwardB, reverseB},
		{sessionC, joinC, forwardC, reverseC},
	} {
		want := FollowerLogJoinParams{
			LogChannel:      forward.Channel(),
			LogStreamID:     forward.StreamID(),
			LogSessionID:    row.sessionID,
			LogJoinPosition: row.joinPosition,
		}
		paramsF, okF := srcForward.Ready(row.sessionID)
		paramsR, okR := srcReverse.Ready(row.sessionID)
		require.True(t, okF)
		require.True(t, okR)
		require.Equal(t, want, paramsF)
		require.Equal(t, want, paramsR)

		require.Same(t, row.wantForward, srcForward.Image(row.sessionID))
		require.Same(t, row.wantReverse, srcReverse.Image(row.sessionID))
	}
}

func TestSubscriptionLogJoinSource_channelStreamIDFromSubscription(t *testing.T) {
	t.Parallel()

	const (
		regID    int64  = 77
		streamID int32  = 100
		channel  string = "aeron:udp?endpoint=127.0.0.1:0"
		session  int32  = 42
		joinPos  int64  = 4096
	)

	sub := aeron.NewSubscriptionForHooks(regID, streamID, channel)
	addLogJoinImage(t, sub, session, 400, joinPos)

	s := NewSubscriptionLogJoinSource(sub)
	params, ok := s.Ready(session)
	require.True(t, ok)
	require.Equal(t, FollowerLogJoinParams{
		LogChannel:      channel,
		LogStreamID:     streamID,
		LogSessionID:    session,
		LogJoinPosition: joinPos,
	}, params)
}

func TestSubscriptionLogJoinSource_channelStreamIDFromFake(t *testing.T) {
	t.Parallel()

	const (
		session  int32  = 55
		joinPos  int64  = 128
		channel  string = "aeron:fake-log"
		streamID int32  = 999
	)

	// Image is built against a real subscription for construction only; the
	// Fake SubscriptionImages reports its own channel and stream id on Ready.
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		session,
		500,
		joinPos,
	)

	fake := &fakeSubscriptionImages{
		channel:  channel,
		streamID: streamID,
		images:   []*aeron.Image{image},
	}
	s := NewSubscriptionLogJoinSource(fake)
	params, ok := s.Ready(session)
	require.True(t, ok)
	require.Equal(t, FollowerLogJoinParams{
		LogChannel:      channel,
		LogStreamID:     streamID,
		LogSessionID:    session,
		LogJoinPosition: joinPos,
	}, params)
	require.Same(t, image, s.Image(session))
}

func TestSubscriptionLogJoinSource_notReadyImageStates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		session int32
		source  func(t *testing.T) *SubscriptionLogJoinSource
	}{
		{"unknown session", 999, func(t *testing.T) *SubscriptionLogJoinSource {
			t.Helper()
			sub := aeron.NewTestSubscriptionForHooks(t)
			addLogJoinImage(t, sub, 10, 600, 0)
			return NewSubscriptionLogJoinSource(sub)
		}},
		// Asserts the outcome, not the walk's !IsClosed guard: Subscription.ImageAt
		// already filters a closed image to ok false.
		{"closed image real subscription", 10, func(t *testing.T) *SubscriptionLogJoinSource {
			t.Helper()
			sub := aeron.NewTestSubscriptionForHooks(t)
			closed := addLogJoinImage(t, sub, 10, 700, 0)
			aeron.ImageInternalCloseForHooks(closed)
			return NewSubscriptionLogJoinSource(sub)
		}},
		// Only row that reaches the walk's !IsClosed guard: fake ImageAt reports
		// ok true for a closed image, so the consumer filter decides.
		{"fake closed image", 10, func(t *testing.T) *SubscriptionLogJoinSource {
			t.Helper()
			sub := aeron.NewTestSubscriptionForHooks(t)
			closed := aeron.NewTestImageForSubscriptionHooksWithSessionID(
				t,
				sub,
				10,
				900,
				0,
			)
			aeron.ImageInternalCloseForHooks(closed)
			require.True(t, closed.IsClosed())
			return NewSubscriptionLogJoinSource(&fakeSubscriptionImages{
				channel:  "aeron:ipc",
				streamID: 100,
				images:   []*aeron.Image{closed},
			})
		}},
		// Subscription.ImageAt returns ok false when the subscription is closed.
		// Close with no conductor only marks closed (no driver needed).
		{"closed subscription", 10, func(t *testing.T) *SubscriptionLogJoinSource {
			t.Helper()
			sub := aeron.NewTestSubscriptionForHooks(t)
			addLogJoinImage(t, sub, 10, 1000, 0)
			require.NoError(t, sub.Close())
			require.True(t, sub.IsClosed())
			return NewSubscriptionLogJoinSource(sub)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := tc.source(t)
			params, ok := s.Ready(tc.session)
			require.False(t, ok)
			require.Equal(t, FollowerLogJoinParams{}, params)
			require.Nil(t, s.Image(tc.session))
		})
	}
}

func TestSubscriptionLogJoinSource_nilSlotRealSubscription(t *testing.T) {
	t.Parallel()

	// Asserts tolerance of a nil slot, not the walk's image != nil guard:
	// Subscription.ImageAt already filters a nil slot to ok false.
	sub := aeron.NewTestSubscriptionForHooks(t)
	aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, 0)
	match := addLogJoinImage(t, sub, 10, 800, 64)

	s := NewSubscriptionLogJoinSource(sub)
	params, ok := s.Ready(10)
	require.True(t, ok)
	require.Equal(t, FollowerLogJoinParams{
		LogChannel:      sub.Channel(),
		LogStreamID:     sub.StreamID(),
		LogSessionID:    10,
		LogJoinPosition: 64,
	}, params)
	require.Same(t, match, s.Image(10))
}

func TestSubscriptionLogJoinSource_fakeClosedThenOpenSameSession(t *testing.T) {
	t.Parallel()

	// First-match skips a closed image and selects a later open image for the
	// same session (fake ImageAt ok true so !IsClosed is reached on the closed slot).
	const (
		session    int32  = 10
		closedJoin int64  = 0
		openJoin   int64  = 96
		channel    string = "aeron:ipc"
		streamID   int32  = 100
	)

	sub := aeron.NewTestSubscriptionForHooks(t)
	closed := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		session,
		910,
		closedJoin,
	)
	aeron.ImageInternalCloseForHooks(closed)
	open := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		session,
		911,
		openJoin,
	)

	fake := &fakeSubscriptionImages{
		channel:  channel,
		streamID: streamID,
		images:   []*aeron.Image{closed, open},
	}
	s := NewSubscriptionLogJoinSource(fake)
	params, ok := s.Ready(session)
	require.True(t, ok)
	require.Equal(t, FollowerLogJoinParams{
		LogChannel:      channel,
		LogStreamID:     streamID,
		LogSessionID:    session,
		LogJoinPosition: openJoin,
	}, params)
	require.Same(t, open, s.Image(session))
}

func agentWithLogJoin(
	t *testing.T,
	sessionID int32,
	joinPosition int64,
	cfg AgentConfig,
) (*Agent, *aeron.Image, *LogAdapter) {
	t.Helper()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := addLogJoinImage(t, sub, sessionID, 2000, joinPosition)
	src := NewSubscriptionLogJoinSource(sub)
	adapter := NewLogAdapter(0)
	cfg.LogJoinSource = src
	cfg.LogAdapter = adapter
	return NewAgent(cfg), image, adapter
}

func TestAgentJoinLogAsFollower_bindsImage(t *testing.T) {
	t.Parallel()

	const (
		session int32 = 10
		joinPos int64 = 96
	)
	a, image, adapter := agentWithLogJoin(t, session, joinPos, AgentConfig{})

	params, ready := a.FollowerLogJoinReady(session)
	require.True(t, ready)
	require.Equal(t, session, params.LogSessionID)
	require.Equal(t, joinPos, params.LogJoinPosition)

	joined, err := a.JoinLogAsFollower(params, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, image, adapter.Image())
	require.False(t, adapter.IsImageClosed())
	require.Equal(t, joinPos, adapter.Position())
}

func TestAgentJoinLogAsFollower_setsFollowerKeepsLeaderAndTerm(t *testing.T) {
	t.Parallel()

	const (
		leaderID int32 = 2
		termID   int64 = 5
	)
	a, _, _ := agentWithLogJoin(t, 10, 0, AgentConfig{
		Role:             RoleCandidate,
		LeaderMemberID:   leaderID,
		LeadershipTermID: termID,
	})
	require.Equal(t, RoleCandidate, a.Role())

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 10}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, leaderID, a.LeaderMemberID())
	require.Equal(t, termID, a.LeadershipTermID())
}

func TestAgentJoinLogAsFollower_idempotent(t *testing.T) {
	t.Parallel()

	a, image, adapter := agentWithLogJoin(t, 10, 0, AgentConfig{Role: RoleCandidate})
	params := FollowerLogJoinParams{LogSessionID: 10}

	joined, err := a.JoinLogAsFollower(params, 1)
	require.NoError(t, err)
	require.True(t, joined)
	joined, err = a.JoinLogAsFollower(params, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, image, adapter.Image())
	require.Equal(t, RoleFollower, a.Role())
}

func TestAgentJoinLogAsFollower_rejoinReplaces(t *testing.T) {
	t.Parallel()

	sub := aeron.NewTestSubscriptionForHooks(t)
	imgA := addLogJoinImage(t, sub, 10, 2100, 0)
	imgB := addLogJoinImage(t, sub, 20, 2101, 96)
	src := NewSubscriptionLogJoinSource(sub)
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 10}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, imgA, adapter.Image())

	joined, err = a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 20}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, imgB, adapter.Image())
}

func TestAgentJoinLogAsFollower_afterImageClosed(t *testing.T) {
	t.Parallel()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := addLogJoinImage(t, sub, 10, 2200, 0)
	src := NewSubscriptionLogJoinSource(sub)
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})

	params, ready := a.FollowerLogJoinReady(10)
	require.True(t, ready)
	require.Equal(t, int32(10), params.LogSessionID)

	// Image closed between readiness and join: re-read fails closed.
	aeron.ImageInternalCloseForHooks(image)
	joined, err := a.JoinLogAsFollower(params, 1)
	require.NoError(t, err)
	require.False(t, joined)
	require.Nil(t, adapter.Image())
}

func TestAgentJoinLogAsFollower_wiredControlStartsRemote(t *testing.T) {
	t.Parallel()

	const (
		session int32 = 10
		joinPos int64 = 96
	)
	control := &fakeLogRecordingControl{nextRecordingID: 88}
	a, image, adapter := agentWithLogJoin(t, session, joinPos, AgentConfig{})
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: session}, 1)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, image, adapter.Image())
	require.True(t, a.IsRecordingActive())
	require.Equal(t, int64(88), a.LocalLogRecordingID())
	require.Len(t, control.startCalls, 1)
	require.Equal(t, archivewire.SourceLocation_REMOTE, control.startCalls[0].sourceLocation)
	require.Equal(t, session, control.startCalls[0].logSessionID)
}

func TestAgentJoinLogAsFollower_wiredControlStartError(t *testing.T) {
	t.Parallel()

	const session int32 = 10
	startErr := errors.New("remote start failed")
	control := &fakeLogRecordingControl{startErr: startErr}
	a, _, adapter := agentWithLogJoin(t, session, 0, AgentConfig{})
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: session}, 1)
	require.ErrorIs(t, err, startErr)
	require.False(t, joined)
	require.Nil(t, adapter.Image())
	require.False(t, a.IsRecordingActive())
}

func TestAgentJoinLogAsFollower_imageVanishesAfterStart(t *testing.T) {
	t.Parallel()

	// Image closed during start: no bind, stop recording, not-joined.
	const session int32 = 10
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := addLogJoinImage(t, sub, session, 2300, 0)
	src := NewSubscriptionLogJoinSource(sub)
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		Role:          RoleCandidate,
		LogJoinSource: src,
		LogAdapter:    adapter,
	})
	control := &fakeLogRecordingControl{
		nextRecordingID:  88,
		nextStopPosition: 50,
		onStart: func() {
			aeron.ImageInternalCloseForHooks(image)
		},
	}
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: session}, 1)
	require.NoError(t, err)
	require.False(t, joined)
	require.Nil(t, adapter.Image())
	require.False(t, a.IsRecordingActive())
	require.Len(t, control.startCalls, 1)
	require.Equal(t, 1, control.stopCalls)
	require.Equal(t, RoleCandidate, a.Role())
}

func TestAgentJoinLogAsFollower_imageVanishesAfterStartStopError(t *testing.T) {
	t.Parallel()

	// Asserts stop failure after a vanished image keeps the control active for
	// prepare drain.
	const session int32 = 10
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := addLogJoinImage(t, sub, session, 2301, 0)
	src := NewSubscriptionLogJoinSource(sub)
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})
	stopErr := errors.New("stop after vanish failed")
	control := &fakeLogRecordingControl{
		nextRecordingID: 88,
		stopErr:         stopErr,
		onStart: func() {
			aeron.ImageInternalCloseForHooks(image)
		},
	}
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: session}, 1)
	require.ErrorIs(t, err, stopErr)
	require.False(t, joined)
	require.Nil(t, adapter.Image())
	require.True(t, a.IsRecordingActive())
	require.Len(t, control.startCalls, 1)
	require.Equal(t, 1, control.stopCalls)
}

func TestAgent_logRecording_wiredControlLeaderJoin(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{nextRecordingID: 77}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	require.True(t, a.IsRecordingActive())
	require.Equal(t, int64(77), a.LocalLogRecordingID())
	require.Len(t, control.startCalls, 1)
	require.Equal(t, archivewire.SourceLocation_LOCAL, control.startCalls[0].sourceLocation)
	require.Equal(t, pub.SessionID(), control.startCalls[0].logSessionID)
}

func TestAgent_logRecording_wiredControlLeaderJoinError(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	startErr := errors.New("start refused")
	control := &fakeLogRecordingControl{startErr: startErr}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	err := a.JoinLogAsLeader(1, 10, pub.SessionID(), false)
	require.ErrorIs(t, err, startErr)
	require.False(t, a.IsRecordingActive())
	require.Equal(t, int64(nullValue), a.LocalLogRecordingID())
	// Role write precedes the start.
	require.Equal(t, RoleLeader, a.Role())
}

func TestAgent_logRecording_secondJoinWhileActive(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{nextRecordingID: 77}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	require.NoError(t, a.JoinLogAsLeader(2, 20, pub.SessionID(), false))
	require.Len(t, control.startCalls, 1)
	require.True(t, a.IsRecordingActive())
}

func TestAgent_logRecording_transitionWithRecording(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{
		nextRecordingID:  77,
		nextStopPosition: 5000,
	}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	got, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	require.Equal(t, int64(5000), got)
	require.False(t, a.IsRecordingActive())
	require.Equal(t, int64(77), a.LocalLogRecordingID())
	require.Equal(t, 1, control.stopCalls)
}

func TestAgent_logRecording_transitionStopError(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	stopErr := errors.New("stop failed")
	control := &fakeLogRecordingControl{
		nextRecordingID: 77,
		stopErr:         stopErr,
	}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	got, err := a.PrepareForNewLeadership(1234, 1)
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, recordinglog.NullPosition, got)
	require.True(t, a.IsRecordingActive())
}

func TestAgent_logRecording_repeatedTransition(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{
		nextRecordingID:  77,
		nextStopPosition: 5000,
	}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	_, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	_, err = a.PrepareForNewLeadership(1234, 2)
	require.NoError(t, err)
	// Second prepare still offers stop (idempotent on the real control).
	require.Equal(t, 2, control.stopCalls)
}

func TestAgent_logRecording_stopKeepsRecordingID(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{
		nextRecordingID:  77,
		nextStopPosition: 5000,
	}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	_, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	require.NotEqual(t, int64(-1), a.LocalLogRecordingID())
}

func TestAgent_logRecording_leaderJoinNegativeSessionID(t *testing.T) {
	t.Parallel()

	// Negative session ids are legitimate; refusal is structural, not a sign test.
	lp, _ := boundLogPublisher(t)
	control := &fakeLogRecordingControl{nextRecordingID: 77}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, -7, false))
	require.Len(t, control.startCalls, 1)
	require.Equal(t, int32(-7), control.startCalls[0].logSessionID)
	require.True(t, a.IsRecordingActive())
}

func TestAgent_SetLogRecordingID_whileActive(t *testing.T) {
	t.Parallel()

	lp, pub := boundLogPublisher(t)
	control := &fakeLogRecordingControl{nextRecordingID: 77}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)

	require.NoError(t, a.JoinLogAsLeader(1, 10, pub.SessionID(), false))
	a.SetLogRecordingID(4242)
	require.Equal(t, int64(77), a.LocalLogRecordingID())
	require.True(t, a.IsRecordingActive())
}
