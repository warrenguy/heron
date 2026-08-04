//go:build testhooks

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	testCatchupEndpoint       = "127.0.0.1:9105"
	testCatchupLogStreamID    = 101
	testCatchupLogSessionID   = 55
	testCatchupRecordingID    = 88
	testCatchupStartPosition  = 4096
	testCatchupCustomLogChan  = "aeron:udp?term-length=1m|mtu=8k|endpoint=239.255.12.1:9004"
	testCatchupDefaultLogChan = DefaultLogChannel + "|endpoint=239.255.12.1:9004"

	// testCatchupDefaultDest is the golden destination URI built from the default
	// template. Geometry matches DefaultLogChannel with the catchup endpoint.
	testCatchupDefaultDest = "aeron:udp?term-length=64k|endpoint=" + testCatchupEndpoint
)

var errTestArchiveRefused = errors.New("archive refused replay")

// Asserts catchup channels build on the configured log channel, not the
// package default. Term geometry must match the live log and the recording
// taken from that channel. A replay onto a shorter term is a different stream.
func TestCatchupControl_channelsFollowConfiguredLogChannel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		logChannel string
		want       string
	}{
		{
			name:       "default template",
			logChannel: testCatchupDefaultLogChan,
			want:       testCatchupDefaultDest,
		},
		{
			name:       "custom term length and mtu",
			logChannel: testCatchupCustomLogChan,
			want: "aeron:udp?term-length=1m|mtu=8k|endpoint=" +
				testCatchupEndpoint,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(
				t,
				tt.want,
				catchupDestinationURI(tt.logChannel, testCatchupEndpoint),
			)

			// The replay channel is the same template plus session scope and the
			// replay-only linger and eos params.
			replay := catchupReplayChannel(
				tt.logChannel,
				testCatchupEndpoint,
				testCatchupLogSessionID,
			)
			require.Equal(
				t,
				tt.want+"|session-id=55|linger=0|eos=false",
				replay,
			)
		})
	}
}

// Asserts a started replay is retained until stop. Without the retained id
// nothing stops the follow-mode replay. The replay outlives the catchup that
// asked for it.
func TestCatchupControl_stopDrainsStartedReplays(t *testing.T) {
	t.Parallel()

	control := &fakeReplayControl{sessionID: 11}
	adder := &fakeSubscriptionAdder{}
	c := newCatchupControl(
		context.Background(),
		control,
		adder,
		testReplayAgent(testCatchupRecordingID),
		nil,
		testCatchupLogStreamID,
		testCatchupDefaultLogChan,
	)

	first, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(11), first)

	control.sessionID = 12
	second, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		"127.0.0.1:9106",
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(12), second)

	require.NoError(t, c.StopCatchupReplays())
	require.Equal(t, []int64{11, 12}, control.stopCalls)

	// Idempotent: a second stop has nothing left to drain.
	require.NoError(t, c.StopCatchupReplays())
	require.Len(t, control.stopCalls, 2)
}

// Asserts a failed start is not retained. No replay exists to stop.
func TestCatchupControl_startFailureRetainsNothing(t *testing.T) {
	t.Parallel()

	control := &fakeReplayControl{startErr: errTestArchiveRefused}
	c := newCatchupControl(
		context.Background(),
		control,
		&fakeSubscriptionAdder{},
		testReplayAgent(testCatchupRecordingID),
		nil,
		testCatchupLogStreamID,
		testCatchupDefaultLogChan,
	)

	_, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.ErrorIs(t, err, errTestArchiveRefused)

	require.NoError(t, c.StopCatchupReplays())
	require.Empty(t, control.stopCalls)
}

// Asserts Remove restores the live join source and lets a later Ensure
// reopen. The catchup subscription must not stay preferred once catchup has
// completed.
func TestCatchupControl_removeRestoresLiveJoinSource(t *testing.T) {
	t.Parallel()

	agent := testReplayAgent(testCatchupRecordingID)
	liveJoin := consensus.NewSubscriptionLogJoinSource(
		aeron.NewTestSubscriptionForHooks(t),
	)
	agent.SetLogJoinSource(liveJoin)

	adder := &fakeSubscriptionAdder{result: aeron.NewTestSubscriptionForHooks(t)}
	c := newCatchupControl(
		context.Background(),
		&fakeReplayControl{},
		adder,
		agent,
		liveJoin,
		testCatchupLogStreamID,
		testCatchupDefaultLogChan,
	)

	want := testCatchupDefaultDest
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Len(t, adder.calls, 1)
	require.Equal(t, want, adder.calls[0].channel)
	require.Equal(t, int32(testCatchupLogStreamID), adder.calls[0].streamID)
	require.IsType(t, (*preferCatchupJoinSource)(nil), agent.LogJoinSource())

	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Same(t, liveJoin, agent.LogJoinSource())

	// Idempotent: a second remove neither reopens nor rebinds.
	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Same(t, liveJoin, agent.LogJoinSource())

	adder.result = aeron.NewTestSubscriptionForHooks(t)
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Len(t, adder.calls, 2)
	require.IsType(t, (*preferCatchupJoinSource)(nil), agent.LogJoinSource())
}
