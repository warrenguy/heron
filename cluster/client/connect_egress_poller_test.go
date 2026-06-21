//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

func TestNewConnectEgressPoller_normalizesFragmentLimit(t *testing.T) {
	cases := []struct {
		name  string
		limit int
	}{
		{name: "zero", limit: 0},
		{name: "negative", limit: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := NewConnectEgressPoller(nil, tc.limit)
			require.Equal(t, DefaultConnectEgressFragmentLimit, poller.FragmentLimit())
		})
	}
}

func TestConnectEgressPoller_Poll_zeroWhenNilSubscription(t *testing.T) {
	poller := newConnectEgressPollerForTest(nil)

	require.Equal(t, 0, poller.Poll())
	require.False(t, poller.IsPollComplete())
}

func writeTwoSequentialUnfragmentedWiresToTerm(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	first, second []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(t, termBuffer, first)
	appendUnfragmentedWireToTerm(t, termBuffer, second)
}

func TestConnectEgressPoller_Poll_challenge(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeChallengeWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Equal(t, int64(404), poller.CorrelationID())
	require.Nil(t, poller.EgressImage())
}

func TestConnectEgressPoller_Poll_sessionEventOK(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, EventCodeOK, poller.EventCode())
	require.Equal(t, int32(gensbe.SessionEventSbeTemplateID), poller.TemplateID())
	require.NotNil(t, poller.EgressImage())
}

func TestConnectEgressPoller_Poll_newLeader(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeNewLeaderWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, int32(gensbe.NewLeaderEventSbeTemplateID), poller.TemplateID())
	require.Equal(t, []byte("0=localhost:20110"), poller.Detail())
	require.NotNil(t, poller.EgressImage())
}

func TestConnectEgressPoller_Poll_sessionMessageHeader(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		[]byte("poll-header-payload"),
	)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, int32(gensbe.SessionMessageHeaderSbeTemplateID), poller.TemplateID())
	require.Zero(t, poller.ClusterSessionID())
	require.Zero(t, poller.LeadershipTermID())
	require.Nil(t, poller.EgressImage())
}

func TestConnectEgressPoller_Poll_unknownTemplateNotChallenged(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionKeepAliveWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsChallenged())
	require.False(t, poller.IsPollComplete())
}

func TestConnectEgressPoller_Poll_fragmentedSessionEvent(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.False(t, poller.IsPollComplete())

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())
	require.Equal(t, []byte("session-event-detail"), poller.Detail())
}

func TestConnectEgressPoller_Poll_resetCaptureClearsPriorCapture(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, int64(77), poller.CorrelationID())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())
	require.Equal(t, int64(88), poller.LeadershipTermID())
	require.Equal(t, int32(3), poller.LeaderMemberID())
	require.Equal(t, EventCodeOK, poller.EventCode())
	require.Equal(t, []byte("session-event-detail"), poller.Detail())
	require.NotNil(t, poller.EgressImage())

	require.Equal(t, 0, poller.Poll())
	require.False(t, poller.IsPollComplete())
	require.Zero(t, poller.TemplateID())
	require.Zero(t, poller.ClusterSessionID())
	require.Equal(t, conductor.NullCorrelationID, poller.CorrelationID())
	require.Zero(t, poller.LeadershipTermID())
	require.Zero(t, poller.LeaderMemberID())
	require.Zero(t, poller.LeaderHeartbeatTimeoutNs())
	require.Zero(t, poller.EventCode())
	require.Nil(t, poller.Detail())
	require.Nil(t, poller.EncodedChallenge())
	require.Nil(t, poller.EgressImage())
}

func TestConnectEgressPoller_Poll_resetsAfterComplete(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	sessionWire := encodeSessionEventWire(t, egressTestClusterSessionID)
	challengeWire := encodeChallengeWire(t, egressTestClusterSessionID)
	writeTwoSequentialUnfragmentedWiresToTerm(t, termBuffer, sessionWire, challengeWire)

	poller := newConnectEgressPollerForTest(sub)

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, int64(77), poller.CorrelationID())

	require.Equal(t, 1, poller.Poll())
	require.True(t, poller.IsChallenged())
	require.Equal(t, int64(404), poller.CorrelationID())
	require.Zero(t, poller.EventCode())
}

func TestConnectEgressPoller_Poll_fragmentLimitTwoCapturesFirstTerminalOnly(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	sessionWire := encodeSessionEventWire(t, egressTestClusterSessionID)
	challengeWire := encodeChallengeWire(t, egressTestClusterSessionID)
	writeTwoSequentialUnfragmentedWiresToTerm(t, termBuffer, sessionWire, challengeWire)

	poller := NewConnectEgressPoller(sub, 2)

	require.Equal(t, 2, poller.Poll())
	require.True(t, poller.IsPollComplete())
	require.False(t, poller.IsChallenged())
	require.Equal(t, int64(77), poller.CorrelationID())
	require.Equal(t, int32(gensbe.SessionEventSbeTemplateID), poller.TemplateID())
}
