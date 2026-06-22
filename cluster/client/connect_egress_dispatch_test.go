package client

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestHandleConnectEgressFragment_sessionEvent(t *testing.T) {
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragment(t, poller, wire)

	require.True(t, poller.IsPollComplete())
	require.Equal(t, int32(gensbe.SessionEventSbeTemplateID), poller.TemplateID())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())
	require.Equal(t, int64(77), poller.CorrelationID())
	require.Equal(t, int64(88), poller.LeadershipTermID())
	require.Equal(t, int32(3), poller.LeaderMemberID())
	require.Zero(t, poller.LeaderHeartbeatTimeoutNs())
	require.Equal(t, EventCodeOK, poller.EventCode())
	captured := poller.Detail()
	require.Equal(t, []byte("session-event-detail"), captured)

	wire[len(wire)-1] = 'X'
	require.Equal(t, []byte("session-event-detail"), captured)
}

func TestHandleConnectEgressFragment_sessionEvent_variants(t *testing.T) {
	const heartbeatNs int64 = 5_000_000_000

	cases := []struct {
		name   string
		encode func(t *testing.T) []byte
		assert func(t *testing.T, poller *ConnectEgressPoller)
	}{
		{
			name: "leaderHeartbeatTimeout",
			encode: func(t *testing.T) []byte {
				return encodeSessionEventWireWithLeaderHeartbeat(
					t,
					egressTestClusterSessionID,
					heartbeatNs,
				)
			},
			assert: func(t *testing.T, poller *ConnectEgressPoller) {
				require.Equal(t, heartbeatNs, poller.LeaderHeartbeatTimeoutNs())
			},
		},
		{
			name: "omittedHeartbeatV13",
			encode: func(t *testing.T) []byte {
				return encodeSessionEventWireOmittedHeartbeat(t, egressTestClusterSessionID)
			},
			assert: func(t *testing.T, poller *ConnectEgressPoller) {
				require.Zero(t, poller.LeaderHeartbeatTimeoutNs())
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := newConnectEgressPollerForTest(nil)

			invokeHandleConnectEgressFragment(t, poller, tc.encode(t))

			require.True(t, poller.IsPollComplete())
			tc.assert(t, poller)
		})
	}
}

func TestHandleConnectEgressFragment_challenge(t *testing.T) {
	wire := encodeChallengeWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragment(t, poller, wire)

	require.True(t, poller.IsChallenged())
	require.True(t, poller.IsPollComplete())
	require.Equal(t, int64(404), poller.CorrelationID())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())

	captured := poller.EncodedChallenge()
	require.Equal(t, []byte("challenge-bytes"), captured)

	wire[len(wire)-1] = 'X'
	require.Equal(t, []byte("challenge-bytes"), captured)
}

func TestHandleConnectEgressFragment_challenge_emptyPayload(t *testing.T) {
	wire := encodeChallengeWireEmpty(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragment(t, poller, wire)

	require.True(t, poller.IsChallenged())
	require.Nil(t, poller.EncodedChallenge())
}

func TestHandleConnectEgressFragment_newLeader(t *testing.T) {
	wire := encodeNewLeaderWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragment(t, poller, wire)

	require.True(t, poller.IsPollComplete())
	require.Equal(t, int32(gensbe.NewLeaderEventSbeTemplateID), poller.TemplateID())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())
	require.Equal(t, int64(200), poller.LeadershipTermID())
	require.Equal(t, int32(1), poller.LeaderMemberID())
	captured := poller.Detail()
	require.Equal(t, []byte("0=localhost:20110"), captured)

	wire[len(wire)-1] = 'X'
	require.Equal(t, []byte("0=localhost:20110"), captured)
}

func TestHandleConnectEgressFragment_sessionMessageHeader(t *testing.T) {
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		[]byte("ignored-app-payload"),
	)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragment(t, poller, wire)

	require.True(t, poller.IsPollComplete())
	require.Equal(t, int32(gensbe.SessionMessageHeaderSbeTemplateID), poller.TemplateID())
	require.Zero(t, poller.ClusterSessionID())
	require.Zero(t, poller.LeadershipTermID())
	require.Equal(t, conductor.NullCorrelationID, poller.CorrelationID())
	require.Nil(t, poller.EgressImage())
}

func TestHandleConnectEgressFragment_skipped(t *testing.T) {
	cases := []struct {
		name                string
		encode              func(t *testing.T) []byte
		assertNotChallenged bool
	}{
		{
			name: "unknownTemplate",
			encode: func(t *testing.T) []byte {
				return encodeSessionKeepAliveWire(t, egressTestClusterSessionID)
			},
			assertNotChallenged: true,
		},
		{
			name: "badSchema",
			encode: func(t *testing.T) []byte {
				return encodeWireWithSchemaID(t, 999)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := newConnectEgressPollerForTest(nil)

			invokeHandleConnectEgressFragment(t, poller, tc.encode(t))

			require.False(t, poller.IsPollComplete())
			if tc.assertNotChallenged {
				require.False(t, poller.IsChallenged())
			}
			require.Zero(t, poller.TemplateID())
		})
	}
}

func requireConnectEgressFragmentAtNoPanicNoCalls(t *testing.T, offset, length int) {
	t.Helper()

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)
	requireHandleConnectEgressFragmentAtNoPanicNoCalls(t, poller, wire, offset, length)
}

func TestHandleConnectEgressFragment_sessionEvent_truncatedBody(t *testing.T) {
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	truncated := wire[:len(wire)-4]
	requireHandleConnectEgressFragmentAtNoPanicNoCalls(t, poller, truncated, 0, len(truncated))
}

func TestHandleConnectEgressFragment_sessionEvent_nonZeroOffset(t *testing.T) {
	const prefixLen = 16

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	poller := newConnectEgressPollerForTest(nil)

	invokeHandleConnectEgressFragmentWithPrefix(t, poller, wire, prefixLen)

	require.True(t, poller.IsPollComplete())
	require.Equal(t, egressTestClusterSessionID, poller.ClusterSessionID())
}

func TestHandleConnectEgressFragment_truncatedBuffer(t *testing.T) {
	requireConnectEgressFragmentAtNoPanicNoCalls(t, 0, codecs.MessageHeaderEncodedLen()-1)
}

func TestHandleConnectEgressFragment_negativeLength(t *testing.T) {
	requireConnectEgressFragmentAtNoPanicNoCalls(t, 0, -1)
}

func TestHandleConnectEgressFragment_overflowBounds(t *testing.T) {
	requireConnectEgressFragmentAtNoPanicNoCalls(t, math.MaxInt-7, codecs.MessageHeaderEncodedLen())
}

func TestHandleConnectEgressFragment_largeCapturedPayloads(t *testing.T) {
	const payloadLen = 2000

	cases := []struct {
		name   string
		encode func(t *testing.T) (wire, expected []byte)
		assert func(t *testing.T, poller *ConnectEgressPoller, expected []byte)
	}{
		{
			name: "ingressEndpoints",
			encode: func(t *testing.T) ([]byte, []byte) {
				endpoints := bytes.Repeat([]byte{0x32}, payloadLen)
				return encodeNewLeaderWireWithEndpoints(
					t,
					egressTestClusterSessionID,
					1,
					endpoints,
				), endpoints
			},
			assert: func(t *testing.T, poller *ConnectEgressPoller, expected []byte) {
				require.True(t, poller.IsPollComplete())
				require.Equal(t, int32(gensbe.NewLeaderEventSbeTemplateID), poller.TemplateID())
				require.Equal(t, expected, poller.Detail())
			},
		},
		{
			name: "detail",
			encode: func(t *testing.T) ([]byte, []byte) {
				detail := bytes.Repeat([]byte{0x33}, payloadLen)
				fields := defaultSessionEventFields(detail)
				fields.Code = gensbe.EventCode_REDIRECT
				return encodeSessionEventWireWithFields(
					t,
					egressTestClusterSessionID,
					fields,
					gensbe.SessionEventSbeSchemaVersion,
				), detail
			},
			assert: func(t *testing.T, poller *ConnectEgressPoller, expected []byte) {
				require.True(t, poller.IsPollComplete())
				require.Equal(t, int32(gensbe.SessionEventSbeTemplateID), poller.TemplateID())
				require.Equal(t, expected, poller.Detail())
				require.Equal(t, EventCodeRedirect, poller.EventCode())
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, expected := tc.encode(t)
			poller := newConnectEgressPollerForTest(nil)

			invokeHandleConnectEgressFragment(t, poller, wire)

			tc.assert(t, poller, expected)
		})
	}
}
