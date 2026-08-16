//go:build testhooks

package consensus

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type recordingSessionConnectHandler struct {
	calls []sessionConnectCall
}

type sessionConnectCall struct {
	fields codecs.SessionConnectRequestFields
	nowNs  int64
}

func (h *recordingSessionConnectHandler) HandleSessionConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	// Clone var slices so later buffer reuse cannot mutate recorded state.
	fields.ResponseChannel = bytes.Clone(fields.ResponseChannel)
	fields.EncodedCredentials = bytes.Clone(fields.EncodedCredentials)
	fields.ClientInfo = bytes.Clone(fields.ClientInfo)
	h.calls = append(h.calls, sessionConnectCall{fields: fields, nowNs: nowNs})
}

func encodeSessionConnectWire(t *testing.T, fields codecs.SessionConnectRequestFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	wireLen, ok := codecs.SessionConnectRequestWireLen(
		actingVersion,
		len(fields.ResponseChannel),
		len(fields.EncodedCredentials),
		len(fields.ClientInfo),
	)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

// encodeUnhandledClientIngressWire encodes a NewLeaderEvent frame. NewLeaderEvent
// is egress-only. The client-ingress switch does not handle that template.
func encodeUnhandledClientIngressWire(t *testing.T) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.NewLeaderEventSbeSchemaVersion)
	fields := codecs.NewLeaderEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		LeaderMemberID:   3,
	}
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.NewLeaderEventSbeBlockLength,
		len(fields.IngressEndpoints),
	)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeNewLeaderEvent(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeChallengeResponseWire(t *testing.T, fields codecs.ChallengeResponseFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.ChallengeResponseSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ChallengeResponseSbeBlockLength,
		len(fields.EncodedCredentials),
	)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeChallengeResponse(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func sampleConnectFields() codecs.SessionConnectRequestFields {
	return codecs.SessionConnectRequestFields{
		CorrelationID:      99,
		ResponseStreamID:   102,
		Version:            codecs.ClusterProtocolSemanticVersion(),
		ResponseChannel:    []byte("aeron:udp?endpoint=127.0.0.1:40123"),
		EncodedCredentials: []byte{0xDE, 0xAD},
		ClientInfo:         []byte("client-a"),
	}
}

// agentWithClientIngress wires a queue client ingress and handler onto an agent
// in role. election may be nil for a settled member. A nil handler stays
// unwired. A typed nil *recordingSessionConnectHandler is non-nil as the
// SessionConnectHandler interface.
//
// leaderMemberID is the agent leader hint. Pass nullMemberID so non-leader
// drop rows stay pre-decode. Redirect tests pass a known leader id.
func agentWithClientIngress(
	role Role,
	election ElectionRunner,
	handler *recordingSessionConnectHandler,
	leaderMemberID int32,
) (*Agent, *QueueClientIngress) {
	q := &QueueClientIngress{}
	a := NewAgent(AgentConfig{
		Role:           role,
		Election:       election,
		LeaderMemberID: leaderMemberID,
	})
	a.SetClientIngress(q)
	if handler != nil {
		a.SetSessionConnectHandler(handler)
	}
	return a, q
}

// leaderWithClientIngress wires a RoleLeader, queue ingress, and recording
// session-connect handler. election may be nil for a settled leader.
func leaderWithClientIngress(
	election ElectionRunner,
) (*Agent, *QueueClientIngress, *recordingSessionConnectHandler) {
	handler := &recordingSessionConnectHandler{}
	a, q := agentWithClientIngress(RoleLeader, election, handler, nullMemberID)
	return a, q, handler
}

func TestPollClientIngress_nilIngress_zero(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.ClientIngress())
	require.NotPanics(t, func() {
		require.Equal(t, 0, a.pollClientIngress(1))
	})
}

func TestHandleSessionConnect_branchTable(t *testing.T) {
	fields := sampleConnectFields()
	wire := encodeSessionConnectWire(t, fields)
	const nowNs int64 = 1_000_000

	cases := []struct {
		name           string
		role           Role
		election       ElectionRunner
		leaderMemberID int32
		handler        *recordingSessionConnectHandler
		wantCalls      int
		wantPending    int
		wantRedirects  int
	}{
		{
			name:           "steady_leader_forwards",
			role:           RoleLeader,
			leaderMemberID: nullMemberID,
			handler:        &recordingSessionConnectHandler{},
			wantCalls:      1,
			wantPending:    0,
		},
		{
			name:           "follower_unknown_leader_drops",
			role:           RoleFollower,
			leaderMemberID: nullMemberID,
			handler:        &recordingSessionConnectHandler{},
			wantCalls:      0,
			wantPending:    0,
		},
		{
			name:           "candidate_unknown_leader_drops",
			role:           RoleCandidate,
			leaderMemberID: nullMemberID,
			handler:        &recordingSessionConnectHandler{},
			wantCalls:      0,
			wantPending:    0,
		},
		{
			name:           "follower_known_leader_redirects",
			role:           RoleFollower,
			leaderMemberID: 3,
			handler:        &recordingSessionConnectHandler{},
			wantCalls:      0,
			wantPending:    0,
			wantRedirects:  1,
		},
		{
			name:           "election_live_leader_drops",
			role:           RoleLeader,
			election:       &recordingRunner{},
			leaderMemberID: nullMemberID,
			handler:        &recordingSessionConnectHandler{},
			wantCalls:      0,
			wantPending:    0,
		},
		{
			// Election live drops before decode even with a known leader.
			name:           "election_live_follower_known_leader_drops",
			role:           RoleFollower,
			election:       &recordingRunner{},
			leaderMemberID: 1,
			handler:        nil,
			wantCalls:      0,
			wantPending:    0,
		},
		{
			name:           "nil_handler_acceptSessionConnect",
			role:           RoleLeader,
			leaderMemberID: nullMemberID,
			handler:        nil,
			wantCalls:      0,
			wantPending:    1,
		},
		{
			name:           "nil_handler_follower_unknown_no_accept",
			role:           RoleFollower,
			leaderMemberID: nullMemberID,
			handler:        nil,
			wantCalls:      0,
			wantPending:    0,
		},
		{
			name:           "nil_handler_follower_known_redirects",
			role:           RoleFollower,
			leaderMemberID: 2,
			handler:        nil,
			wantCalls:      0,
			wantPending:    0,
			wantRedirects:  1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, q := agentWithClientIngress(
				tc.role, tc.election, tc.handler, tc.leaderMemberID,
			)
			wireImmediateEgress(t, a)
			a.SetIngressEndpoints("0=host:1")

			q.Enqueue(bytes.Clone(wire))
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			require.Equal(t, tc.wantPending, a.PendingSessionCount())
			require.Equal(t, 0, a.OpenSessionCount())
			require.Len(t, a.egressEvents, tc.wantRedirects)

			if tc.handler == nil {
				if tc.wantPending == 1 {
					s, ok := a.sessions.sessionByID(1)
					require.True(t, ok)
					requireClientSessionFields(t, s, fields, sessionPending, nowNs)
				}
				return
			}
			require.Len(t, tc.handler.calls, tc.wantCalls)
			if tc.wantCalls == 0 {
				return
			}
			got := tc.handler.calls[0]
			require.Equal(t, nowNs, got.nowNs)
			require.Equal(t, fields, got.fields)
		})
	}
}

func TestHandleClientIngressFragment_dropsWrongFrames(t *testing.T) {
	a, q, handler := leaderWithClientIngress(nil)
	fields := sampleConnectFields()

	// Unhandled template. Outside the client-ingress switch.
	q.Enqueue(encodeUnhandledClientIngressWire(t))
	// Wrong schema id.
	badSchema := encodeSessionConnectWire(t, fields)
	badSchema[4], badSchema[5] = 0, 0
	q.Enqueue(badSchema)
	// Truncated frame. Header peek fails.
	q.Enqueue([]byte{0x10, 0x00, 0x03})
	// Valid template 3 header and fixed block with truncated var section.
	// Settled leader peeks template then decode fails closed.
	corruptBody := encodeSessionConnectWire(t, fields)
	// Keep MessageHeader and root block. Drop var payloads.
	headerAndBlock := codecs.MessageHeaderEncodedLen() +
		int(clusterwire.SessionConnectRequestSbeBlockLength)
	q.Enqueue(corruptBody[:headerAndBlock])

	require.Equal(t, 4, a.pollClientIngress(50))
	require.Empty(t, handler.calls)
}

func TestHandleClientIngressFragment_challengeResponse(t *testing.T) {
	const nowNs int64 = 500

	t.Run("leader_challenged_forwards", func(t *testing.T) {
		auth := newScriptedAuthenticator()
		auth.enqueue(1, authAction{kind: authActionChallenge, challenge: []byte("c")})
		p, _ := boundLogPublisher(t)
		a := leaderWithAuth(t, auth, p, 0)
		q := &QueueClientIngress{}
		a.SetClientIngress(q)
		a.acceptSessionConnect(validConnectFields(1), 1)
		require.Equal(t, 0, a.sessionWork(1))
		require.Equal(t, sessionChallenged, a.sessions.pending[0].state)

		q.Enqueue(encodeChallengeResponseWire(t, codecs.ChallengeResponseFields{
			CorrelationID:      77,
			ClusterSessionID:   1,
			EncodedCredentials: []byte("resp"),
		}))
		require.Equal(t, 1, a.pollClientIngress(nowNs))
		require.Len(t, auth.challengeResp, 1)
		require.Equal(t, int64(1), auth.challengeResp[0].sessionID)
		require.Equal(t, []byte("resp"), auth.challengeResp[0].credentials)
		require.Equal(t, nowNs, auth.challengeResp[0].nowNs)
		require.Equal(t, int64(77), a.sessions.pending[0].correlationID)
	})

	// Drop paths share setup. Non-leader known-leader is a named deviation: no
	// consensus-channel forward of ChallengeResponse and no REDIRECT.
	dropCases := []struct {
		name       string
		cfg        AgentConfig
		wireEgress bool
		sessionID  int64
	}{
		{
			name:      "leader_unknown_session_drops",
			cfg:       AgentConfig{Role: RoleLeader},
			sessionID: 99,
		},
		{
			name:       "non_leader_known_leader_drops",
			cfg:        AgentConfig{Role: RoleFollower, LeaderMemberID: 3},
			wireEgress: true,
			sessionID:  1,
		},
		{
			name:      "election_live_drops",
			cfg:       AgentConfig{Role: RoleLeader, Election: &recordingRunner{}},
			sessionID: 1,
		},
	}
	for _, tc := range dropCases {
		t.Run(tc.name, func(t *testing.T) {
			auth := newScriptedAuthenticator()
			tc.cfg.Authenticator = auth
			a := NewAgent(tc.cfg)
			if tc.wireEgress {
				wireImmediateEgress(t, a)
			}
			q := &QueueClientIngress{}
			a.SetClientIngress(q)
			q.Enqueue(encodeChallengeResponseWire(t, codecs.ChallengeResponseFields{
				CorrelationID:    1,
				ClusterSessionID: tc.sessionID,
			}))
			require.Equal(t, 1, a.pollClientIngress(nowNs))
			require.Empty(t, auth.challengeResp)
			// Asserts no REDIRECT or other egress event for ChallengeResponse.
			require.Empty(t, a.egressEvents)
		})
	}
}

func TestSetClientIngress_wireAndClear(t *testing.T) {
	handler := &recordingSessionConnectHandler{}
	q := &QueueClientIngress{}
	a := NewAgent(AgentConfig{Role: RoleLeader})
	a.SetSessionConnectHandler(handler)

	// Wire: poll delivers.
	a.SetClientIngress(q)
	require.Same(t, q, a.ClientIngress())
	q.Enqueue(encodeSessionConnectWire(t, sampleConnectFields()))
	require.Equal(t, 1, a.pollClientIngress(1))
	require.Len(t, handler.calls, 1)

	// Clear: poll returns 0. Queued frame is not drained after unbind.
	a.SetClientIngress(nil)
	require.Nil(t, a.ClientIngress())
	q.Enqueue(encodeSessionConnectWire(t, sampleConnectFields()))
	require.Equal(t, 0, a.pollClientIngress(1))
	require.Len(t, handler.calls, 1)
}

// TestClientIngress_earlyUnbindBlocksAccept asserts that unbinding client
// ingress before clearing election blocks accept on a settled leader. With
// client ingress nil, poll returns 0 and does not invoke the session-connect
// handler.
//
// First leg mirrors module Close binding clear order: ingress, then handler,
// then election. poll == 0 depends on SetClientIngress(nil).
//
// Counter-case: clearing election alone with ingress still wired accepts.
func TestClientIngress_earlyUnbindBlocksAccept(t *testing.T) {
	a, q, handler := leaderWithClientIngress(&recordingRunner{})

	// Module Close early binding clear order.
	a.SetClientIngress(nil)
	a.SetSessionConnectHandler(nil)
	a.CloseSessionEgress()
	a.SetEgressPublicationAdder(nil)
	a.SetElection(nil)
	// Role may still read leader during teardown.
	require.Equal(t, RoleLeader, a.Role())
	require.Nil(t, a.Election())
	require.Nil(t, a.ClientIngress())

	q.Enqueue(encodeSessionConnectWire(t, sampleConnectFields()))
	require.Equal(t, 0, a.pollClientIngress(1))
	require.Empty(t, handler.calls)

	// Counter-case: election clear alone with ingress still wired accepts.
	// Asserts early unbind is required, not only SetElection(nil).
	hazard, hazardQ, hazardHandler := leaderWithClientIngress(&recordingRunner{})
	hazard.SetElection(nil)
	hazardQ.Enqueue(encodeSessionConnectWire(t, sampleConnectFields()))
	require.Equal(t, 1, hazard.pollClientIngress(1))
	require.Len(t, hazardHandler.calls, 1)
}

func TestPollClientIngress_fragmentLimit(t *testing.T) {
	a, q, handler := leaderWithClientIngress(nil)

	wire := encodeSessionConnectWire(t, sampleConnectFields())
	for range defaultClientIngressFragmentLimit + 1 {
		q.Enqueue(bytes.Clone(wire))
	}

	require.Equal(t, defaultClientIngressFragmentLimit, a.pollClientIngress(1))
	require.Len(t, handler.calls, defaultClientIngressFragmentLimit)
	require.Equal(t, 1, a.pollClientIngress(1))
	require.Len(t, handler.calls, defaultClientIngressFragmentLimit+1)
	require.Equal(t, 0, a.pollClientIngress(1))
}
