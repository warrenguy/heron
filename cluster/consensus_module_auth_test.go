package cluster

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

// recordingConnectAuthenticator records HandleConnectRequest only. Other
// Authenticator methods are silent no-ops so sessions stay unresolved.
type recordingConnectAuthenticator struct {
	sessionIDs  []int64
	credentials [][]byte
	nowNs       []int64
}

func (r *recordingConnectAuthenticator) HandleConnectRequest(
	sessionID int64,
	encodedCredentials []byte,
	nowNs int64,
) {
	r.sessionIDs = append(r.sessionIDs, sessionID)
	r.credentials = append(r.credentials, bytes.Clone(encodedCredentials))
	r.nowNs = append(r.nowNs, nowNs)
}

func (r *recordingConnectAuthenticator) HandleChallengeResponse(int64, []byte, int64) {}

func (r *recordingConnectAuthenticator) HandleConnectedSession(consensus.SessionProxy, int64) {}

func (r *recordingConnectAuthenticator) HandleChallengedSession(consensus.SessionProxy, int64) {}

var _ consensus.Authenticator = (*recordingConnectAuthenticator)(nil)

// openWireStyleAgent builds an agent the way OpenConsensusModule maps
// Config.Authenticator into AgentConfig. Sets RoleLeader so connect handoff
// runs without an active election.
func openWireStyleAgent(cfg Config) *consensus.Agent {
	return consensus.NewAgent(consensus.AgentConfig{
		Commit:           new(atomic.Int64),
		ThisMemberID:     cfg.MemberID,
		LeaderMemberID:   election.NullMemberID,
		Role:             consensus.RoleLeader,
		Authenticator:    cfg.Authenticator,
		SessionTimeoutNs: cfg.SessionTimeoutNs,
	})
}

// TestConfigAuthenticator_openWireStyle asserts Config.Authenticator reaches
// the agent the same way OpenConsensusModule wires AgentConfig.
//
// Does not call OpenConsensusModule. OpenConsensusModule needs a connected
// media driver and Archive control client.
func TestConfigAuthenticator_openWireStyle(t *testing.T) {
	t.Parallel()

	auth := &recordingConnectAuthenticator{}
	agent := openWireStyleAgent(Config{
		MemberID:      0,
		Authenticator: auth,
	})
	enqueueSessionConnect(t, agent, codecs.SessionConnectRequestFields{
		CorrelationID:      42,
		ResponseStreamID:   100,
		Version:            codecs.ClusterProtocolSemanticVersion(),
		ResponseChannel:    []byte("aeron:udp?endpoint=127.0.0.1:40123"),
		EncodedCredentials: []byte{0xCA, 0xFE},
		ClientInfo:         []byte("wire"),
	})

	const nowNs int64 = 9_000
	require.Positive(t, agent.DoWork(nowNs))
	require.Equal(t, 1, agent.PendingSessionCount())
	require.Equal(t, []int64{1}, auth.sessionIDs)
	require.Equal(t, [][]byte{{0xCA, 0xFE}}, auth.credentials)
	require.Equal(t, []int64{nowNs}, auth.nowNs)
}

// TestConfigAuthenticator_nilDefaultOpenWireStyle asserts a nil
// Config.Authenticator installs the accept-all default.
//
// Exercises connect handoff only. Accept stores a pending session without
// error. Full session open needs egress and log wired.
func TestConfigAuthenticator_nilDefaultOpenWireStyle(t *testing.T) {
	t.Parallel()

	agent := openWireStyleAgent(Config{
		MemberID:      0,
		Authenticator: nil,
	})
	enqueueSessionConnect(t, agent, minimalConnectFields())
	require.Positive(t, agent.DoWork(1))
	require.Equal(t, 1, agent.PendingSessionCount())
}

// TestConfigSessionTimeoutNs_openWireStyle asserts Config.SessionTimeoutNs
// reaches the agent the same way OpenConsensusModule wires AgentConfig.
//
// Custom timeout bounds pre-open reclaim: a pending session drops only after
// the configured window.
func TestConfigSessionTimeoutNs_openWireStyle(t *testing.T) {
	t.Parallel()

	const timeoutNs int64 = 1_000
	agent := openWireStyleAgent(Config{
		MemberID:         0,
		SessionTimeoutNs: timeoutNs,
	})
	// No egress adder: accept stays pending for deadline reclaim.
	enqueueSessionConnect(t, agent, minimalConnectFields())
	require.Positive(t, agent.DoWork(0))
	require.Equal(t, 1, agent.PendingSessionCount())
	// Just before timeout: still pending.
	require.Equal(t, 0, agent.DoWork(timeoutNs-1))
	require.Equal(t, 1, agent.PendingSessionCount())
	// At deadline: reclaim.
	require.Equal(t, 1, agent.DoWork(timeoutNs))
	require.Equal(t, 0, agent.PendingSessionCount())
}

// enqueueSessionConnect wires QueueClientIngress and enqueues one connect.
func enqueueSessionConnect(
	t *testing.T,
	agent *consensus.Agent,
	fields codecs.SessionConnectRequestFields,
) {
	t.Helper()
	q := &consensus.QueueClientIngress{}
	agent.SetClientIngress(q)
	q.Enqueue(encodeSessionConnectForAuthTest(t, fields))
}

// minimalConnectFields is a valid connect used when credentials and channel
// shape are not under test.
func minimalConnectFields() codecs.SessionConnectRequestFields {
	return codecs.SessionConnectRequestFields{
		CorrelationID:    1,
		ResponseStreamID: 1,
		Version:          codecs.ClusterProtocolSemanticVersion(),
		ResponseChannel:  []byte("ch"),
	}
}

func encodeSessionConnectForAuthTest(
	t *testing.T,
	fields codecs.SessionConnectRequestFields,
) []byte {
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
