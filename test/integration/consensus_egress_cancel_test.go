//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

// recordingEgressAdder records the id of each add the embedded client starts.
type recordingEgressAdder struct {
	*aeron.Client
	adds []int64
}

func (r *recordingEgressAdder) OfferAddPublication(
	streamID int32,
	channel string,
) (int64, error) {
	id, err := r.Client.OfferAddPublication(streamID, channel)
	if err == nil {
		r.adds = append(r.adds, id)
	}
	return id, err
}

// encodeIPCSessionConnect encodes one SessionConnectRequest whose response
// channel is IPC on streamID.
func encodeIPCSessionConnect(t *testing.T, streamID int32) []byte {
	t.Helper()
	fields := codecs.SessionConnectRequestFields{
		CorrelationID:    1,
		ResponseStreamID: streamID,
		Version:          codecs.ClusterProtocolSemanticVersion(),
		ResponseChannel:  []byte(harness.IPCChannel),
	}
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

// TestConsensusEgress_cancelAfterReadyReleasesPublication asserts the driver
// frees an egress publication released after its ready reply.
func TestConsensusEgress_cancelAfterReadyReleasesPublication(t *testing.T) {
	tests := []struct {
		name     string
		streamID int32
		// extraTicks is the number of client.DoWork calls after the ready
		// reply. One tick moves the ready mark off the add.
		extraTicks int
	}{
		{
			name:     "markNamesAdd",
			streamID: harness.ConsensusEgressCancelMarkedStreamID,
		},
		{
			name:       "markMoved",
			streamID:   harness.ConsensusEgressCancelMovedStreamID,
			extraTicks: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := harness.SharedParallel(t)
			client, hadClientError := harness.ConnectIPCClient(t, md)
			adder := &recordingEgressAdder{Client: client}

			// No log publisher: sessionPendingWork never polls the add.
			agent := consensus.NewAgent(consensus.AgentConfig{Role: consensus.RoleLeader})
			agent.SetEgressPublicationAdder(adder)
			ingress := &consensus.QueueClientIngress{}
			agent.SetClientIngress(ingress)

			// Phase 1: one DoWork starts the egress add.
			ingress.Enqueue(encodeIPCSessionConnect(t, tc.streamID))
			agent.DoWork(time.Now().UnixNano())
			require.Equal(t, 1, agent.PendingSessionCount())
			require.Len(t, adder.adds, 1)

			// Phase 2: release after the ready reply. The agent has not polled.
			id := adder.adds[0]
			requireReadyPublicationReleased(
				t,
				client,
				hadClientError,
				id,
				func() bool {
					_, ok := client.Publication(id)
					return ok
				},
				tc.extraTicks,
				agent.CloseSessionEgress,
			)
			finishPubSubIPCTest(t, md, hadClientError)
		})
	}
}
