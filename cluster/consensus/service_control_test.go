package consensus

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func encodeServiceAckWire(t *testing.T, fields codecs.ServiceAckFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.ServiceAckSbeSchemaVersion)
	buf := make([]byte, codecs.ServiceAckEncodedLen())
	end, ok := codecs.EncodeServiceAck(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeSessionCloseEventWire(t *testing.T) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.SessionCloseEventSbeSchemaVersion)
	buf := make([]byte, codecs.SessionCloseEventEncodedLen())
	end, ok := codecs.EncodeSessionCloseEvent(buf, 0, actingVersion, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	require.True(t, ok)
	return buf[:end]
}

func agentWithServiceIngress(serviceCount int) (*Agent, *QueueConsensusIngress) {
	return agentWithServiceIngressConfig(AgentConfig{ServiceCount: serviceCount})
}

func agentWithServiceIngressConfig(cfg AgentConfig) (*Agent, *QueueConsensusIngress) {
	q := &QueueConsensusIngress{}
	a := NewAgent(cfg)
	a.SetServiceIngress(q)
	return a, q
}

func TestPollServiceControl_nilIngress_zero(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 1})
	require.Nil(t, a.ServiceIngress())
	require.NotPanics(t, func() {
		require.Equal(t, 0, a.pollServiceControl())
	})
	require.Equal(t, 0, a.DoWork(1))
}

func TestSetServiceIngress_wireAndClear(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	require.Same(t, q, a.ServiceIngress())

	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 0, RelevantID: 1, ServiceID: 0,
	}))
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))

	a.SetServiceIngress(nil)
	require.Nil(t, a.ServiceIngress())
	require.Equal(t, 0, a.pollServiceControl())
}

func TestHandleServiceAck_happy(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	fields := codecs.ServiceAckFields{
		LogPosition: 42, Timestamp: 99, AckID: 0, RelevantID: 7, ServiceID: 0,
	}
	q.Enqueue(encodeServiceAckWire(t, fields))

	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())

	// Asserts check-only hasReached at the offered position matches the offered
	// fields and receive left ServiceAckFailure nil.
	ok, err := a.hasReachedServiceAcks(fields.LogPosition)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, a.ServiceAckFailure())
	// Depth unchanged after check-only hasReached.
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
}

func TestHandleServiceAck_receiveTimeMismatch(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)

	// First queued entry carries ack id 1 while expected id is 0.
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 1, RelevantID: 1, ServiceID: 0,
	}))
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	first := a.ServiceAckFailure()
	require.ErrorIs(t, first, ErrServiceAckOutOfSequence)

	// Second mismatched receive keeps the first cause (marked once).
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 20, AckID: 2, RelevantID: 2, ServiceID: 0,
	}))
	require.Equal(t, 1, a.DoWork(2))
	require.Equal(t, 2, a.ServiceAckQueueDepth(0))
	require.Same(t, first, a.ServiceAckFailure())
}

func TestHandleServiceAck_emptyLowerNoReceiveMark(t *testing.T) {
	t.Parallel()

	// Service 0 queue empty; mismatched ack arrives for service 1. Walk stops
	// at the empty lower-id queue: no failure marked at receive.
	a, q := agentWithServiceIngress(2)
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 99, RelevantID: 1, ServiceID: 1,
	}))
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_batchOrder(t *testing.T) {
	t.Parallel()

	// Multiple ServiceAcks in one poll batch. Offers keep arrival order.
	// Receive-time checks the first queued entry for the expected
	// serviceAckID step, not each incoming frame position. Asserts a multi-
	// step FIFO marks no out-of-sequence failure.
	a, q := agentWithServiceIngress(1)
	for i := range 3 {
		q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
			LogPosition: int64(i + 1),
			AckID:       int64(i),
			RelevantID:  int64(i * 10),
			ServiceID:   0,
		}))
	}
	require.Equal(t, 3, a.pollServiceControl())
	require.Equal(t, 3, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
	// First queued entry is still ack id 0 at log position 1.
	ok, err := a.hasReachedServiceAcks(1)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestHandleServiceAck_multiGenRemoveBetween(t *testing.T) {
	t.Parallel()

	// Two serviceAckID steps with remove between them. Asserts receive stays
	// clean and the next queued entry matches after advance.
	a, q := agentWithServiceIngress(1)
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 0, RelevantID: 1, ServiceID: 0,
	}))
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())
	ok, err := a.hasReachedServiceAcks(10)
	require.True(t, ok)
	require.NoError(t, err)
	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())

	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 20, AckID: 1, RelevantID: 2, ServiceID: 0,
	}))
	require.Equal(t, 1, a.pollServiceControl())
	require.NoError(t, a.ServiceAckFailure())
	ok, err = a.hasReachedServiceAcks(20)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
}

func TestHandleServiceAck_multiGenBufferedThenRemove(t *testing.T) {
	t.Parallel()

	// Two serviceAckID steps land before any remove. The queue holds both.
	// Asserts the first remove exposes the next queued entry for expected id 1.
	a, q := agentWithServiceIngress(1)
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 0, RelevantID: 1, ServiceID: 0,
	}))
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 20, AckID: 1, RelevantID: 2, ServiceID: 0,
	}))
	require.Equal(t, 2, a.pollServiceControl())
	require.Equal(t, 2, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())

	ok, err := a.hasReachedServiceAcks(10)
	require.True(t, ok)
	require.NoError(t, err)
	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))

	ok, err = a.hasReachedServiceAcks(20)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_outOfRangeServiceID(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)

	// Pre-seed a mismatched first entry so a hostile out-of-range frame must
	// not fire detection on a refused offer.
	require.True(t, a.serviceAcks.offer(0, serviceAck{ackID: 99, logPosition: 10}))

	for _, serviceID := range []int32{5, -1} {
		q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
			LogPosition: 10, AckID: 0, RelevantID: 1, ServiceID: serviceID,
		}))
	}
	require.Equal(t, 2, a.pollServiceControl())
	// Depth still only the pre-seeded entry. No failure from refused offers.
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceControlFragment_unknownTemplate(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)
	q.Enqueue(encodeSessionCloseEventWire(t))
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceControlFragment_undecodable(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(1)

	// Short buffer. Template peek fails.
	q.Enqueue([]byte{0x01, 0x02})
	// Wrong schema id. Template peek rejects.
	bad := make([]byte, 16)
	// blockLength=8, template=33, schema=0 (wrong), version=0
	bad[0] = 8
	bad[2] = 33
	q.Enqueue(bad)
	// Valid cluster schema and ServiceAck template. Truncated body makes
	// TryDecodeServiceAckAt return false after the template match.
	truncated := make([]byte, codecs.MessageHeaderEncodedLen())
	binary.LittleEndian.PutUint16(truncated[0:], clusterwire.ServiceAckSbeBlockLength)
	binary.LittleEndian.PutUint16(truncated[2:], clusterwire.ServiceAckSbeTemplateID)
	binary.LittleEndian.PutUint16(truncated[4:], clusterwire.MessageHeaderSbeSchemaID)
	binary.LittleEndian.PutUint16(truncated[6:], clusterwire.ServiceAckSbeSchemaVersion)
	q.Enqueue(truncated)

	require.NotPanics(t, func() {
		require.Equal(t, 3, a.pollServiceControl())
	})
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_electionLive(t *testing.T) {
	t.Parallel()

	// Acks still drain into queues while election is live (always polled).
	a, q := agentWithServiceIngressConfig(AgentConfig{
		ServiceCount: 1,
		Election:     &orderRunner{n: 0},
	})
	q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: 10, AckID: 0, RelevantID: 1, ServiceID: 0,
	}))
	require.Equal(t, 1, a.DoWork(1))
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.NoError(t, a.ServiceAckFailure())
}
