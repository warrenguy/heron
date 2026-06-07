package aeron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	clientTestRingDataCapacity    = 4096
	clientTestInterServiceTimeout = time.Second
	clientTestEpochMs             = 1_700_000_000_123
)

var clientTestStartNanoNs = (2 * time.Second).Nanoseconds()

func TestClient_nilGuards(t *testing.T) {
	var client *aeron.Client

	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
	require.NoError(t, client.Close())
	require.Zero(t, client.ClientId())
}

func TestClient_closeBehavior(t *testing.T) {
	client := &aeron.Client{}

	require.False(t, client.IsTerminating())
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.False(t, client.IsTerminating())
}

func TestClient_accessorGuardsAfterClose(t *testing.T) {
	accessorCases := []struct {
		name   string
		client func(t *testing.T) *aeron.Client
	}{
		{
			name: "closedClient",
			client: func(t *testing.T) *aeron.Client {
				client := aeron.NewAccessorTestClient(t)
				require.NoError(t, client.Close())
				return client
			},
		},
		{
			name: "nilClient",
			client: func(t *testing.T) *aeron.Client {
				// Nil accessors share mappedCncFile guard with TestClient_nilGuards.
				return nil
			},
		},
	}
	accessorCalls := []struct {
		name string
		call func(client *aeron.Client) error
	}{
		{
			name: "CncMetadata",
			call: func(client *aeron.Client) error {
				_, err := client.CncMetadata()
				return err
			},
		},
		{
			name: "CountersReader",
			call: func(client *aeron.Client) error {
				_, err := client.CountersReader()
				return err
			},
		},
		{
			name: "ErrorLogBuffer",
			call: func(client *aeron.Client) error {
				_, err := client.ErrorLogBuffer()
				return err
			},
		},
	}

	for _, accessorCase := range accessorCases {
		t.Run(accessorCase.name, func(t *testing.T) {
			client := accessorCase.client(t)
			for _, accessorCall := range accessorCalls {
				t.Run(accessorCall.name, func(t *testing.T) {
					require.EqualError(t, accessorCall.call(client), "aeron: client is closed")
				})
			}
		})
	}

	t.Run("retainedCountersReader", func(t *testing.T) {
		client := aeron.NewAccessorTestClient(t)

		retained, err := client.CountersReader()
		require.NoError(t, err)
		require.NotNil(t, retained)

		_, err = retained.GetCounterValue(0)
		require.NoError(t, err)

		require.NoError(t, client.Close())

		_, err = client.CountersReader()
		require.EqualError(t, err, "aeron: client is closed")

		_, err = retained.GetCounterValue(1)
		require.EqualError(t, err, "cnc: counter id 1 out of range: maxCounterId=0")
	})
}

func TestClient_conductorServiceTimeoutAfterInterServiceTimeout(t *testing.T) {
	nanoNs := clientTestStartNanoNs
	conductorInstance := newClientTestConductor(t, &nanoNs, clientTestEpochMs, nil)

	nanoNs += clientTestInterServiceTimeout.Nanoseconds() + 1

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
}

func TestClient_delayedFirstDoWorkReSeedsAfterConstructorSeed(t *testing.T) {
	nanoNs := clientTestStartNanoNs
	conductorInstance := newClientTestConductor(t, &nanoNs, clientTestEpochMs, nil)

	nanoNs += clientTestInterServiceTimeout.Nanoseconds() + 1

	client := aeron.NewClientForTest(
		conductorInstance,
		func() int64 { return nanoNs },
	)
	client.DoWork()
	require.False(t, client.IsTerminating())
}

type clientTestTB interface {
	Helper()
	require.TestingT
}

func newClientTestConductor(
	tb clientTestTB,
	nanoNs *int64,
	epochMs int64,
	errorHandler func(error),
) *conductor.Conductor {
	tb.Helper()

	ring := newClientTestRing(tb)
	ring.SetConsumerHeartbeatTime(epochMs)

	_, broadcastReceiver := newClientTestBroadcastPair(tb)

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)

	config := conductor.Config{
		ClientID:            42,
		DriverTimeout:       10 * time.Second,
		KeepAliveInterval:   500 * time.Millisecond,
		IdleSleepDuration:   16 * time.Millisecond,
		InterServiceTimeout: clientTestInterServiceTimeout,
		EpochClock: func() int64 {
			return epochMs
		},
		NanoClock: func() int64 {
			return *nanoNs
		},
	}
	if errorHandler != nil {
		config.ErrorHandler = errorHandler
	}

	return conductor.NewConductor(
		conductor.NewDriverProxy(ring, 42),
		broadcastReceiver,
		countersReader,
		config,
	)
}

func newClientTestRing(tb clientTestTB) *ringbuffer.ManyToOneRingBuffer {
	tb.Helper()

	bufferBytes := make([]byte, ringbuffer.TotalBufferLength(clientTestRingDataCapacity))
	buffer := aeronatomic.Wrap(bufferBytes)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring
}

func newClientTestBroadcastPair(
	tb clientTestTB,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	tb.Helper()

	bufferBytes := make([]byte, broadcast.TotalBufferLength(clientTestRingDataCapacity))
	buffer := aeronatomic.Wrap(bufferBytes)
	receiver, err := broadcast.NewBroadcastReceiver(buffer)
	require.NoError(tb, err)
	transmitter, err := broadcast.NewBroadcastTransmitter(buffer)
	require.NoError(tb, err)
	return transmitter, receiver
}

func newRapidTestClient(tb clientTestTB, clientID int64) *aeron.Client {
	tb.Helper()

	nanoNs := clientTestStartNanoNs
	conductorInstance := newClientTestConductor(tb, &nanoNs, clientTestEpochMs, nil)
	return aeron.NewClientForTest(
		conductorInstance,
		func() int64 { return nanoNs },
		clientID,
	)
}
