package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestDriverEventsAdapter_OnClientTimeout_matchingClient(t *testing.T) {
	var (
		timeoutCalled bool
		gotErrors     []error
	)
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func(err error) { gotErrors = append(gotErrors, err) },
		func() { timeoutCalled = true },
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, clientTimeoutPayload(42))

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Equal(t, 1, count)
	require.True(t, timeoutCalled)
	require.Empty(t, gotErrors)
}

func TestDriverEventsAdapter_OnClientTimeout_otherClient(t *testing.T) {
	var timeoutCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func(error) { t.Fatal("error handler should not run") },
		func() { timeoutCalled = true },
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, clientTimeoutPayload(99))

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Equal(t, 1, count)
	require.False(t, timeoutCalled)
}

func TestDriverEventsAdapter_OnClientTimeout_shortBody(t *testing.T) {
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, []byte{0x01})

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}

func TestDriverEventsAdapter_OnError(t *testing.T) {
	var gotErrors []error
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		func(err error) { gotErrors = append(gotErrors, err) },
		func() { t.Fatal("timeout handler should not run") },
	)

	transmitConductorEvent(t, adapter, conductor.OnError, []byte{0x01, 0x02, 0x03, 0x04})

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Equal(t, 1, count)
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrDriverErrorEvent)
	require.EqualError(t, gotErrors[0], "conductor: driver error event received")
}

func TestDriverEventsAdapter_unhandledEvent(t *testing.T) {
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
	)

	transmitConductorEvent(t, adapter, conductor.OnPublicationReady, []byte{0x01, 0x02})

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}

func TestDriverEventsAdapter_broadcastLapped(t *testing.T) {
	var gotErrors []error
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		func(err error) { gotErrors = append(gotErrors, err) },
		func() { t.Fatal("timeout handler should not run") },
	)

	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	capacity := copyReceiver.BroadcastReceiver().Capacity()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	fillBroadcastUntilLap(t, tx, conductor.OnClientTimeout, payload, capacity)
	rewindBroadcastTailForLap(t, buf, capacity)

	count := adapter.Receive(conductor.NullCorrelationID)
	require.Zero(t, count)
	require.True(t, adapter.IsInvalid())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrBroadcastLapped)

	count = adapter.Receive(conductor.NullCorrelationID)
	require.Zero(t, count)
	require.Len(t, gotErrors, 1)
}

func clientTimeoutPayload(clientID int64) []byte {
	payload := make([]byte, conductor.ClientTimeoutBodyLength)
	aeronatomic.Wrap(payload).PutInt64(0, clientID)
	return payload
}

func newDriverEventsAdapterForTest(
	t *testing.T,
	clientID int64,
	errorHandler func(error),
	onClientTimeout func(),
) *conductor.DriverEventsAdapter {
	t.Helper()

	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	return conductor.NewDriverEventsAdapter(
		clientID,
		copyReceiver,
		errorHandler,
		onClientTimeout,
	)
}

func transmitConductorEvent(
	t *testing.T,
	adapter *conductor.DriverEventsAdapter,
	msgTypeID int,
	payload []byte,
) {
	t.Helper()

	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)
	err = tx.Transmit(msgTypeID, payload)
	require.NoError(t, err)
}
