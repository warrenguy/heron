package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
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

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, testutil.ClientTimeoutPayload(42))

	count := adapter.Receive(conductor.NullCorrelationID, nil)
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

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, testutil.ClientTimeoutPayload(99))

	count := adapter.Receive(conductor.NullCorrelationID, nil)
	require.Equal(t, 1, count)
	require.False(t, timeoutCalled)
}

func TestDriverEventsAdapter_OnNextAvailableSessionID(t *testing.T) {
	const nextSessionID int32 = 42

	tests := []struct {
		name                 string
		setActiveCorrelation bool
		eventCorrelationID   int64
		eventSessionID       int32
		eventPayload         []byte
		receiveCorrelationID int64
		useNilCompleter      bool
		wantUnhandled        int64
		wantDone             bool
		wantSessionID        int32
		wantPendingRemoved   bool
	}{
		{
			name:                 "success",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantSessionID:        nextSessionID,
			wantPendingRemoved:   true,
		},
		{
			name:                 "wrongActiveCorrelationID",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestOtherCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "nullCorrelationID",
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: conductor.NullCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "nilCompleter",
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
			useNilCompleter:      true,
			wantUnhandled:        1,
		},
		{
			name:                 "shortBody",
			eventPayload:         []byte{0x01},
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance := newConductorForTestAtWorkNano(
				t,
				func(error) {},
				0,
				0,
			)
			require.NoError(
				t,
				conductorInstance.RegisterPending(
					conductor.PendingKindNextSessionID,
					pendingTestCorrelationID,
					pendingTestTimeout,
				),
			)
			if testCase.setActiveCorrelation {
				conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)
			}

			adapter := conductorInstance.EventsAdapterForTest()
			payload := testCase.eventPayload
			if payload == nil {
				payload = conductor.CorrelationAndInt32Bytes(
					testCase.eventCorrelationID,
					testCase.eventSessionID,
				)
			}
			transmitConductorEvent(
				t,
				adapter,
				conductor.OnNextAvailableSessionID,
				payload,
			)

			var count int
			if testCase.useNilCompleter {
				count = adapter.Receive(testCase.receiveCorrelationID, nil)
			} else {
				count = adapter.Receive(
					testCase.receiveCorrelationID,
					conductorInstance,
				)
			}
			require.Equal(t, 1, count)
			require.Equal(t, testCase.wantUnhandled, adapter.UnhandledEventCount())

			done, registration, err := conductorInstance.PollPending(
				pendingTestCorrelationID,
			)
			require.Equal(t, testCase.wantDone, done)
			if testCase.wantDone {
				require.Equal(t, testCase.wantSessionID, registration.Int32Value)
			} else {
				require.Zero(t, registration)
			}
			require.NoError(t, err)

			_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.Equal(t, !testCase.wantPendingRemoved, ok)
		})
	}
}

func TestDriverEventsAdapter_OnClientTimeout_shortBody(t *testing.T) {
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, []byte{0x01})

	count := adapter.Receive(conductor.NullCorrelationID, nil)
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

	count := adapter.Receive(conductor.NullCorrelationID, nil)
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

	count := adapter.Receive(conductor.NullCorrelationID, nil)
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

	count := adapter.Receive(conductor.NullCorrelationID, nil)
	require.Zero(t, count)
	require.True(t, adapter.IsInvalid())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrBroadcastLapped)

	count = adapter.Receive(conductor.NullCorrelationID, nil)
	require.Zero(t, count)
	require.Len(t, gotErrors, 1)
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
