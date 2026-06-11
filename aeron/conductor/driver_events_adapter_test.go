package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestDriverEventsAdapter_clientTimeout_matchingClient(t *testing.T) {
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

func TestDriverEventsAdapter_clientTimeout_otherClient(t *testing.T) {
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

func TestDriverEventsAdapter_nextAvailableSessionID(t *testing.T) {
	const nextSessionID int32 = 42

	tests := []struct {
		name                 string
		setActiveCorrelation bool
		eventCorrelationID   int64
		eventSessionID       int32
		eventPayload         []byte
		receiveCorrelationID int64
		pendingKind          conductor.PendingKind
		useNilPending        bool
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
			pendingKind:          conductor.PendingKindNextSessionID,
			wantDone:             true,
			wantSessionID:        nextSessionID,
			wantPendingRemoved:   true,
		},
		{
			name:                 "paddedBody",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			eventPayload: func() []byte {
				payload := conductor.CorrelationAndInt32Bytes(
					pendingTestCorrelationID,
					nextSessionID,
				)
				return append(payload, 0xFF)
			}(),
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
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
			pendingKind:          conductor.PendingKindNextSessionID,
			wantUnhandled:        1,
		},
		{
			name:                 "nullCorrelationID",
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: conductor.NullCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
			wantUnhandled:        1,
		},
		{
			name:                 "nilPending",
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
			useNilPending:        true,
			wantUnhandled:        1,
		},
		{
			name:                 "wrongKind",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			eventSessionID:       nextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
		},
		{
			name:                 "shortBody",
			eventPayload:         []byte{0x01},
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
			wantUnhandled:        1,
		},
		{
			name:                 "minimumMinusOneBody",
			eventPayload:         bodyOneByteShort(conductor.NextAvailableSessionIDBodyLength),
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
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
					testCase.pendingKind,
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
			if testCase.useNilPending {
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

func TestDriverEventsAdapter_clientTimeout_shortBody(t *testing.T) {
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

func TestDriverEventsAdapter_errorEvent(t *testing.T) {
	const errorCode int32 = 11

	tests := []struct {
		name                 string
		setActiveCorrelation bool
		registerPending      bool
		pendingKind          conductor.PendingKind
		eventCorrelationID   int64
		eventPayload         []byte
		receiveCorrelationID int64
		useNilPending        bool
		wantUnhandled        int64
		wantDone             bool
		wantDriverError      bool
		wantErrorMessage     string
		wantPendingRemoved   bool
	}{
		{
			name:                 "matchingActive",
			setActiveCorrelation: true,
			registerPending:      true,
			pendingKind:          conductor.PendingKindNextSessionID,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantDriverError:      true,
			wantErrorMessage:     "registration failed",
			wantPendingRemoved:   true,
		},
		{
			name:                 "matchingActiveOperationSuccessKind",
			setActiveCorrelation: true,
			registerPending:      true,
			pendingKind:          conductor.PendingKindOperationSuccess,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantDriverError:      true,
			wantErrorMessage:     "registration failed",
			wantPendingRemoved:   true,
		},
		{
			name:                 "emptyMessage",
			setActiveCorrelation: true,
			registerPending:      true,
			pendingKind:          conductor.PendingKindNextSessionID,
			eventCorrelationID:   pendingTestCorrelationID,
			eventPayload: conductor.ErrorResponseBytes(
				pendingTestCorrelationID,
				errorCode,
				"",
			),
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantDriverError:      true,
			wantErrorMessage:     "",
			wantPendingRemoved:   true,
		},
		{
			name:                 "nonMatchingActive",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestOtherCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "nullCorrelationID",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: conductor.NullCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "nilPending",
			setActiveCorrelation: true,
			registerPending:      true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			useNilPending:        true,
			wantUnhandled:        1,
		},
		{
			name:                 "noPendingRegistration",
			setActiveCorrelation: true,
			registerPending:      false,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "shortBody",
			eventPayload:         []byte{0x01},
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "minimumMinusOneBody",
			eventPayload:         bodyOneByteShort(conductor.ErrorResponseMinBodyLength),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name: "lengthMismatch",
			eventPayload: func() []byte {
				payload := make([]byte, conductor.ErrorResponseMinBodyLength)
				buffer := atomic.Wrap(payload)
				correlationOffset := conductor.ErrorResponseOffendingCommandCorrelationIDOffset
				buffer.PutInt64(correlationOffset, pendingTestCorrelationID)
				buffer.PutInt32(conductor.ErrorResponseErrorCodeOffset, errorCode)
				buffer.PutInt32(conductor.ErrorResponseErrorMessageLengthOffset, 8)
				return payload
			}(),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name: "truncatedMessage",
			eventPayload: func() []byte {
				payload := conductor.ErrorResponseBytes(
					pendingTestCorrelationID,
					errorCode,
					"fail",
				)
				return payload[:len(payload)-1]
			}(),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name: "bodyLongerThanDecoded",
			eventPayload: func() []byte {
				payload := conductor.ErrorResponseBytes(
					pendingTestCorrelationID,
					errorCode,
					"fail",
				)
				return append(payload, 0xFF)
			}(),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "negativeMessageLength",
			setActiveCorrelation: true,
			registerPending:      true,
			eventPayload: func() []byte {
				payload := make([]byte, conductor.ErrorResponseMinBodyLength)
				buffer := atomic.Wrap(payload)
				correlationOffset := conductor.ErrorResponseOffendingCommandCorrelationIDOffset
				buffer.PutInt64(correlationOffset, pendingTestCorrelationID)
				buffer.PutInt32(conductor.ErrorResponseErrorCodeOffset, errorCode)
				buffer.PutInt32(conductor.ErrorResponseErrorMessageLengthOffset, -1)
				return payload
			}(),
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
			if testCase.registerPending {
				require.NoError(
					t,
					conductorInstance.RegisterPending(
						testCase.pendingKind,
						pendingTestCorrelationID,
						pendingTestTimeout,
					),
				)
			}
			if testCase.setActiveCorrelation {
				conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)
			}

			adapter := conductorInstance.EventsAdapterForTest()
			payload := testCase.eventPayload
			if payload == nil {
				payload = conductor.ErrorResponseBytes(
					testCase.eventCorrelationID,
					errorCode,
					"registration failed",
				)
			}
			transmitConductorEvent(t, adapter, conductor.OnError, payload)

			var count int
			if testCase.useNilPending {
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
			if testCase.wantDriverError {
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, err, &driverErr)
				require.Equal(t, pendingTestCorrelationID, driverErr.CorrelationID)
				require.Equal(t, errorCode, driverErr.ErrorCode)
				require.EqualError(
					t,
					err,
					"conductor: driver registration error: correlation=7 code=11 message="+
						testCase.wantErrorMessage,
				)
			} else {
				require.NoError(t, err)
				require.Zero(t, registration)
			}

			if testCase.wantPendingRemoved {
				_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
				require.False(t, ok)
			}
		})
	}
}

func TestDriverEventsAdapter_operationSuccess(t *testing.T) {
	type testCase struct {
		name                 string
		setActiveCorrelation bool
		eventCorrelationID   int64
		eventPayload         []byte
		receiveCorrelationID int64
		pendingKind          conductor.PendingKind
		useNilPending        bool
		wantUnhandled        int64
		wantDone             bool
		wantPendingRemoved   bool
		wantPendingRetained  bool
	}

	tests := []testCase{
		{
			name:                 "matchingActive",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "paddedBody",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			eventPayload: func() []byte {
				payload := conductor.OperationSuccessBytes(pendingTestCorrelationID)
				return append(payload, 0xFF)
			}(),
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "nonMatchingActive",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestOtherCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
		},
		{
			name:                 "nullCorrelationID",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: conductor.NullCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
		},
		{
			name:                 "nilPending",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			useNilPending:        true,
			wantUnhandled:        1,
		},
		{
			name:                 "wrongKind",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindNextSessionID,
			wantUnhandled:        1,
			wantPendingRetained:  true,
		},
		{
			name:                 "shortBody",
			eventPayload:         []byte{0x01},
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
		},
		{
			name:                 "minimumMinusOneBody",
			eventPayload:         bodyOneByteShort(conductor.OperationSuccessBodyLength),
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
		},
		{
			name:                 "addPublicationAckOnly",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindAddPublication,
			wantPendingRetained:  true,
		},
		{
			name:                 "addSubscriptionAckOnly",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindAddSubscription,
			wantPendingRetained:  true,
		},
		{
			name:                 "removePublication",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindRemovePublication,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "removeSubscription",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindRemoveSubscription,
			wantDone:             true,
			wantPendingRemoved:   true,
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
				registerPendingKindForTest(
					conductorInstance,
					testCase.pendingKind,
					pendingTestCorrelationID,
				),
			)
			if testCase.setActiveCorrelation {
				conductorInstance.SetActiveCorrelationID(pendingTestCorrelationID)
			}

			adapter := conductorInstance.EventsAdapterForTest()
			payload := testCase.eventPayload
			if payload == nil {
				payload = conductor.OperationSuccessBytes(testCase.eventCorrelationID)
			}
			transmitConductorEvent(t, adapter, conductor.OnOperationSuccess, payload)

			var count int
			if testCase.useNilPending {
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
				require.NoError(t, err)
				require.Equal(t, testCase.pendingKind, registration.Kind)
			} else {
				require.NoError(t, err)
				require.Zero(t, registration)
			}

			if testCase.wantPendingRetained {
				_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
				require.True(t, ok)
				require.Equal(
					t,
					conductor.NullCorrelationID,
					conductorInstance.ReceivedCorrelationID(),
				)
			}
			if testCase.wantPendingRemoved {
				_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
				require.False(t, ok)
			}
		})
	}
}

func TestDriverEventsAdapter_publicationReady_matchingCorrelation(t *testing.T) {
	tests := []struct {
		name      string
		msgTypeID int
	}{
		{name: "publication", msgTypeID: conductor.OnPublicationReady},
		{name: "exclusive", msgTypeID: conductor.OnExclusivePublicationReady},
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
					conductor.PendingKindOperationSuccess,
					goldenPublicationReadyCorrelationID,
					pendingTestTimeout,
				),
			)
			conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)

			adapter := conductorInstance.EventsAdapterForTest()
			transmitConductorEvent(
				t,
				adapter,
				testCase.msgTypeID,
				conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
			)

			count := adapter.Receive(goldenPublicationReadyCorrelationID, conductorInstance)
			require.Equal(t, 1, count)
			require.Zero(t, adapter.UnhandledEventCount())

			_, latched := conductor.LastPublicationReadyForTest(conductorInstance)
			require.False(t, latched)
			require.Equal(
				t,
				goldenPublicationReadyCorrelationID,
				conductorInstance.ReceivedCorrelationID(),
			)
		})
	}
}

func TestDriverEventsAdapter_publicationReady_matchingCorrelation_paddedBody(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			goldenPublicationReadyCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := append(
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
		0xFF,
	)
	transmitConductorEvent(t, adapter, conductor.OnPublicationReady, payload)

	count := adapter.Receive(goldenPublicationReadyCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastPublicationReadyForTest(conductorInstance)
	require.False(t, latched)
}

func TestDriverEventsAdapter_publicationReady_pendingRegistryOnly(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			goldenPublicationReadyCorrelationID,
			pendingTestTimeout,
		),
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastPublicationReadyForTest(conductorInstance)
	require.False(t, latched)
	require.Equal(
		t,
		goldenPublicationReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)
}

func TestDriverEventsAdapter_publicationReady_wrongAddKind(t *testing.T) {
	testDriverEventsAdapterOnReadyWrongAddKind(
		t,
		conductor.PendingKindAddSubscription,
		goldenPublicationReadyCorrelationID,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)
}

func TestDriverEventsAdapter_subscriptionReady_wrongAddKind(t *testing.T) {
	testDriverEventsAdapterOnReadyWrongAddKind(
		t,
		conductor.PendingKindAddPublication,
		goldenSubscriptionReadyCorrelationID,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)
}

func TestDriverEventsAdapter_publicationReady_unrelatedCorrelation(t *testing.T) {
	var handlerCalled bool
	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	adapter := conductor.NewDriverEventsAdapter(
		1,
		copyReceiver,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
		conductor.DriverEventsAdapterHandlers{
			PublicationReady: func(conductor.PublicationBuffersReady) {
				handlerCalled = true
			},
		},
		nil,
	)

	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, nil)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.False(t, handlerCalled)
}

func TestDriverEventsAdapter_publicationReady_foreignPendingCorrelation(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			pendingTestOtherCorrelationID,
			pendingTestTimeout,
		),
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationReady,
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastPublicationReadyForTest(conductorInstance)
	require.False(t, latched)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestDriverEventsAdapter_publicationReady_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnPublicationReady,
		bodyOneByteShort(
			conductor.PublicationBuffersReadyFixedLength+atomic.StringASCIIHeaderLength,
		),
		goldenPublicationReadyCorrelationID,
	)
}

func TestDriverEventsAdapter_subscriptionReady_matchingCorrelation(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenSubscriptionReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	count := adapter.Receive(goldenSubscriptionReadyCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastSubscriptionReadyForTest(conductorInstance)
	require.False(t, latched)
	require.Equal(
		t,
		goldenSubscriptionReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)
}

func TestDriverEventsAdapter_subscriptionReady_unrelatedCorrelation(t *testing.T) {
	var handlerCalled bool
	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	adapter := conductor.NewDriverEventsAdapter(
		1,
		copyReceiver,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
		conductor.DriverEventsAdapterHandlers{
			SubscriptionReady: func(conductor.SubscriptionReady) {
				handlerCalled = true
			},
		},
		nil,
	)

	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, nil)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.False(t, handlerCalled)
}

func TestDriverEventsAdapter_subscriptionReady_matchingCorrelation_paddedBody(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
			pendingTestTimeout,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenSubscriptionReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := append(conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent), 0xFF)
	transmitConductorEvent(t, adapter, conductor.OnSubscriptionReady, payload)

	count := adapter.Receive(goldenSubscriptionReadyCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastSubscriptionReadyForTest(conductorInstance)
	require.False(t, latched)
}

func TestDriverEventsAdapter_subscriptionReady_foreignPendingCorrelation(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			pendingTestOtherCorrelationID,
			pendingTestTimeout,
		),
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastSubscriptionReadyForTest(conductorInstance)
	require.False(t, latched)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestDriverEventsAdapter_subscriptionReady_pendingRegistryOnly(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		conductorInstance.RegisterPending(
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
			pendingTestTimeout,
		),
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnSubscriptionReady,
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	_, latched := conductor.LastSubscriptionReadyForTest(conductorInstance)
	require.False(t, latched)
	require.Equal(
		t,
		goldenSubscriptionReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)
}

func TestDriverEventsAdapter_subscriptionReady_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnSubscriptionReady,
		bodyOneByteShort(conductor.SubscriptionReadyBodyLength),
		conductor.NullCorrelationID,
	)
}

func TestDriverEventsAdapter_availableImage(t *testing.T) {
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorForImageAdapterTest(t, lifecycle, true)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.Equal(t, goldenAvailableImageEvent, lifecycle.lastEvent)
}

func TestDriverEventsAdapter_availableImage_paddedBody(t *testing.T) {
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorForImageAdapterTest(t, lifecycle, true)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := append(
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
		0xFF,
	)
	transmitConductorEvent(t, adapter, conductor.OnAvailableImage, payload)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.Equal(t, goldenAvailableImageEvent, lifecycle.lastEvent)
}

func TestDriverEventsAdapter_availableImage_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnAvailableImage,
		bodyOneByteShort(
			conductor.AvailableImageFixedLength+atomic.StringASCIIHeaderLength,
		),
		conductor.NullCorrelationID,
	)
}

func TestDriverEventsAdapter_unavailableImage(t *testing.T) {
	lifecycle := &recordingImageLifecycle{}
	conductorInstance := newConductorForImageAdapterTest(t, lifecycle, false)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.True(t, lifecycle.closed)
	require.Equal(t, goldenUnavailableImageCorrelationID, lifecycle.lastRemovedCorrelationID)
	require.Equal(
		t,
		goldenUnavailableImageEvent.SubscriptionRegistrationID,
		lifecycle.lastSubscriptionRegistrationID,
	)
}

func TestDriverEventsAdapter_unavailableImage_paddedBody(t *testing.T) {
	lifecycle := &recordingImageLifecycle{}
	conductorInstance := newConductorForImageAdapterTest(t, lifecycle, false)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := append(
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
		0xFF,
	)
	transmitConductorEvent(t, adapter, conductor.OnUnavailableImage, payload)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
	require.True(t, lifecycle.closed)
	require.Equal(
		t,
		goldenUnavailableImageEvent.SubscriptionRegistrationID,
		lifecycle.lastSubscriptionRegistrationID,
	)
}

func TestDriverEventsAdapter_unavailableImage_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnUnavailableImage,
		bodyOneByteShort(
			conductor.UnavailableImageFixedLength+atomic.StringASCIIHeaderLength,
		),
		conductor.NullCorrelationID,
	)
}

func TestDriverEventsAdapter_publicationError(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationError,
		conductor.PublicationErrorBytes(goldenPublicationErrorEvent),
	)

	count := adapter.Receive(conductor.NullCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	errors := conductor.PublicationErrorsForTest(conductorInstance)
	require.Len(t, errors, 1)
	require.Equal(t, goldenPublicationErrorEvent, errors[0])
}

func TestDriverEventsAdapter_publicationError_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnPublicationError,
		bodyOneByteShort(
			conductor.PublicationErrorFixedLength+atomic.StringASCIIHeaderLength,
		),
		conductor.NullCorrelationID,
	)
}

func TestDriverEventsAdapter_unhandledEvent(t *testing.T) {
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
	)

	transmitConductorEvent(t, adapter, 0xFFFF, []byte{0x01, 0x02})

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

func newConductorForImageAdapterTest(
	t *testing.T,
	lifecycle *recordingImageLifecycle,
	seedLogBuffers bool,
) *conductor.Conductor {
	t.Helper()

	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	registerGoldenImageSubscription(conductorInstance)
	if seedLogBuffers {
		seedGoldenAvailableLogBuffers(t, conductorInstance)
	}
	return conductorInstance
}

func newDriverEventsAdapterForTest(
	t *testing.T,
	clientID int64,
	errorHandler func(error),
	clientTimeoutHandler func(),
) *conductor.DriverEventsAdapter {
	t.Helper()

	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	return conductor.NewDriverEventsAdapter(
		clientID,
		copyReceiver,
		errorHandler,
		clientTimeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
		nil,
	)
}

func testDriverEventsAdapterOnReadyWrongAddKind(
	t *testing.T,
	pendingKind conductor.PendingKind,
	correlationID int64,
	msgTypeID int,
	payload []byte,
) {
	t.Helper()

	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		0,
		0,
	)
	require.NoError(
		t,
		registerPendingKindForTest(conductorInstance, pendingKind, correlationID),
	)
	conductorInstance.SetActiveCorrelationID(correlationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(t, adapter, msgTypeID, payload)

	count := adapter.Receive(correlationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())

	done, registration, err := conductorInstance.PollPending(correlationID)
	require.False(t, done)
	require.Zero(t, registration)
	require.NoError(t, err)
	_, ok := conductorInstance.PendingResult(correlationID)
	require.True(t, ok)
	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func bodyOneByteShort(minimumBodyLength int) []byte {
	return make([]byte, minimumBodyLength-1)
}

func testDriverEventsAdapterEventDecodeFailure(
	t *testing.T,
	msgTypeID int,
	payload []byte,
	receiveCorrelationID int64,
) {
	t.Helper()

	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	adapter := conductor.NewDriverEventsAdapter(
		1,
		copyReceiver,
		func(error) { t.Fatal("error handler should not run") },
		func() { t.Fatal("timeout handler should not run") },
		conductor.DriverEventsAdapterHandlers{},
		nil,
	)

	transmitConductorEvent(t, adapter, msgTypeID, payload)

	count := adapter.Receive(receiveCorrelationID, nil)
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}
