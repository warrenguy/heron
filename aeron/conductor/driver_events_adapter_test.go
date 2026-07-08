package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
)

func TestDriverEventsAdapter_clientTimeout_matchingClient(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	var timeoutCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		errorHandler,
		func() { timeoutCalled = true },
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, testutil.ClientTimeoutPayload(42))

	count := adapter.Receive(conductor.NullCorrelationID, nil)
	require.Equal(t, 1, count)
	require.True(t, timeoutCalled)
	require.Empty(t, *gotErrors)
}

func TestDriverEventsAdapter_duplicateClientTimeoutIgnored_adapterLevel(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)
	adapter := conductorInstance.EventsAdapterForTest()
	timeoutPayload := testutil.ClientTimeoutPayload(conductorTestClientID)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, timeoutPayload)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.True(t, conductorInstance.IsTerminating())

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, timeoutPayload)
	require.Equal(t, 1, adapter.Receive(conductor.NullCorrelationID, nil))
	require.Len(t, *gotErrors, 1)
}

func TestDriverEventsAdapter_clientTimeout_otherClient(t *testing.T) {
	errorHandler := fatalDriverEventsAdapterErrorHandler(t)
	var timeoutCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		errorHandler,
		func() { timeoutCalled = true },
		conductor.DriverEventsAdapterHandlers{},
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
			eventPayload: paddedEventPayload(
				conductor.CorrelationAndInt32Bytes(
					pendingTestCorrelationID,
					nextSessionID,
				),
			),
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

			count := receiveDriverEventForTest(
				t,
				adapter,
				testCase.receiveCorrelationID,
				conductorInstance,
				testCase.useNilPending,
			)
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
	errorHandler := fatalDriverEventsAdapterErrorHandler(t)
	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		errorHandler,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
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
		registerOtherPending bool
		pendingKind          conductor.PendingKind
		eventCorrelationID   int64
		eventPayload         []byte
		receiveCorrelationID int64
		pollCorrelationID    int64
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
			name:                 "pendingRegistryNonActive",
			setActiveCorrelation: true,
			registerPending:      true,
			registerOtherPending: true,
			pendingKind:          conductor.PendingKindNextSessionID,
			eventCorrelationID:   pendingTestOtherCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pollCorrelationID:    pendingTestOtherCorrelationID,
			wantDone:             true,
			wantDriverError:      true,
			wantErrorMessage:     "registration failed",
			wantPendingRemoved:   true,
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
				buffer := atomicbuffer.Wrap(payload)
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
			eventPayload: paddedEventPayload(
				conductor.ErrorResponseBytes(
					pendingTestCorrelationID,
					errorCode,
					"fail",
				),
			),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "negativeMessageLength",
			setActiveCorrelation: true,
			registerPending:      true,
			eventPayload: func() []byte {
				payload := make([]byte, conductor.ErrorResponseMinBodyLength)
				buffer := atomicbuffer.Wrap(payload)
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
			registerErrorPending := func(correlationID int64) {
				t.Helper()
				require.NoError(
					t,
					registerPendingKindForTest(
						conductorInstance,
						testCase.pendingKind,
						correlationID,
					),
				)
			}
			if testCase.registerPending {
				registerErrorPending(pendingTestCorrelationID)
			}
			if testCase.registerOtherPending {
				registerErrorPending(pendingTestOtherCorrelationID)
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

			count := receiveDriverEventForTest(
				t,
				adapter,
				testCase.receiveCorrelationID,
				conductorInstance,
				testCase.useNilPending,
			)
			require.Equal(t, 1, count)
			require.Equal(t, testCase.wantUnhandled, adapter.UnhandledEventCount())

			pollCorrelationID := testCase.pollCorrelationID
			if pollCorrelationID == 0 {
				pollCorrelationID = pendingTestCorrelationID
			}
			done, registration, err := conductorInstance.PollPending(pollCorrelationID)
			require.Equal(t, testCase.wantDone, done)
			if testCase.wantDriverError {
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(t, err, &driverErr)
				require.Equal(t, pollCorrelationID, driverErr.CorrelationID)
				require.Equal(t, errorCode, driverErr.ErrorCode)
				require.Equal(t, testCase.wantErrorMessage, driverErr.Message)
				require.EqualError(t, err, (&conductor.DriverRegistrationError{
					CorrelationID: pollCorrelationID,
					ErrorCode:     errorCode,
					Message:       testCase.wantErrorMessage,
				}).Error())
			} else {
				require.NoError(t, err)
				require.Zero(t, registration)
			}

			if testCase.wantPendingRemoved {
				_, ok := conductorInstance.PendingResult(pollCorrelationID)
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
			eventPayload: paddedEventPayload(
				conductor.OperationSuccessBytes(pendingTestCorrelationID),
			),
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
	}
	for _, addKind := range addPendingKindCases {
		tests = append(tests, testCase{
			name:                 addKind.name + "AckOnly",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          addKind.kind,
			wantPendingRetained:  true,
		})
	}
	tests = append(
		tests,
		testCase{
			name:                 "removePublication",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindRemovePublication,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		testCase{
			name:                 "removeSubscription",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindRemoveSubscription,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
	)

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

			count := receiveDriverEventForTest(
				t,
				adapter,
				testCase.receiveCorrelationID,
				conductorInstance,
				testCase.useNilPending,
			)
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
				registerPendingKindForTest(
					conductorInstance,
					conductor.PendingKindOperationSuccess,
					goldenPublicationReadyCorrelationID,
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			goldenPublicationReadyCorrelationID,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenPublicationReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := paddedEventPayload(
		conductor.PublicationBuffersReadyBytes(goldenPublicationBuffersReadyEvent),
	)
	transmitConductorEvent(t, adapter, conductor.OnPublicationReady, payload)

	count := adapter.Receive(goldenPublicationReadyCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			goldenPublicationReadyCorrelationID,
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
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		fatalDriverEventsAdapterErrorHandler(t),
		fatalDriverEventsAdapterTimeoutHandler(t),
		conductor.DriverEventsAdapterHandlers{
			PublicationReady: func(conductor.PublicationBuffersReady) {
				handlerCalled = true
			},
		},
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			pendingTestOtherCorrelationID,
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

	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
}

func TestDriverEventsAdapter_publicationReady_decodeFailure(t *testing.T) {
	testDriverEventsAdapterEventDecodeFailure(
		t,
		conductor.OnPublicationReady,
		bodyOneByteShort(
			conductor.PublicationBuffersReadyFixedLength+atomicbuffer.StringASCIIHeaderLength,
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
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

	require.Equal(
		t,
		goldenSubscriptionReadyCorrelationID,
		conductorInstance.ReceivedCorrelationID(),
	)
}

func TestDriverEventsAdapter_subscriptionReady_unrelatedCorrelation(t *testing.T) {
	var handlerCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		fatalDriverEventsAdapterErrorHandler(t),
		fatalDriverEventsAdapterTimeoutHandler(t),
		conductor.DriverEventsAdapterHandlers{
			SubscriptionReady: func(conductor.SubscriptionReady) {
				handlerCalled = true
			},
		},
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
		),
	)
	conductorInstance.SetActiveCorrelationID(goldenSubscriptionReadyCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	payload := paddedEventPayload(
		conductor.SubscriptionReadyBytes(goldenSubscriptionReadyEvent),
	)
	transmitConductorEvent(t, adapter, conductor.OnSubscriptionReady, payload)

	count := adapter.Receive(goldenSubscriptionReadyCorrelationID, conductorInstance)
	require.Equal(t, 1, count)
	require.Zero(t, adapter.UnhandledEventCount())
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			pendingTestOtherCorrelationID,
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
		registerPendingKindForTest(
			conductorInstance,
			conductor.PendingKindOperationSuccess,
			goldenSubscriptionReadyCorrelationID,
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
	payload := paddedEventPayload(
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
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
			conductor.AvailableImageFixedLength+atomicbuffer.StringASCIIHeaderLength,
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
	payload := paddedEventPayload(
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
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
			conductor.UnavailableImageFixedLength+atomicbuffer.StringASCIIHeaderLength,
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
			conductor.PublicationErrorFixedLength+atomicbuffer.StringASCIIHeaderLength,
		),
		conductor.NullCorrelationID,
	)
}

func TestDriverEventsAdapter_unhandledEvent(t *testing.T) {
	errorHandler := fatalDriverEventsAdapterErrorHandler(t)
	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		errorHandler,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
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
		conductor.DriverEventsAdapterHandlers{},
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

func fatalDriverEventsAdapterErrorHandler(t *testing.T) func(error) {
	t.Helper()
	return func(error) { t.Fatal("error handler should not run") }
}

func fatalDriverEventsAdapterTimeoutHandler(t *testing.T) func() {
	t.Helper()
	return func() { t.Fatal("timeout handler should not run") }
}

func newDriverEventsAdapterForTest(
	t *testing.T,
	clientID int64,
	errorHandler func(error),
	clientTimeoutHandler func(),
	handlers conductor.DriverEventsAdapterHandlers,
) *conductor.DriverEventsAdapter {
	t.Helper()

	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	return conductor.NewDriverEventsAdapter(
		clientID,
		copyReceiver,
		errorHandler,
		clientTimeoutHandler,
		handlers,
		nil,
	)
}

func receiveDriverEventForTest(
	t *testing.T,
	adapter *conductor.DriverEventsAdapter,
	receiveCorrelationID int64,
	conductorInstance *conductor.Conductor,
	useNilPending bool,
) int {
	t.Helper()
	if useNilPending {
		return adapter.Receive(receiveCorrelationID, nil)
	}
	return adapter.Receive(receiveCorrelationID, conductorInstance)
}

func paddedEventPayload(payload []byte) []byte {
	return append(payload, 0xFF)
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

	errorHandler := fatalDriverEventsAdapterErrorHandler(t)
	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		errorHandler,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, msgTypeID, payload)

	count := adapter.Receive(receiveCorrelationID, nil)
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}
