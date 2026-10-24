package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDriverEventsAdapter_clientTimeout_matchingClient(t *testing.T) {
	var timeoutCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func() { timeoutCalled = true },
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, testutil.ClientTimeoutPayload(42))

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
	require.True(t, timeoutCalled)
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
	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
	require.Len(t, *gotErrors, 1)
}

func TestDriverEventsAdapter_clientTimeout_otherClient(t *testing.T) {
	var timeoutCalled bool
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		func() { timeoutCalled = true },
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, testutil.ClientTimeoutPayload(99))

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
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

func TestDriverEventsAdapter_counterEvents(t *testing.T) {
	const counterID int32 = 5

	tests := []struct {
		name                 string
		msgTypeID            int
		setActiveCorrelation bool
		pendingKind          conductor.PendingKind
		eventPayload         []byte
		receiveCorrelationID int64
		useNilPending        bool
		wantUnhandled        int64
		wantDone             bool
		wantPendingRemoved   bool
	}{
		{
			name:                 "readyActive",
			msgTypeID:            conductor.OnCounterReady,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "readyRegistryOnly",
			msgTypeID:            conductor.OnCounterReady,
			pendingKind:          conductor.PendingKindAddCounter,
			receiveCorrelationID: conductor.NullCorrelationID,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "readyWrongKind",
			msgTypeID:            conductor.OnCounterReady,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindNextSessionID,
			receiveCorrelationID: pendingTestCorrelationID,
		},
		{
			name:                 "readyPaddedBody",
			msgTypeID:            conductor.OnCounterReady,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			eventPayload: paddedEventPayload(
				conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, counterID),
			),
			receiveCorrelationID: pendingTestCorrelationID,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		{
			name:                 "readyShortBody",
			msgTypeID:            conductor.OnCounterReady,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			eventPayload:         bodyOneByteShort(conductor.CounterUpdateBodyLength),
			receiveCorrelationID: pendingTestCorrelationID,
			wantUnhandled:        1,
		},
		{
			name:                 "readyNilPending",
			msgTypeID:            conductor.OnCounterReady,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			receiveCorrelationID: pendingTestCorrelationID,
			useNilPending:        true,
		},
		{
			name:                 "unavailableWellFormed",
			msgTypeID:            conductor.OnUnavailableCounter,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			receiveCorrelationID: pendingTestCorrelationID,
		},
		{
			name:                 "unavailableShortBody",
			msgTypeID:            conductor.OnUnavailableCounter,
			setActiveCorrelation: true,
			pendingKind:          conductor.PendingKindAddCounter,
			eventPayload:         bodyOneByteShort(conductor.CounterUpdateBodyLength),
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
				payload = conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, counterID)
			}
			transmitConductorEvent(t, adapter, testCase.msgTypeID, payload)

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
			require.NoError(t, err)
			require.Equal(t, testCase.wantDone, done)
			if testCase.wantDone {
				require.True(t, registration.Ready)
				require.Equal(t, counterID, registration.Int32Value)
			} else {
				require.Zero(t, registration)
			}

			_, ok := conductorInstance.PendingResult(pendingTestCorrelationID)
			require.Equal(t, !testCase.wantPendingRemoved, ok)
		})
	}
}

func TestDriverEventsAdapter_counterReady_foreign(t *testing.T) {
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
			conductor.PendingKindNextSessionID,
			pendingTestOtherCorrelationID,
		),
	)
	conductorInstance.SetActiveCorrelationID(pendingTestOtherCorrelationID)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnCounterReady,
		conductor.CorrelationAndInt32Bytes(pendingTestCorrelationID, 3),
	)

	receiveOneDriverEvent(t, adapter, pendingTestOtherCorrelationID, conductorInstance)
	require.Zero(t, adapter.UnhandledEventCount())

	require.Equal(t, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())
	_, ok := conductorInstance.PendingResult(pendingTestOtherCorrelationID)
	require.True(t, ok)
}

func TestDriverEventsAdapter_clientTimeout_shortBody(t *testing.T) {
	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		42,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, []byte{0x01})

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
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
		testCase{
			name:                 "destinationActive",
			setActiveCorrelation: true,
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestCorrelationID,
			pendingKind:          conductor.PendingKindDestination,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		testCase{
			name:                 "destinationNotActive",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestOtherCorrelationID,
			pendingKind:          conductor.PendingKindDestination,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		testCase{
			name:                 "destinationNoActive",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: conductor.NullCorrelationID,
			pendingKind:          conductor.PendingKindDestination,
			wantDone:             true,
			wantPendingRemoved:   true,
		},
		testCase{
			name:                 "destinationNilPending",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: conductor.NullCorrelationID,
			pendingKind:          conductor.PendingKindDestination,
			useNilPending:        true,
			wantUnhandled:        1,
			wantPendingRetained:  true,
		},
		testCase{
			name:                 "operationSuccessNotActive",
			eventCorrelationID:   pendingTestCorrelationID,
			receiveCorrelationID: pendingTestOtherCorrelationID,
			pendingKind:          conductor.PendingKindOperationSuccess,
			wantUnhandled:        1,
			wantPendingRetained:  true,
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

			receiveOneDriverEvent(t, adapter, goldenPublicationReadyCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, goldenPublicationReadyCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, goldenSubscriptionReadyCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
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

	receiveOneDriverEvent(t, adapter, goldenSubscriptionReadyCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
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
	frameID := goldenPublicationErrorEvent.RegistrationID
	const foreignID int64 = 3003

	tests := []struct {
		name         string
		publications []stubPublication
		nilHandler   bool
		wantCalls    int
	}{
		{
			name:      "no publications",
			wantCalls: 0,
		},
		{
			name: "foreign original id",
			publications: []stubPublication{
				{registrationID: 10, originalRegistrationID: foreignID},
			},
			wantCalls: 0,
		},
		{
			name: "frame id in map key and registration id only",
			publications: []stubPublication{
				{registrationID: frameID, originalRegistrationID: foreignID},
			},
			wantCalls: 0,
		},
		{
			name: "one match",
			publications: []stubPublication{
				{registrationID: 10, originalRegistrationID: frameID},
			},
			wantCalls: 1,
		},
		{
			name: "two adds on one driver id plus a foreign add",
			publications: []stubPublication{
				{registrationID: 10, originalRegistrationID: frameID},
				{registrationID: 11, originalRegistrationID: frameID},
				{registrationID: 12, originalRegistrationID: foreignID},
			},
			wantCalls: 2,
		},
		{
			name: "nil handler",
			publications: []stubPublication{
				{registrationID: 10, originalRegistrationID: frameID},
			},
			nilHandler: true,
			wantCalls:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var frames []conductor.PublicationError
			errorHandler, gotErrors := collectConductorErrors()
			config := conductorTestConfig(t, errorHandler, fixedClocks{})
			if !tt.nilHandler {
				config.PublicationErrorFrameHandler = func(event conductor.PublicationError) {
					frames = append(frames, event)
				}
			}
			conductorInstance := newConductorForTest(t, config)
			for _, publication := range tt.publications {
				conductorInstance.InsertPublicationForTest(publication.registrationID, publication)
			}

			adapter := conductorInstance.EventsAdapterForTest()
			transmitConductorEvent(
				t,
				adapter,
				conductor.OnPublicationError,
				conductor.PublicationErrorBytes(goldenPublicationErrorEvent),
			)

			receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
			require.Zero(t, adapter.UnhandledEventCount())
			require.Len(t, frames, tt.wantCalls)
			for _, frame := range frames {
				require.Equal(t, goldenPublicationErrorEvent, frame)
			}
			require.Equal(t, len(tt.publications), conductorInstance.PublicationCountForTest())
			require.Empty(t, *gotErrors)
		})
	}
}

// TestDriverEventsAdapter_publicationError_handlerReentry closes both
// matching publications inside the first handler call. The handler still runs
// twice: the conductor counts matches before delivery.
func TestDriverEventsAdapter_publicationError_handlerReentry(t *testing.T) {
	frameID := goldenPublicationErrorEvent.RegistrationID
	errorHandler, gotErrors := collectConductorErrors()
	config := conductorTestConfig(t, errorHandler, fixedClocks{})

	var conductorInstance *conductor.Conductor
	var frames []conductor.PublicationError
	var lookups []bool
	var closeErrors []error
	config.PublicationErrorFrameHandler = func(event conductor.PublicationError) {
		frames = append(frames, event)
		if len(frames) == 1 {
			for _, registrationID := range []int64{10, 11} {
				publication, ok := conductorInstance.PublicationByRegID(registrationID)
				lookups = append(lookups, ok)
				if ok {
					closeErrors = append(closeErrors, conductorInstance.ClosePublication(publication))
				}
			}
		}
		panic("handler panic")
	}
	conductorInstance = newConductorForTest(t, config)
	conductorInstance.InsertPublicationForTest(
		10,
		stubPublication{registrationID: 10, originalRegistrationID: frameID},
	)
	conductorInstance.InsertPublicationForTest(
		11,
		stubPublication{registrationID: 11, originalRegistrationID: frameID},
	)

	adapter := conductorInstance.EventsAdapterForTest()
	transmitConductorEvent(
		t,
		adapter,
		conductor.OnPublicationError,
		conductor.PublicationErrorBytes(goldenPublicationErrorEvent),
	)

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, conductorInstance)
	require.Zero(t, adapter.UnhandledEventCount())
	require.Equal(t, []bool{true, true}, lookups)
	require.Equal(t, []error{nil, nil}, closeErrors)
	require.Equal(
		t,
		[]conductor.PublicationError{goldenPublicationErrorEvent, goldenPublicationErrorEvent},
		frames,
	)
	require.Len(t, *gotErrors, 2)
	for _, err := range *gotErrors {
		require.ErrorContains(t, err, "publication error frame handler panic: handler panic")
	}
	require.Zero(t, conductorInstance.PublicationCountForTest())
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
	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, unknownDriverEventTypeID, []byte{0x01, 0x02})

	receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}

func TestDriverEventsAdapter_broadcastLapped(t *testing.T) {
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		fatalDriverEventsAdapterTimeoutHandler(t),
		conductor.DriverEventsAdapterHandlers{},
	)

	scriptBroadcastLap(t, adapter)

	count, err := adapter.Receive(conductor.NullCorrelationID, nil)
	require.ErrorIs(t, err, conductor.ErrBroadcastLapped)
	require.Zero(t, count)

	// Phase: the next Receive delivers the latest record.
	count, err = adapter.Receive(conductor.NullCorrelationID, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())

	// Phase: a later transmit follows it.
	transmitConductorEvent(t, adapter, unknownDriverEventTypeID, []byte{0x01, 0x02})
	count, err = adapter.Receive(conductor.NullCorrelationID, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, int64(2), adapter.UnhandledEventCount())
}

func TestDriverEventsAdapter_unknownRegistrationHook(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int32
		payload   []byte
		wantHook  bool
	}{
		{
			name:      "unknownSubscription",
			errorCode: conductor.ErrorCodeUnknownSubscription,
			wantHook:  true,
		},
		{
			name:      "unknownPublication",
			errorCode: conductor.ErrorCodeUnknownPublication,
			wantHook:  true,
		},
		{
			name:      "otherCode",
			errorCode: 11,
		},
		{
			name:    "shortBody",
			payload: []byte{0x01},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var hookCalls int
			adapter := newDriverEventsAdapterForTest(
				t,
				1,
				fatalDriverEventsAdapterTimeoutHandler(t),
				conductor.DriverEventsAdapterHandlers{
					UnknownRegistration: func() { hookCalls++ },
				},
			)
			payload := testCase.payload
			if payload == nil {
				payload = conductor.ErrorResponseBytes(
					pendingTestCorrelationID,
					testCase.errorCode,
					"unknown registration",
				)
			}
			transmitConductorEvent(t, adapter, conductor.OnError, payload)

			receiveOneDriverEvent(t, adapter, conductor.NullCorrelationID, nil)
			require.Equal(t, int64(1), adapter.UnhandledEventCount())
			if testCase.wantHook {
				require.Equal(t, 1, hookCalls)
			} else {
				require.Zero(t, hookCalls)
			}
		})
	}
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

func fatalDriverEventsAdapterTimeoutHandler(t *testing.T) func() {
	t.Helper()
	return func() { t.Fatal("timeout handler should not run") }
}

func newDriverEventsAdapterForTest(
	t *testing.T,
	clientID int64,
	clientTimeoutHandler func(),
	handlers conductor.DriverEventsAdapterHandlers,
) *conductor.DriverEventsAdapter {
	t.Helper()

	_, rx := newCopyBroadcastPair(t)
	copyReceiver := conductor.NewCopyBroadcastReceiver(rx)
	return conductor.NewDriverEventsAdapter(
		clientID,
		copyReceiver,
		clientTimeoutHandler,
		handlers,
		nil,
	)
}

// receiveOneDriverEvent runs one Receive that must dispatch one event.
func receiveOneDriverEvent(
	tb testutil.TB,
	adapter *conductor.DriverEventsAdapter,
	activeCorrelationID int64,
	pending conductor.PendingEventHandler,
) {
	tb.Helper()
	count, err := adapter.Receive(activeCorrelationID, pending)
	require.NoError(tb, err)
	require.Equal(tb, 1, count)
}

func receiveDriverEventForTest(
	t *testing.T,
	adapter *conductor.DriverEventsAdapter,
	receiveCorrelationID int64,
	conductorInstance *conductor.Conductor,
	useNilPending bool,
) int {
	t.Helper()
	var pending conductor.PendingEventHandler
	if !useNilPending {
		pending = conductorInstance
	}
	count, err := adapter.Receive(receiveCorrelationID, pending)
	require.NoError(t, err)
	return count
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

	receiveOneDriverEvent(t, adapter, correlationID, conductorInstance)
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

	timeoutHandler := fatalDriverEventsAdapterTimeoutHandler(t)
	adapter := newDriverEventsAdapterForTest(
		t,
		1,
		timeoutHandler,
		conductor.DriverEventsAdapterHandlers{},
	)

	transmitConductorEvent(t, adapter, msgTypeID, payload)

	receiveOneDriverEvent(t, adapter, receiveCorrelationID, nil)
	require.Equal(t, int64(1), adapter.UnhandledEventCount())
}
