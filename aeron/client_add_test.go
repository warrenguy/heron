package aeron

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_AddPublicationKind_insertsMapOnSyntheticReady(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := testCase.pollRespond(
				t,
				fixture,
				logPath,
				pollDeadline,
				nil,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.NoError(t, got.err)
			require.NotNil(t, got.value)
			require.Equal(t, correlationID, fixture.DriverProxy.NextCorrelationID()-1)
			registrationID, originalRegistrationID := addPublicationIDs(got.value)
			require.Equal(t, correlationID, registrationID)
			require.Equal(t, clientAddPublicationRegistrationID, originalRegistrationID)
			lookup, ok := testCase.lookup(client, correlationID)
			require.True(t, ok)
			require.Same(t, got.value, lookup)
			require.Equal(t, 1, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)

			_, oppositeOK := testCase.oppositeLookup(client, correlationID)
			require.False(t, oppositeOK)
		})
	}
}

func TestClient_AddPublicationKind_driverRegistrationError_failsAwait(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := pollAndRespondToAddCommandError(
				t,
				fixture,
				testCase.addMsgType,
				testCase.decodeCorrelationID,
				pollDeadline,
				11,
				testCase.driverRejectMessage,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			var registrationErr *RegistrationError
			require.ErrorAs(t, got.err, &registrationErr)
			require.Equal(t, registrationKindFor(testCase.pendingKind), registrationErr.Kind)
			require.Equal(t, correlationID, registrationErr.CorrelationID)
			require.Equal(t, int32(11), registrationErr.ErrorCode)
			require.Equal(t, testCase.driverRejectMessage, registrationErr.Message)
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(t, registrationErr.Cause, &driverErr)
			require.EqualError(
				t,
				got.err,
				fmt.Sprintf(
					"aeron: driver registration error: correlation=%d code=11 message=%s",
					correlationID,
					testCase.driverRejectMessage,
				),
			)
			require.Equal(t, 0, client.PublicationCountForTest())
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_AddPublicationKind_offerPhaseTimeout(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			nanoNs := testutil.TestStartNanoNs
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				0,
				time.Microsecond,
				dir,
			)

			pub, err := testCase.add(client)
			require.Nil(t, pub)
			requireAddPhaseTimeout(
				t,
				fixture,
				client,
				err,
				registrationKindFor(testCase.pendingKind),
				(*Client).PublicationCountForTest,
				true,
			)
			requireClientAddRingNoCommittedCommands(t, fixture.Ring)
		})
	}
}

func TestClient_AddPublicationKind_pollPhaseTimeout(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs

	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(startNs, timeout)
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				timeout,
				time.Microsecond,
				dir,
			)

			resultCh := startClientAddAsync(client, testCase.add)

			testCase.waitForCommand(t, fixture)
			clock.EnterDeadlinePhase()

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			requireAddPhaseTimeout(
				t,
				fixture,
				client,
				got.err,
				registrationKindFor(testCase.pendingKind),
				(*Client).PublicationCountForTest,
				false,
			)
		})
	}
}

func TestClient_Add_ringExhaustion_returnsRegistrationError(t *testing.T) {
	const timeout = time.Millisecond
	startNs := testutil.TestStartNanoNs

	type ringExhaustionCase struct {
		name             string
		dir              func(t *testing.T) string
		addCall          func(*Client) (any, error)
		registrationKind string
		countFn          func(*Client) int
	}

	pubCases := clientAddPublicationKindCases()
	tests := make([]ringExhaustionCase, 0, len(pubCases)+1)
	for _, pubCase := range pubCases {
		tests = append(tests, ringExhaustionCase{
			name:             pubCase.name,
			dir:              clientAddMediaDir,
			addCall:          pubCase.add,
			registrationKind: registrationKindFor(pubCase.pendingKind),
			countFn:          (*Client).PublicationCountForTest,
		})
	}
	tests = append(tests, ringExhaustionCase{
		name: "subscription",
		dir:  clientAddMediaDir,
		addCall: func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		},
		registrationKind: registrationKindSubscription,
		countFn:          (*Client).SubscriptionCountForTest,
	})

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(startNs, timeout).WithDeadlineAfterReads(4)
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				timeout,
				time.Microsecond,
				testCase.dir(t),
			)
			seedFullClientAddRing(t, fixture.Ring)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireAddRingExhaustionRegistrationError(
				t,
				fixture,
				client,
				err,
				testCase.registrationKind,
				testCase.countFn,
			)
		})
	}
}

func TestClient_AddPublicationKind_ringBackpressureRetryThenSuccess(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(0, time.Second)
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
			seedFullClientAddRing(t, fixture.Ring)

			resultCh := startClientAddAsync(client, testCase.add)

			waitForClientAddClockReads(t, clock, 4)
			resetClientAddRingEmpty(fixture.Ring)

			pollDeadline := time.Now().Add(time.Second)
			responded, _ := testCase.pollRespond(t, fixture, logPath, pollDeadline, nil)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.NoError(t, got.err)
			require.NotNil(t, got.value)
			require.Equal(t, 1, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_AddSubscription_ringBackpressureRetryThenSuccess(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Millisecond,
		dir,
	)
	seedFullClientAddRing(t, fixture.Ring)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	waitForClientAddClockReads(t, clock, 4)
	resetClientAddRingEmpty(fixture.Ring)

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.value)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddPublicationKind_terminationDuringOfferRetry(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(0, time.Second)
			dir := t.TempDir()
			client, fixture := newClientAddOfferRetryTerminationFixture(
				t,
				clock.NanoClock(),
				time.Second,
				time.Microsecond,
				dir,
			)
			seedFullClientAddRing(t, fixture.Ring)

			resultCh := startClientAddAsync(client, testCase.add)

			waitForClientAddClockReads(t, clock, clientAddReadsBeforeFirstServiceTick)
			requireAddCommandNotOnRing(t, fixture, testCase.addMsgType)

			correlationID := fixture.Conductor.ActiveCorrelationID()
			require.NotEqual(t, conductor.NullCorrelationID, correlationID)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.True(t, ok, "expected pending registration during offer-phase backpressure")

			fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
			clock.SetNanoNs(testutil.TestWorkNanoNs)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			requireDriverShutdownTimeout(t, got.err)
			_, ok = fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_AddSubscription_terminationDuringOfferRetry(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir := t.TempDir()
	client, fixture := newClientAddOfferRetryTerminationFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Microsecond,
		dir,
	)
	seedFullClientAddRing(t, fixture.Ring)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	waitForClientAddClockReads(t, clock, clientAddReadsBeforeFirstServiceTick)
	requireAddCommandNotOnRing(t, fixture, conductor.AddSubscription)

	correlationID := fixture.Conductor.ActiveCorrelationID()
	require.NotEqual(t, conductor.NullCorrelationID, correlationID)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok, "expected pending registration during offer-phase backpressure")

	fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
	clock.SetNanoNs(testutil.TestWorkNanoNs)

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
	requireDriverShutdownTimeout(t, got.err)
	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

type clientAddTerminationCase struct {
	name    string
	dir     func(t *testing.T) string
	addCall func(*Client) (any, error)
}

func clientAddTerminationCases() []clientAddTerminationCase {
	pubCases := clientAddPublicationKindCases()
	cases := make([]clientAddTerminationCase, 0, len(pubCases)+1)
	for _, pubCase := range pubCases {
		cases = append(cases, clientAddTerminationCase{
			name:    pubCase.name,
			dir:     clientAddMediaDir,
			addCall: pubCase.add,
		})
	}
	return append(cases, clientAddTerminationCase{
		name: "subscription",
		dir:  clientAddMediaDir,
		addCall: func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		},
	})
}

func TestClient_Add_terminationCause(t *testing.T) {
	for _, testCase := range clientAddTerminationCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				testCase.dir(t),
			)

			client.DoWork()
			nanoNs = testutil.TestWorkNanoNs
			fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireDriverShutdownTimeout(t, err)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_Add_clientTimeoutTerminationCause(t *testing.T) {
	for _, testCase := range clientAddTerminationCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				testCase.dir(t),
			)

			client.DoWork()
			require.NoError(
				t,
				fixture.Transmit(
					conductor.OnClientTimeout,
					testutil.ClientTimeoutPayload(testutil.TestClientID),
				),
			)
			nanoNs = testutil.TestWorkNanoNs

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireClientTimeoutFromDriver(t, err)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func addLiveClientSubscription(
	t *testing.T,
	client *Client,
	fixture *testutil.ConductorFixture,
) *Subscription {
	t.Helper()

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID),
	)
	sub, done, err := client.PollAddSubscription(correlationID)
	require.NoError(t, err)
	require.True(t, done)
	require.NotNil(t, sub)
	return sub
}

func TestClient_terminateClosesResourceView(t *testing.T) {
	var nanoNs int64
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	sub := addLiveClientSubscription(t, client, fixture)
	pub := addLiveClientPublication(t, client, fixture, logPath)

	require.False(t, client.IsTerminating())
	require.False(t, sub.IsClosed())
	require.False(t, pub.IsClosed())

	beforeTypeIDs := testutil.CommittedRecordTypeIDs(fixture.Ring)
	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnClientTimeout,
			testutil.ClientTimeoutPayload(testutil.TestClientID),
		),
	)
	require.Equal(t, 1, client.DoWork())

	require.True(t, client.IsTerminating())
	require.ErrorIs(t, fixture.Conductor.TerminationCause(), conductor.ErrClientTimeout)
	require.True(t, sub.IsClosed())
	require.True(t, pub.IsClosed())
	require.Equal(t, Closed, pub.Offer([]byte{1}))
	require.Equal(t, 0, client.PublicationCountForTest())
	require.Equal(t, 0, client.SubscriptionCountForTest())
	require.Equal(
		t,
		beforeTypeIDs,
		testutil.CommittedRecordTypeIDs(fixture.Ring),
		"terminate must not offer REMOVE",
	)
}

func TestClient_AddSubscription_insertsMapOnSyntheticReady(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	sub, ok := got.value.(*Subscription)
	require.True(t, ok)
	require.NotNil(t, sub)
	require.Equal(t, correlationID, sub.RegistrationID())
	lookup, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Same(t, sub, lookup)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_driverRegistrationError_failsAwait(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddCommandError(
		t,
		fixture,
		conductor.AddSubscription,
		decodeAddSubscriptionCorrelationID,
		pollDeadline,
		11,
		"add subscription rejected",
	)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
	var registrationErr *RegistrationError
	require.ErrorAs(t, got.err, &registrationErr)
	require.Equal(t, registrationKindSubscription, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.Equal(t, int32(11), registrationErr.ErrorCode)
	require.Equal(t, "add subscription rejected", registrationErr.Message)
	var driverErr *conductor.DriverRegistrationError
	require.ErrorAs(t, registrationErr.Cause, &driverErr)
	require.EqualError(
		t,
		got.err,
		fmt.Sprintf(
			"aeron: driver registration error: correlation=%d code=11 message=add subscription rejected",
			correlationID,
		),
	)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_offerPhaseTimeout(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		0,
		time.Microsecond,
		dir,
	)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		err,
		registrationKindSubscription,
		(*Client).SubscriptionCountForTest,
		true,
	)
	requireClientAddRingNoCommittedCommands(t, fixture.Ring)
}

func TestClient_AddSubscription_pollPhaseTimeout(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	clock := newClientAddTestClock(startNs, timeout)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		timeout,
		time.Microsecond,
		dir,
	)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	waitForAddSubscriptionCommand(t, fixture)
	clock.EnterDeadlinePhase()

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		got.err,
		registrationKindSubscription,
		(*Client).SubscriptionCountForTest,
		false,
	)
}

func TestClient_Add_registerPendingFailures(t *testing.T) {
	const pendingCorrelationID int64 = 0

	tests := []struct {
		name    string
		setup   func(t *testing.T, fixture *testutil.ConductorFixture, setNanoNs func(int64))
		addCall func(client *Client) error
		wantErr error
		wantMsg string
	}{
		{
			name:  "publicationConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddPublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name:  "exclusivePublicationConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name:  "subscriptionConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name: "publicationDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddPublication,
					pendingCorrelationID,
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddPublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
		{
			name: "exclusivePublicationDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddExclusivePublication,
					pendingCorrelationID,
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
		{
			name: "subscriptionDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddSubscription,
					pendingCorrelationID,
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			testCase.setup(t, fixture, func(value int64) { nanoNs = value })

			err := testCase.addCall(client)
			require.ErrorIs(t, err, testCase.wantErr)
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestClient_Add_duplicatePendingAtNullCorrelationID_preservesExisting(t *testing.T) {
	const existingCorrelationID int64 = 0

	runCase := func(
		t *testing.T,
		pendingKind conductor.PendingKind,
		add func(*Client) (any, error),
	) {
		t.Helper()

		var nanoNs int64
		client, fixture := newClientAddFixture(
			t,
			func() int64 { return nanoNs },
			time.Second,
			time.Millisecond,
			t.TempDir(),
		)
		requireRegisterClientAddPending(t, fixture, pendingKind, existingCorrelationID)

		_, err := add(client)
		require.ErrorIs(t, err, ErrPendingAlreadyRegistered)
		require.EqualError(t, err, "aeron: correlation already pending")

		_, ok := fixture.Conductor.PendingResult(existingCorrelationID)
		require.True(t, ok)
		requireActiveCorrelationCleared(t, fixture)
	}

	for _, pubCase := range clientAddPublicationKindCases() {
		t.Run(pubCase.name, func(t *testing.T) {
			runCase(t, pubCase.pendingKind, pubCase.add)
		})
	}
	t.Run("subscription", func(t *testing.T) {
		runCase(t, conductor.PendingKindAddSubscription, func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		})
	})
}

func TestClient_OfferAdd_dupRegisterFailurePreservesPendingAtZero(t *testing.T) {
	const (
		existingCorrelationID  int64 = 0
		conflictingCorrelation int64 = 1
	)

	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	require.Equal(t, existingCorrelationID, correlationID)
	_, ok := fixture.Conductor.PendingResult(existingCorrelationID)
	require.True(t, ok)

	requireRegisterClientAddPending(
		t,
		fixture,
		conductor.PendingKindAddSubscription,
		conflictingCorrelation,
	)

	_, err = client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.ErrorIs(t, err, ErrPendingAlreadyRegistered)
	require.EqualError(t, err, "aeron: correlation already pending")

	_, ok = fixture.Conductor.PendingResult(existingCorrelationID)
	require.True(t, ok)
	_, ok = fixture.Conductor.PendingResult(conflictingCorrelation)
	require.True(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AwaitDrainForTest_emptyClient(t *testing.T) {
	var nanoNs int64
	client, _ := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)

	require.NoError(t, client.AwaitDrainForTest(time.Second))
}

func TestClient_Add_nilDriverProxy(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	_, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	client := NewClientForTestWithDriverProxy(
		fixture.Conductor,
		nil,
		fixture.NanoClock,
		testutil.TestClientID,
		time.Second,
		time.Millisecond,
	)

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientNotReady)

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientNotReady)
}

func TestClient_Add_registerPendingSharedDeadline(t *testing.T) {
	const timeout = 500 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	deadlineNs := startNs + timeout.Nanoseconds()
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return startNs },
		timeout,
		time.Millisecond,
		dir,
	)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	correlationID := waitForAddSubscriptionCommand(t, fixture)
	registration, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok)
	require.Equal(t, deadlineNs, registration.DeadlineNs)

	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnSubscriptionReady,
			conductor.SubscriptionReadyBytes(
				clientAddSubscriptionReadyEvent(correlationID),
			),
		),
	)

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.value)
}

func TestClient_AddPublicationKind_pollPhaseLogMapFailure(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir := t.TempDir()
			badLogPath := filepath.Join(dir, "missing.log")
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := testCase.pollRespond(
				t,
				fixture,
				badLogPath,
				pollDeadline,
				nil,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			var registrationErr *RegistrationError
			require.ErrorAs(t, got.err, &registrationErr)
			require.Equal(t, registrationKindFor(testCase.pendingKind), registrationErr.Kind)
			require.Equal(t, correlationID, registrationErr.CorrelationID)
			require.ErrorIs(t, got.err, conductor.ErrLogBufferMapFailed)
			require.EqualError(
				t,
				got.err,
				fmt.Sprintf(
					"aeron: %s registration error: correlation=%d: conductor: failed to map log buffer",
					testCase.registrationErrorLabel,
					correlationID,
				),
			)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_addPublicationKind(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			correlationID := client.NextCorrelationID()
			deadlineNs := client.nanoClock() + time.Second.Nanoseconds()
			require.NoError(
				t,
				fixture.Conductor.RegisterPendingAddWithDeadline(
					testCase.pendingKind,
					correlationID,
					clientAddStreamID,
					clientAddChannel,
					deadlineNs,
				),
			)
			testCase.signalReady(
				fixture.Conductor,
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
			require.Equal(t, 1, client.PublicationCountForTest())

			_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
				client.PublishClosedForTest()
			})
			require.True(t, completed)
			requireErrClientClosed(t, err)
			require.Equal(t, 0, client.PublicationCountForTest())
		})
	}
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_addSubscription(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	correlationID := client.NextCorrelationID()
	deadlineNs := client.nanoClock() + time.Second.Nanoseconds()
	require.NoError(
		t,
		fixture.Conductor.RegisterPendingAddWithDeadline(
			conductor.PendingKindAddSubscription,
			correlationID,
			clientAddStreamID,
			clientAddChannel,
			deadlineNs,
		),
	)
	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID),
	)
	require.Equal(t, 1, client.SubscriptionCountForTest())

	_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
		client.PublishClosedForTest()
	})
	require.True(t, completed)
	requireErrClientClosed(t, err)
	require.Equal(t, 0, client.SubscriptionCountForTest())
}

func newGuardClosedBeforeOfferClient(t *testing.T) (*Client, *testutil.ConductorFixture) {
	t.Helper()
	var nanoNs int64
	return newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)
}

func TestClient_GuardClosedBeforeOfferForTest_proxyMismatch(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)
	otherProxy := conductor.NewDriverProxy(fixture.Ring, testutil.TestClientID+1)

	err := client.GuardClosedBeforeOfferForTest(otherProxy, false)
	require.ErrorIs(t, err, ErrClientNotReady)
	require.EqualError(t, err, ErrClientNotReady.Error())
}

func TestClient_GuardClosedBeforeOfferForTest_closedFlag(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)
	client.PublishClosedForTest()

	err := client.GuardClosedBeforeOfferForTest(fixture.DriverProxy, false)
	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
}

func TestClient_GuardClosedBeforeOfferForTest_staleMappedCncFlag(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)

	err := client.GuardClosedBeforeOfferForTest(fixture.DriverProxy, true)
	require.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_ExclusivePublication_lookup_wrongTypeHandle(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	const registrationID int64 = 42
	conductor.RegisterPublicationForTest(
		fixture.Conductor,
		registrationID,
		stubPublicationForTest{registrationID: registrationID},
	)

	pub, ok := client.ExclusivePublication(registrationID)
	require.Nil(t, pub)
	require.False(t, ok)
}

func TestClient_Publication_lookup_wrongTypeHandle(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	const registrationID int64 = 42
	conductor.RegisterPublicationForTest(
		fixture.Conductor,
		registrationID,
		stubPublicationForTest{registrationID: registrationID},
	)

	pub, ok := client.Publication(registrationID)
	require.Nil(t, pub)
	require.False(t, ok)
}

type clientAddWrongSubscriptionHandle struct {
	registrationID int64
	streamID       int32
}

func (handle clientAddWrongSubscriptionHandle) RegistrationID() int64 {
	return handle.registrationID
}

func (handle clientAddWrongSubscriptionHandle) StreamID() int32 {
	return handle.streamID
}

func TestClient_Subscription_lookup_wrongTypeHandle(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	const registrationID int64 = 42
	conductor.RegisterSubscriptionForTest(
		fixture.Conductor,
		registrationID,
		clientAddWrongSubscriptionHandle{
			registrationID: registrationID,
			streamID:       clientAddStreamID,
		},
	)

	sub, ok := client.Subscription(registrationID)
	require.Nil(t, sub)
	require.False(t, ok)
}

func TestClient_Add_lookup_miss(t *testing.T) {
	tests := []struct {
		name   string
		lookup func(*Client) (any, bool)
	}{
		{
			name: "publication",
			lookup: func(client *Client) (any, bool) {
				pub, ok := client.Publication(clientAddPublicationRegistrationID)
				return pub, ok
			},
		},
		{
			name: "exclusivePublication",
			lookup: func(client *Client) (any, bool) {
				pub, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
				return pub, ok
			},
		},
		{
			name: "subscription",
			lookup: func(client *Client) (any, bool) {
				sub, ok := client.Subscription(99)
				return sub, ok
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, _ := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				t.TempDir(),
			)

			got, ok := testCase.lookup(client)
			require.Nil(t, got)
			require.False(t, ok)
		})
	}
}

func TestClient_Add_nonASCIIChannel(t *testing.T) {
	channels := []struct {
		name    string
		channel string
	}{
		{name: "high byte", channel: "aeron:\x80"},
		{name: "utf8 multibyte", channel: "\u00fc"},
	}
	tests := []struct {
		name    string
		addCall func(*Client, string) (any, error)
	}{
		{
			name: "publication",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddPublication(clientAddStreamID, channel)
			},
		},
		{
			name: "exclusivePublication",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddExclusivePublication(clientAddStreamID, channel)
			},
		},
		{
			name: "subscription",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddSubscription(clientAddStreamID, channel)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			for _, channelTest := range channels {
				t.Run(channelTest.name, func(t *testing.T) {
					var nanoNs int64
					client, fixture := newClientAddFixture(
						t,
						func() int64 { return nanoNs },
						time.Second,
						time.Millisecond,
						t.TempDir(),
					)

					got, err := testCase.addCall(client, channelTest.channel)
					require.Nil(t, got)
					require.EqualError(t, err, "aeron: channel must be ASCII")
					requireClientAddRingNoCommittedCommands(t, fixture.Ring)
					correlationID := fixture.DriverProxy.NextCorrelationID() - 1
					_, ok := fixture.Conductor.PendingResult(correlationID)
					require.False(t, ok)
				})
			}
		})
	}
}

func TestClient_Add_emptyChannel(t *testing.T) {
	tests := []struct {
		name    string
		addCall func(*Client) (any, error)
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, "")
			},
		},
		{
			name: "exclusivePublication",
			addCall: func(client *Client) (any, error) {
				return client.AddExclusivePublication(clientAddStreamID, "")
			},
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, "")
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, _ := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				t.TempDir(),
			)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			require.EqualError(t, err, "aeron: channel is required")
		})
	}
}

func TestClient_AddPublication_refcountsLog(t *testing.T) {
	var nanoNs int64
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddPublication(clientAddStreamID, clientAddChannel)
	})

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		logPath,
		pollDeadline,
		nil,
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddPublication client result",
	)
	require.NoError(t, got.err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestClient_closedAddAndLookupPaths(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	client, _ := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	require.NoError(t, client.Close())

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientClosed)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientClosed)

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
	require.ErrorIs(t, err, ErrClientClosed)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupSub, ok := client.Subscription(99)
	require.Nil(t, lookupSub)
	require.False(t, ok)

	lookupExclusive, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupExclusive)
	require.False(t, ok)
}

func TestHandleFromPending_wrongHandle(t *testing.T) {
	tests := []struct {
		name    string
		run     func(conductor.PendingRegistration) (any, error)
		wantMsg string
	}{
		{
			name: "publicationNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return publicationFromPending(pending)
			},
			wantMsg: "aeron: publication handle is nil",
		},
		{
			name: "publicationWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Publication = "not-a-publication"
				return publicationFromPending(pending)
			},
			wantMsg: "aeron: publication handle string is not *Publication",
		},
		{
			name: "exclusivePublicationNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return exclusivePublicationFromPending(pending)
			},
			wantMsg: "aeron: exclusive publication handle is nil",
		},
		{
			name: "exclusivePublicationWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Publication = "not-an-exclusive-publication"
				return exclusivePublicationFromPending(pending)
			},
			wantMsg: "aeron: exclusive publication handle string is not *ExclusivePublication",
		},
		{
			name: "subscriptionNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return subscriptionFromPending(pending)
			},
			wantMsg: "aeron: subscription handle is nil",
		},
		{
			name: "subscriptionWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Subscription = "not-a-subscription"
				return subscriptionFromPending(pending)
			},
			wantMsg: "aeron: subscription handle string is not *Subscription",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testCase.run(conductor.PendingRegistration{})
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestClient_nilAddAndLookupPaths(t *testing.T) {
	var client *Client

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientNotReady)

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientNotReady)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupExclusive, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupExclusive)
	require.False(t, ok)

	lookupSub, ok := client.Subscription(99)
	require.Nil(t, lookupSub)
	require.False(t, ok)
}

func TestMapAddPollPendingError_ctorFailure(t *testing.T) {
	cause := errors.New("aeron: map log file failed")
	const correlationID int64 = 1001
	mapped := MapAddPollPendingErrorForTest(
		registrationKindPublication,
		correlationID,
		clientAddStreamID,
		clientAddChannel,
		cause,
	)
	var registrationErr *RegistrationError
	require.ErrorAs(t, mapped, &registrationErr)
	require.Equal(t, registrationKindPublication, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.ErrorIs(t, mapped, cause)
	require.EqualError(
		t,
		mapped,
		"aeron: publication registration error: correlation=1001: aeron: map log file failed",
	)
}
