package aeron

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const (
	clientOfferAddMember1RegistrationID     int64 = 2003
	clientOfferAddSubscriptionRejectCode    int32 = 11
	clientOfferAddSubscriptionRejectMessage       = "add subscription rejected"
)

func newClientOfferAddTestFixture(
	t *testing.T,
	mediaDriverDir string,
) (*Client, *testutil.ConductorFixture) {
	t.Helper()

	return newClientAddFixture(
		t,
		func() int64 { return 0 },
		time.Second,
		time.Millisecond,
		mediaDriverDir,
	)
}

func clientOfferAddMultiPublicationReadyEvent(
	correlationID int64,
	logPath string,
) conductor.PublicationBuffersReady {
	ready := clientAddPublicationReadyEvent(correlationID, logPath)
	ready.RegistrationID = clientOfferAddMember1RegistrationID
	ready.PublicationLimitCounterID = 2
	ready.ChannelStatusCounterID = 3
	return ready
}

func requireDriverRegistrationRejected(
	t *testing.T,
	err error,
	registrationKind string,
	correlationID int64,
	errorCode int32,
	message string,
) {
	t.Helper()

	var registrationErr *RegistrationError
	require.ErrorAs(t, err, &registrationErr)
	require.Equal(t, registrationKind, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.Equal(t, errorCode, registrationErr.ErrorCode)
	require.Equal(t, message, registrationErr.Message)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"aeron: driver registration error: correlation=%d code=%d message=%s",
			correlationID,
			errorCode,
			message,
		),
	)
}

func TestClient_OfferAddPublicationKind_pollUntilReady(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			correlationID, err := testCase.offerAdd(client)
			require.NoError(t, err)
			require.NotEqual(t, conductor.NullCorrelationID, correlationID)

			pub := pollAddUntilReady(
				t,
				client,
				correlationID,
				func(t *testing.T, deadline time.Time) {
					_, _ = testCase.pollRespond(t, fixture, logPath, deadline, nil)
				},
				testCase.pollAdd,
				testCase.pollTimeoutMsg,
			)
			registrationID, originalRegistrationID := addPublicationIDs(pub)
			require.Equal(t, correlationID, registrationID)
			require.Equal(t, clientAddPublicationRegistrationID, originalRegistrationID)
			lookup, ok := testCase.lookup(client, correlationID)
			require.True(t, ok)
			require.Same(t, pub, lookup)
			require.Equal(t, 1, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_OfferAddSubscription_pollUntilReady(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	require.NotEqual(t, conductor.NullCorrelationID, correlationID)

	sub := pollAddUntilReady(
		t,
		client,
		correlationID,
		func(t *testing.T, deadline time.Time) {
			_, _ = pollAndRespondToAddSubscriptionCommand(t, fixture, deadline, nil)
		},
		(*Client).PollAddSubscription,
		"timed out waiting for PollAddSubscription",
	)
	require.Equal(t, correlationID, sub.RegistrationID())
	lookup, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Same(t, sub, lookup)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_multiPendingSameTick_subscription(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID0, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	correlationID1, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	require.NotEqual(t, correlationID0, correlationID1)

	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID0),
	)
	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID1),
	)

	sub0, done0, err := client.PollAddSubscription(correlationID0)
	require.True(t, done0)
	require.NoError(t, err)
	require.NotNil(t, sub0)
	require.Equal(t, correlationID0, sub0.RegistrationID())

	sub1, done1, err := client.PollAddSubscription(correlationID1)
	require.True(t, done1)
	require.NoError(t, err)
	require.NotNil(t, sub1)
	require.Equal(t, correlationID1, sub1.RegistrationID())
	require.Equal(t, 2, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_multiPendingSameTick_publicationKind(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
			fixture.Conductor.SetCountersReaderForTest(
				countersReaderForClientAddPublications(
					t,
					clientAddPublicationRegistrationID,
					clientOfferAddMember1RegistrationID,
				),
			)

			correlationID0, err := testCase.offerAdd(client)
			require.NoError(t, err)
			correlationID1, err := testCase.offerAdd(client)
			require.NoError(t, err)

			testCase.signalReady(
				fixture.Conductor,
				clientAddPublicationReadyEvent(correlationID0, logPath),
			)
			testCase.signalReady(
				fixture.Conductor,
				clientOfferAddMultiPublicationReadyEvent(correlationID1, logPath),
			)

			pub0, done0, err := testCase.pollAdd(client, correlationID0)
			require.True(t, done0)
			require.NoError(t, err)
			registrationID0, originalRegistrationID0 := addPublicationIDs(pub0)
			require.Equal(t, correlationID0, registrationID0)
			require.Equal(t, clientAddPublicationRegistrationID, originalRegistrationID0)

			pub1, done1, err := testCase.pollAdd(client, correlationID1)
			require.True(t, done1)
			require.NoError(t, err)
			registrationID1, originalRegistrationID1 := addPublicationIDs(pub1)
			require.Equal(t, correlationID1, registrationID1)
			require.Equal(t, clientOfferAddMember1RegistrationID, originalRegistrationID1)
			require.Equal(t, 2, client.PublicationCountForTest())
		})
	}
}

// TestClient_AddPublication_sharedDriverPublication_closeEachAdd adds two
// publications that share one driver registration id. Each Close must act on
// its own add only.
func TestClient_AddPublication_sharedDriverPublication_closeEachAdd(t *testing.T) {
	tests := []struct {
		name       string
		closeOrder [2]int
	}{
		{name: "closeFirstAddFirst", closeOrder: [2]int{0, 1}},
		{name: "closeSecondAddFirst", closeOrder: [2]int{1, 0}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := setupClientAddPublicationLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
			registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

			var publications [2]*Publication
			for i := range publications {
				pub := addLiveClientPublication(t, client, fixture, logPath)
				require.NotEqual(t, clientAddPublicationRegistrationID, pub.RegistrationID())
				publications[i] = pub
			}
			require.Equal(
				t,
				[]int{conductor.AddPublication, conductor.AddPublication},
				testutil.CommittedRecordTypeIDs(fixture.Ring),
			)
			correlationIDs := testutil.AddPublicationCorrelationIDs(t, fixture.Ring)
			require.Equal(
				t,
				[]int64{publications[0].RegistrationID(), publications[1].RegistrationID()},
				correlationIDs,
			)
			require.NotEqual(t, correlationIDs[0], correlationIDs[1])

			// Phase: each add is keyed by its own correlation id.
			for i, pub := range publications {
				require.Equal(t, clientAddPublicationRegistrationID, pub.OriginalRegistrationID())
				require.NotEqual(t, pub.OriginalRegistrationID(), pub.RegistrationID())
				lookup, ok := client.Publication(correlationIDs[i])
				require.True(t, ok)
				require.Same(t, pub, lookup)
			}
			_, ok := client.Publication(clientAddPublicationRegistrationID)
			require.False(t, ok)
			require.Equal(t, 2, client.PublicationCountForTest())
			require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

			// Phase: each close retires its own add and releases one log reference.
			for step, closeIndex := range testCase.closeOrder {
				require.NoError(t, publications[closeIndex].Close())

				require.True(t, publications[closeIndex].IsClosed())
				require.Equal(
					t,
					[]int64{correlationIDs[closeIndex]},
					testutil.RemovePublicationRegistrationIDs(t, fixture.Ring),
				)
				_, ok := client.Publication(correlationIDs[closeIndex])
				require.False(t, ok)
				require.Equal(t, 1-step, client.PublicationCountForTest())
				require.Equal(
					t,
					int32(1-step),
					conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
				)

				// Phase: repeat Close sends nothing and releases nothing.
				require.NoError(t, publications[closeIndex].Close())
				requireClientAddRingNoCommittedCommands(t, fixture.Ring)
				require.Equal(t, 1-step, client.PublicationCountForTest())
				require.Equal(
					t,
					int32(1-step),
					conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
				)

				if step == 0 {
					openIndex := testCase.closeOrder[1]
					require.False(t, publications[openIndex].IsClosed())
					lookup, ok := client.Publication(correlationIDs[openIndex])
					require.True(t, ok)
					require.Same(t, publications[openIndex], lookup)
				}
			}
		})
	}
}

// TestClient_AddPublicationKind_closeSendsCorrelationID closes one add whose
// driver registration id differs from its correlation id.
func TestClient_AddPublicationKind_closeSendsCorrelationID(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
			registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

			correlationID, err := testCase.offerAdd(client)
			require.NoError(t, err)
			require.NotEqual(t, clientAddPublicationRegistrationID, correlationID)
			testCase.signalReady(
				fixture.Conductor,
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
			got, done, err := testCase.pollAdd(client, correlationID)
			require.True(t, done)
			require.NoError(t, err)
			pub, ok := got.(interface {
				Close() error
				IsClosed() bool
			})
			require.True(t, ok)
			require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

			require.Equal(t, []int{testCase.addMsgType}, testutil.CommittedRecordTypeIDs(fixture.Ring))
			require.Equal(
				t,
				[]int64{correlationID},
				testutil.AddPublicationCorrelationIDs(t, fixture.Ring),
			)

			require.NoError(t, pub.Close())

			require.True(t, pub.IsClosed())
			require.Equal(
				t,
				[]int64{correlationID},
				testutil.RemovePublicationRegistrationIDs(t, fixture.Ring),
			)
			_, ok = testCase.lookup(client, correlationID)
			require.False(t, ok)
			require.Zero(t, client.PublicationCountForTest())
			require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
		})
	}
}

func TestClient_PollAdd_multiPendingSameTick_registrationError(t *testing.T) {
	t.Run("subscription", func(t *testing.T) {
		testPollAddMultiPendingRegistrationError(
			t,
			t.TempDir(),
			func(client *Client) (int64, error) {
				return client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
			},
			func(client *Client, correlationID int64) (any, bool, error) {
				return client.PollAddSubscription(correlationID)
			},
			func(fixture *testutil.ConductorFixture, correlationID int64) {
				conductor.SubscriptionReadyForTest(
					fixture.Conductor,
					clientAddSubscriptionReadyEvent(correlationID),
				)
			},
			registrationKindSubscription,
			clientOfferAddSubscriptionRejectMessage,
			(*Client).SubscriptionCountForTest,
			nil,
		)
	})

	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			testPollAddMultiPendingRegistrationError(
				t,
				dir,
				testCase.offerAdd,
				testCase.pollAdd,
				func(fixture *testutil.ConductorFixture, correlationID int64) {
					testCase.signalReady(
						fixture.Conductor,
						clientAddPublicationReadyEvent(correlationID, logPath),
					)
				},
				registrationKindFor(testCase.pendingKind),
				testCase.driverRejectMessage,
				(*Client).PublicationCountForTest,
				func(fixture *testutil.ConductorFixture) {
					fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
				},
			)
		})
	}
}

func testPollAddMultiPendingRegistrationError(
	t *testing.T,
	mediaDriverDir string,
	offerAdd func(*Client) (int64, error),
	pollAdd func(*Client, int64) (any, bool, error),
	signalReady func(*testutil.ConductorFixture, int64),
	registrationKind string,
	driverRejectMessage string,
	countFn func(*Client) int,
	setupFixture func(*testutil.ConductorFixture),
) {
	t.Helper()

	client, fixture := newClientOfferAddTestFixture(t, mediaDriverDir)
	if setupFixture != nil {
		setupFixture(fixture)
	}

	correlationIDSuccess, err := offerAdd(client)
	require.NoError(t, err)
	correlationIDFail, err := offerAdd(client)
	require.NoError(t, err)

	signalReady(fixture, correlationIDSuccess)
	require.True(
		t,
		fixture.Conductor.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: correlationIDFail,
			ErrorCode:                     clientOfferAddSubscriptionRejectCode,
			ErrorMessage:                  driverRejectMessage,
		}),
	)

	resource, doneSuccess, err := pollAdd(client, correlationIDSuccess)
	require.True(t, doneSuccess)
	require.NoError(t, err)
	require.NotNil(t, resource)

	_, doneFail, err := pollAdd(client, correlationIDFail)
	require.True(t, doneFail)
	require.Error(t, err)
	requireDriverRegistrationRejected(
		t,
		err,
		registrationKind,
		correlationIDFail,
		clientOfferAddSubscriptionRejectCode,
		driverRejectMessage,
	)
	require.Equal(t, 1, countFn(client))
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_timeout(t *testing.T) {
	for _, testCase := range clientOfferAddAsyncKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testPollAddTimeout(
				t,
				testCase.offerAdd,
				testCase.poll,
				testCase.registrationKind,
				testCase.countFn,
			)
		})
	}
}

func testPollAddTimeout(
	t *testing.T,
	offer func(*Client) (int64, error),
	poll func(*Client, int64) (any, bool, error),
	registrationKind string,
	countFn func(*Client) int,
) {
	t.Helper()
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

	correlationID, err := offer(client)
	require.NoError(t, err)
	clock.EnterDeadlinePhase()

	_, done, err := poll(client, correlationID)
	require.True(t, done)
	requireRegistrationTimeoutError(t, err, registrationKind, correlationID, true)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, countFn(client))
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_clientDeadlineFallback(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	deadlineNs := startNs + timeout.Nanoseconds()
	pastDeadline := false
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 {
			if pastDeadline {
				return deadlineNs + 1
			}
			return startNs
		},
		timeout,
		time.Microsecond,
		dir,
	)

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	pastDeadline = true

	_, done, err := pollAddForTest(
		client,
		correlationID,
		subscriptionFromPending,
		true,
		nil,
	)
	require.True(t, done)
	requireRegistrationTimeoutError(t, err, registrationKindSubscription, correlationID, true)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
}

func TestClient_PollAdd_expiredWhilePollingOther(t *testing.T) {
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

	correlationIDActive, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	correlationIDExpired, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)

	_, doneBeforeDeadline, err := client.PollAddSubscription(correlationIDActive)
	require.False(t, doneBeforeDeadline)
	require.NoError(t, err)

	clock.EnterDeadlinePhase()
	_, doneActive, err := client.PollAddSubscription(correlationIDActive)
	require.True(t, doneActive)
	requireRegistrationTimeoutError(t, err, registrationKindSubscription, correlationIDActive, true)

	_, doneExpired, err := client.PollAddSubscription(correlationIDExpired)
	require.True(t, doneExpired)
	requireRegistrationTimeoutError(t, err, registrationKindSubscription, correlationIDExpired, true)
	_, ok := fixture.Conductor.PendingResult(correlationIDExpired)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_succeededUnpolledSurvivesExpire_multiPending(t *testing.T) {
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

	correlationID0, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	correlationID1, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)

	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID0),
	)

	clock.EnterDeadlinePhase()

	_, done1, err := client.PollAddSubscription(correlationID1)
	require.True(t, done1)
	requireRegistrationTimeoutError(t, err, registrationKindSubscription, correlationID1, true)
	_, ok := fixture.Conductor.PendingResult(correlationID1)
	require.False(t, ok)

	sub0, done0, err := client.PollAddSubscription(correlationID0)
	require.True(t, done0)
	require.NoError(t, err)
	require.NotNil(t, sub0)
	require.Equal(t, correlationID0, sub0.RegistrationID())
	require.Equal(t, 1, client.SubscriptionCountForTest())
	_, ok = fixture.Conductor.PendingResult(correlationID0)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_driverErrorForNonActivePending_viaDoWork(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationIDActive, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	correlationIDFail, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)

	require.NoError(t, fixture.Transmit(
		conductor.OnError,
		conductor.ErrorResponseBytes(
			correlationIDFail,
			clientOfferAddSubscriptionRejectCode,
			clientOfferAddSubscriptionRejectMessage,
		),
	))

	_, doneActive, err := client.PollAddSubscription(correlationIDActive)
	require.False(t, doneActive)
	require.NoError(t, err)

	_, doneFail, err := client.PollAddSubscription(correlationIDFail)
	require.True(t, doneFail)
	require.Error(t, err)
	requireDriverRegistrationRejected(
		t,
		err,
		registrationKindSubscription,
		correlationIDFail,
		clientOfferAddSubscriptionRejectCode,
		clientOfferAddSubscriptionRejectMessage,
	)
	_, ok := fixture.Conductor.PendingResult(correlationIDFail)
	require.False(t, ok)
	_, ok = fixture.Conductor.PendingResult(correlationIDActive)
	require.True(t, ok)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_driverErrorSurvivesExpire_multiPending(t *testing.T) {
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

	correlationIDSuccess, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	correlationIDFail, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)

	require.True(
		t,
		fixture.Conductor.DriverError(conductor.ErrorResponse{
			OffendingCommandCorrelationID: correlationIDFail,
			ErrorCode:                     clientOfferAddSubscriptionRejectCode,
			ErrorMessage:                  clientOfferAddSubscriptionRejectMessage,
		}),
	)

	clock.EnterDeadlinePhase()

	_, doneFail, err := client.PollAddSubscription(correlationIDFail)
	require.True(t, doneFail)
	require.Error(t, err)
	requireDriverRegistrationRejected(
		t,
		err,
		registrationKindSubscription,
		correlationIDFail,
		clientOfferAddSubscriptionRejectCode,
		clientOfferAddSubscriptionRejectMessage,
	)
	require.NotErrorIs(t, err, conductor.ErrAwaitTimeout)
	require.Equal(t, 1, client.SubscriptionCountForTest())

	_, ok := fixture.Conductor.PendingResult(correlationIDFail)
	require.False(t, ok)
	_, ok = fixture.Conductor.PendingResult(correlationIDSuccess)
	require.True(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_closedClient(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID, err := client.OfferAddPublication(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	require.NoError(t, client.Close())

	pub, done, err := client.PollAddPublication(correlationID)
	require.Nil(t, pub)
	require.False(t, done)
	requireErrClientClosed(t, err)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_OfferAdd_closedClient(t *testing.T) {
	client, _ := newClientOfferAddTestFixture(t, t.TempDir())
	require.NoError(t, client.Close())

	correlationID, err := client.OfferAddPublication(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), correlationID)
	require.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_PollAdd_staleCorrelation(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID),
	)

	sub, done, err := client.PollAddSubscription(correlationID)
	require.True(t, done)
	require.NoError(t, err)
	require.NotNil(t, sub)

	_, doneAgain, err := client.PollAddSubscription(correlationID)
	require.False(t, doneAgain)
	require.ErrorIs(t, err, ErrAwaitInterrupted)

	neverOfferedID := client.NextCorrelationID() + 1000
	_, doneMissing, err := client.PollAddSubscription(neverOfferedID)
	require.False(t, doneMissing)
	require.ErrorIs(t, err, ErrAwaitInterrupted)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_PollAdd_otherKindCorrelation(t *testing.T) {
	polls := []struct {
		name string
		poll func(*testing.T, *Client, int64) (bool, error)
	}{
		{
			name: "PollAddPublication",
			poll: func(t *testing.T, client *Client, correlationID int64) (bool, error) {
				pub, done, err := client.PollAddPublication(correlationID)
				require.Nil(t, pub)
				return done, err
			},
		},
		{
			name: "PollAddExclusivePublication",
			poll: func(t *testing.T, client *Client, correlationID int64) (bool, error) {
				pub, done, err := client.PollAddExclusivePublication(correlationID)
				require.Nil(t, pub)
				return done, err
			},
		},
		{
			name: "PollAddSubscription",
			poll: func(t *testing.T, client *Client, correlationID int64) (bool, error) {
				sub, done, err := client.PollAddSubscription(correlationID)
				require.Nil(t, sub)
				return done, err
			},
		},
	}
	for _, pollCase := range polls {
		t.Run(pollCase.name, func(t *testing.T) {
			client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

			correlationID := client.NextCorrelationID()
			require.NoError(t, fixture.Conductor.RegisterPendingWithDeadline(
				conductor.PendingKindDestination,
				correlationID,
				client.nanoClock()+time.Second.Nanoseconds(),
			))

			done, err := pollCase.poll(t, client, correlationID)
			require.False(t, done)
			require.EqualError(t, err, fmt.Sprintf(
				"aeron: poll add: correlation id %d is not an add command",
				correlationID,
			))
			registration, ok := fixture.Conductor.PendingResult(correlationID)
			require.True(t, ok)
			require.Equal(t, conductor.PendingKindDestination, registration.Kind)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_PollAdd_closedGuardAfterPollPending_publicationKind(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
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

			_, done, err := testCase.pollOneTick(
				client,
				correlationID,
				true,
				func() { client.PublishClosedForTest() },
			)
			require.True(t, done)
			requireErrClientClosed(t, err)
			require.Equal(t, 0, client.PublicationCountForTest())
		})
	}
}

func TestClient_PollAdd_closedGuardAfterPollPending_subscription(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

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

	_, done, err := pollAddForTest(
		client,
		correlationID,
		subscriptionFromPending,
		true,
		func() { client.PublishClosedForTest() },
	)
	require.True(t, done)
	requireErrClientClosed(t, err)
	require.Equal(t, 0, client.SubscriptionCountForTest())
}

func TestClient_OfferAdd_offerPhaseTimeout(t *testing.T) {
	for _, testCase := range clientOfferAddAsyncKindCases() {
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

			correlationID, err := testCase.offerAdd(client)
			require.Equal(t, int64(0), correlationID)
			requireAddPhaseTimeout(
				t,
				fixture,
				client,
				err,
				testCase.registrationKind,
				testCase.countFn,
				true,
			)
			requireClientAddRingNoCommittedCommands(t, fixture.Ring)
		})
	}
}

func TestClient_OfferAdd_ringExhaustion_returnsRegistrationError(t *testing.T) {
	const timeout = time.Millisecond
	startNs := testutil.TestStartNanoNs

	for _, testCase := range clientOfferAddAsyncKindCases() {
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

			correlationID, err := testCase.offerAdd(client)
			require.Equal(t, int64(0), correlationID)
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

func TestClient_OfferAdd_terminationDuringOfferRetry(t *testing.T) {
	for _, testCase := range clientOfferAddAsyncKindCases() {
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

			resultCh := make(chan clientOfferAddResult, 1)
			go func() {
				correlationID, err := testCase.offerAdd(client)
				resultCh <- clientOfferAddResult{correlationID: correlationID, err: err}
			}()

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
				testCase.offerResultTimeoutMsg,
			)
			require.Equal(t, int64(0), got.correlationID)
			requireDriverShutdownTimeout(t, got.err)
			_, ok = fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, testCase.countFn(client))
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_OfferAdd_invalidChannel(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		wantMsg string
		offer   func(*Client, string) (int64, error)
	}{
		{
			name:    "publicationEmpty",
			channel: "",
			wantMsg: "aeron: channel is required",
			offer: func(client *Client, channel string) (int64, error) {
				return client.OfferAddPublication(clientAddStreamID, channel)
			},
		},
		{
			name:    "exclusivePublicationNonASCII",
			channel: "aeron:\x80",
			wantMsg: "aeron: channel must be ASCII",
			offer: func(client *Client, channel string) (int64, error) {
				return client.OfferAddExclusivePublication(clientAddStreamID, channel)
			},
		},
		{
			name:    "subscriptionEmpty",
			channel: "",
			wantMsg: "aeron: channel is required",
			offer: func(client *Client, channel string) (int64, error) {
				return client.OfferAddSubscription(clientAddStreamID, channel)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

			correlationID, err := testCase.offer(client, testCase.channel)
			require.Equal(t, int64(0), correlationID)
			require.EqualError(t, err, testCase.wantMsg)
			requireClientAddRingNoCommittedCommands(t, fixture.Ring)
		})
	}
}

func TestClient_OfferAdd_withoutPoll_cancelsOnClose(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok)

	const activeCorrelationID int64 = 42
	fixture.Conductor.SetActiveCorrelationID(activeCorrelationID)

	require.NoError(t, client.Close())
	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	require.Equal(t, activeCorrelationID, fixture.Conductor.ActiveCorrelationID())
}

func TestClient_OfferAdd_readyWithoutPoll_closeLingerHoldsLog(t *testing.T) {
	const clientCloseLinger = 200 * time.Millisecond

	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientOfferAddTestFixture(t, dir)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
			registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

			correlationID, err := testCase.offerAdd(client)
			require.NoError(t, err)
			testCase.signalReady(
				fixture.Conductor,
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
			require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

			client.SetClientCloseLingerDurationForTest(clientCloseLinger)
			started := time.Now()
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- client.Close()
			}()

			sawLinger := false
			var cleared time.Time
			deadline := time.After(2 * time.Second)
		poll:
			for {
				if registry.HasScheduledLinger() {
					sawLinger = true
				} else if sawLinger && cleared.IsZero() {
					cleared = time.Now()
				}
				select {
				case closeErr := <-closeDone:
					require.NoError(t, closeErr)
					break poll
				case <-deadline:
					t.Fatal("client close hung")
				default:
					time.Sleep(time.Millisecond)
				}
			}

			require.True(t, sawLinger, "close unmapped the unpolled log before the sleep")
			require.GreaterOrEqual(t, cleared.Sub(started), clientCloseLinger)
			require.Zero(t, conductor.LogBuffersRegistryEntryCountForTest(registry))
		})
	}
}

func TestClient_CancelPending_idempotent(t *testing.T) {
	client, fixture := newClientOfferAddTestFixture(t, t.TempDir())

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)

	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok)

	client.CancelPending(correlationID)
	client.CancelPending(correlationID)

	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)

	client.CancelPending(conductor.NullCorrelationID)
}

func TestClient_nilOfferAddAndPollAddPaths(t *testing.T) {
	var client *Client
	const correlationID int64 = 7

	offerPublicationID, err := client.OfferAddPublication(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), offerPublicationID)
	require.ErrorIs(t, err, ErrClientNotReady)

	offerExclusiveID, err := client.OfferAddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), offerExclusiveID)
	require.ErrorIs(t, err, ErrClientNotReady)

	offerSubscriptionID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), offerSubscriptionID)
	require.ErrorIs(t, err, ErrClientNotReady)

	pub, done, err := client.PollAddPublication(correlationID)
	require.Nil(t, pub)
	require.False(t, done)
	require.ErrorIs(t, err, ErrClientNotReady)

	exclusivePub, done, err := client.PollAddExclusivePublication(correlationID)
	require.Nil(t, exclusivePub)
	require.False(t, done)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, done, err := client.PollAddSubscription(correlationID)
	require.Nil(t, sub)
	require.False(t, done)
	require.ErrorIs(t, err, ErrClientNotReady)
}

func TestClient_OfferAdd_nilDriverProxy(t *testing.T) {
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

	correlationID, err := client.OfferAddPublication(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), correlationID)
	require.ErrorIs(t, err, ErrClientNotReady)

	correlationID, err = client.OfferAddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), correlationID)
	require.ErrorIs(t, err, ErrClientNotReady)

	correlationID, err = client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.Equal(t, int64(0), correlationID)
	require.ErrorIs(t, err, ErrClientNotReady)
}
