package aeron

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	clientCounterTestTypeID    int32 = 1001
	clientCounterTestCounterID int32 = 7
	clientCounterTestLabel           = "counter1"
)

var clientCounterTestKey = []byte{1, 2, 3, 4, 5}

type addCounterResult struct {
	counter *Counter
	err     error
}

func startAddCounter(
	client *Client,
	typeID int32,
	key []byte,
	label string,
) <-chan addCounterResult {
	resultCh := make(chan addCounterResult, 1)
	go func() {
		counter, err := client.AddCounter(typeID, key, label)
		resultCh <- addCounterResult{counter: counter, err: err}
	}()
	return resultCh
}

func receiveAddCounterResult(
	t *testing.T,
	resultCh <-chan addCounterResult,
) addCounterResult {
	t.Helper()

	select {
	case result := <-resultCh:
		return result
	case <-time.After(clientConcurrentResultTimeout):
		require.FailNow(t, "timed out waiting for AddCounter result")
		return addCounterResult{}
	}
}

// decodeAddCounterInto returns a ring decode func that stores the decoded
// ADD_COUNTER body in message.
func decodeAddCounterInto(
	message *conductor.CounterMessage,
) func(atomicbuffer.AtomicBuffer, int, int) (int64, error) {
	return func(buffer atomicbuffer.AtomicBuffer, index, length int) (int64, error) {
		decoded, err := conductor.DecodeCounterMessage(buffer, index, length)
		if err != nil {
			return 0, err
		}
		*message = decoded
		return decoded.CorrelationID, nil
	}
}

func pollAndRespondToAddCounterCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	counterID int32,
	message *conductor.CounterMessage,
) bool {
	t.Helper()

	responded, _ := pollAndRespondToAddCommand(
		t,
		fixture,
		conductor.AddCounter,
		decodeAddCounterInto(message),
		time.Now().Add(time.Second),
		func(correlationID int64) (int, []byte) {
			return conductor.OnCounterReady,
				conductor.CorrelationAndInt32Bytes(correlationID, counterID)
		},
		nil,
	)
	return responded
}

func pollAndRespondToAddCounterCommandError(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	errorCode int32,
	message string,
) bool {
	t.Helper()

	var decoded conductor.CounterMessage
	responded, _ := pollAndRespondToAddCommandError(
		t,
		fixture,
		conductor.AddCounter,
		decodeAddCounterInto(&decoded),
		time.Now().Add(time.Second),
		errorCode,
		message,
	)
	return responded
}

func waitForAddCounterCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
) int64 {
	t.Helper()

	var decoded conductor.CounterMessage
	return waitForAddCommand(
		t,
		fixture,
		conductor.AddCounter,
		decodeAddCounterInto(&decoded),
		"AddCounter",
	)
}

// addCounterForTest allocates a counter against a synthetic ON_COUNTER_READY.
func addCounterForTest(
	t *testing.T,
	client *Client,
	fixture *testutil.ConductorFixture,
) *Counter {
	t.Helper()

	resultCh := startAddCounter(
		client,
		clientCounterTestTypeID,
		clientCounterTestKey,
		clientCounterTestLabel,
	)
	var message conductor.CounterMessage
	require.True(
		t,
		pollAndRespondToAddCounterCommand(t, fixture, clientCounterTestCounterID, &message),
		"timed out waiting for AddCounter command on ring",
	)
	result := receiveAddCounterResult(t, resultCh)
	require.NoError(t, result.err)
	return result.counter
}

// drainRemoveCounterMessages reads every committed ring record. It returns the
// REMOVE_COUNTER bodies in ring order.
func drainRemoveCounterMessages(
	t *testing.T,
	fixture *testutil.ConductorFixture,
) []conductor.RemoveMessage {
	t.Helper()

	var messages []conductor.RemoveMessage
	for {
		before := fixture.Ring.ConsumerPosition()
		fixture.Ring.Read(
			func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
				if msgTypeID != conductor.RemoveCounter {
					return
				}
				message, err := conductor.DecodeRemoveMessage(buffer, index, length)
				require.NoError(t, err)
				messages = append(messages, message)
			},
		)
		if fixture.Ring.ConsumerPosition() == before {
			return messages
		}
	}
}

func requireAddCounterRejected(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	result addCounterResult,
	wantCorrelationID int64,
	wantErr string,
) {
	t.Helper()

	require.EqualError(t, result.err, wantErr)
	require.Nil(t, result.counter)
	requireAddCommandNotOnRing(t, fixture, conductor.AddCounter)
	require.Equal(
		t,
		wantCorrelationID,
		fixture.DriverProxy.NextCorrelationID(),
		"AddCounter drew a correlation id before it checked its input",
	)
}

func TestClient_AddCounter(t *testing.T) {
	cases := []struct {
		name          string
		key           []byte
		label         string
		driverTimeout time.Duration
		setup         func(
			t *testing.T,
			client *Client,
			fixture *testutil.ConductorFixture,
			setNanoNs func(int64),
		)
		check func(
			t *testing.T,
			fixture *testutil.ConductorFixture,
			resultCh <-chan addCounterResult,
			wantCorrelationID int64,
		)
	}{
		{
			name:          "ready",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				var message conductor.CounterMessage
				require.True(
					t,
					pollAndRespondToAddCounterCommand(
						t,
						fixture,
						clientCounterTestCounterID,
						&message,
					),
					"timed out waiting for AddCounter command on ring",
				)

				result := receiveAddCounterResult(t, resultCh)
				require.NoError(t, result.err)
				require.Equal(t, clientCounterTestCounterID, result.counter.CounterID())
				require.Equal(t, wantCorrelationID, result.counter.RegistrationID())

				require.Equal(t, testutil.TestClientID, message.ClientID)
				require.Equal(t, wantCorrelationID, message.CorrelationID)
				require.Equal(t, clientCounterTestTypeID, message.TypeID)
				require.Equal(t, clientCounterTestKey, message.Key)
				require.Equal(t, clientCounterTestLabel, message.Label)

				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
			},
		},
		{
			name:          "driverError",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				require.True(
					t,
					pollAndRespondToAddCounterCommandError(t, fixture, 11, "counter rejected"),
					"timed out waiting for AddCounter command on ring",
				)

				result := receiveAddCounterResult(t, resultCh)
				require.Nil(t, result.counter)
				var registrationErr *RegistrationError
				require.ErrorAs(t, result.err, &registrationErr)
				require.Equal(t, wantCorrelationID, registrationErr.CorrelationID)
				require.Equal(t, int32(11), registrationErr.ErrorCode)
				require.Equal(t, "counter rejected", registrationErr.Message)

				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
			},
		},
		{
			name:          "timeout",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: 0,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				result := receiveAddCounterResult(t, resultCh)
				require.Nil(t, result.counter)
				requireAwaitTimeout(
					t,
					fixture,
					result.err,
					wantCorrelationID,
					0,
					fmt.Sprintf(
						"aeron: await response timeout: correlation=%d timeout=0s",
						wantCorrelationID,
					),
				)
				require.Equal(t, wantCorrelationID, waitForAddCounterCommand(t, fixture))
			},
		},
		{
			name:          "keyTooLong",
			key:           make([]byte, 113),
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				requireAddCounterRejected(
					t,
					fixture,
					receiveAddCounterResult(t, resultCh),
					wantCorrelationID,
					"conductor: counter key exceeds 112 bytes",
				)
			},
		},
		{
			name:          "labelTooLong",
			key:           clientCounterTestKey,
			label:         strings.Repeat("a", 381),
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				requireAddCounterRejected(
					t,
					fixture,
					receiveAddCounterResult(t, resultCh),
					wantCorrelationID,
					"conductor: counter label exceeds 380 bytes",
				)
			},
		},
		{
			name:          "labelNotASCII",
			key:           clientCounterTestKey,
			label:         "caf\xe9",
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				requireAddCounterRejected(
					t,
					fixture,
					receiveAddCounterResult(t, resultCh),
					wantCorrelationID,
					"atomicbuffer: string ascii non-ascii",
				)
			},
		},
		{
			name:          "closedClient",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			setup: func(t *testing.T, client *Client, _ *testutil.ConductorFixture, _ func(int64)) {
				require.NoError(t, client.Close())
			},
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				_ int64,
			) {
				result := receiveAddCounterResult(t, resultCh)
				require.Nil(t, result.counter)
				requireErrClientClosed(t, result.err)
				requireAddCommandNotOnRing(t, fixture, conductor.AddCounter)
			},
		},
		{
			name:          "fullRing",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			setup: func(t *testing.T, _ *Client, fixture *testutil.ConductorFixture, _ func(int64)) {
				seedFullClientAddRing(t, fixture.Ring)
			},
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				result := receiveAddCounterResult(t, resultCh)
				require.Nil(t, result.counter)
				require.ErrorIs(t, result.err, conductor.ErrRingInsufficientCapacity)
				require.EqualError(t, result.err, "conductor: ring insufficient capacity")

				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
			},
		},
		{
			name:          "terminatingConductor",
			key:           clientCounterTestKey,
			label:         clientCounterTestLabel,
			driverTimeout: time.Second,
			setup: func(
				t *testing.T,
				_ *Client,
				fixture *testutil.ConductorFixture,
				setNanoNs func(int64),
			) {
				setupClientAddConductorTerminating(t, fixture, setNanoNs)
			},
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				resultCh <-chan addCounterResult,
				wantCorrelationID int64,
			) {
				result := receiveAddCounterResult(t, resultCh)
				require.Nil(t, result.counter)
				require.ErrorIs(t, result.err, ErrConductorTerminating)
				require.EqualError(t, result.err, "aeron: conductor is terminating")
				requireAddCommandNotOnRing(t, fixture, conductor.AddCounter)

				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAwaitFixture(
				t,
				func() int64 { return nanoNs },
				tc.driverTimeout,
				time.Millisecond,
			)

			wantCorrelationID := client.NextCorrelationID() + 1
			if tc.setup != nil {
				tc.setup(t, client, fixture, func(ns int64) { nanoNs = ns })
			}
			resultCh := startAddCounter(client, clientCounterTestTypeID, tc.key, tc.label)
			tc.check(t, fixture, resultCh, wantCorrelationID)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestCounter_Close(t *testing.T) {
	cases := []struct {
		name  string
		setup func(
			t *testing.T,
			client *Client,
			fixture *testutil.ConductorFixture,
			setNanoNs func(int64),
		)
		check func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter)
	}{
		{
			name: "offersOnce",
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				require.NoError(t, counter.Close())

				messages := drainRemoveCounterMessages(t, fixture)
				require.Len(t, messages, 1)
				require.Equal(t, testutil.TestClientID, messages[0].ClientID)
				require.Equal(t, counter.RegistrationID(), messages[0].RegistrationID)
				require.Greater(t, messages[0].CorrelationID, counter.RegistrationID())
			},
		},
		{
			name: "secondCallOffersNothing",
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				require.NoError(t, counter.Close())
				require.Len(t, drainRemoveCounterMessages(t, fixture), 1)

				require.NoError(t, counter.Close())
				require.Empty(t, drainRemoveCounterMessages(t, fixture))
			},
		},
		{
			name: "closedClient",
			setup: func(
				t *testing.T,
				client *Client,
				_ *testutil.ConductorFixture,
				_ func(int64),
			) {
				require.NoError(t, client.Close())
			},
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				require.NoError(t, counter.Close())
				require.Empty(t, drainRemoveCounterMessages(t, fixture))
			},
		},
		{
			name: "closedFlagOnly",
			setup: func(
				_ *testing.T,
				client *Client,
				_ *testutil.ConductorFixture,
				_ func(int64),
			) {
				client.closed.Store(true)
			},
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				require.NoError(t, counter.Close())
				require.Empty(t, drainRemoveCounterMessages(t, fixture))
			},
		},
		{
			name: "terminatingConductor",
			setup: func(
				t *testing.T,
				_ *Client,
				fixture *testutil.ConductorFixture,
				setNanoNs func(int64),
			) {
				setupClientAddConductorTerminating(t, fixture, setNanoNs)
			},
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				require.NoError(t, counter.Close())
				require.Empty(t, drainRemoveCounterMessages(t, fixture))
			},
		},
		{
			name: "fullRing",
			setup: func(
				t *testing.T,
				_ *Client,
				fixture *testutil.ConductorFixture,
				_ func(int64),
			) {
				seedFullClientAddRing(t, fixture.Ring)
			},
			check: func(t *testing.T, fixture *testutil.ConductorFixture, counter *Counter) {
				err := counter.Close()
				require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
				require.EqualError(t, err, "aeron: remove counter: conductor: ring insufficient capacity")

				resetClientAddRingEmpty(fixture.Ring)
				require.NoError(t, counter.Close())
				require.Empty(t, drainRemoveCounterMessages(t, fixture))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nanoNs atomic.Int64
			client, fixture := newClientAwaitFixture(
				t,
				nanoNs.Load,
				time.Second,
				time.Millisecond,
			)
			counter := addCounterForTest(t, client, fixture)
			require.Empty(t, drainRemoveCounterMessages(t, fixture))

			if tc.setup != nil {
				tc.setup(t, client, fixture, nanoNs.Store)
			}
			tc.check(t, fixture, counter)
		})
	}
}
