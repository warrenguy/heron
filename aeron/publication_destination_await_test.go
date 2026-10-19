package aeron

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	sendDestinationAwaitRegistrationID         int64 = 501
	sendDestinationAwaitOriginalRegistrationID int64 = 401
	sendDestinationAwaitDestinationID          int64 = 601
	sendDestinationAwaitChannel                      = "aeron:udp?endpoint=127.0.0.1:24611"
	sendDestinationAwaitErrorCode              int32 = 3
	sendDestinationAwaitErrorMessage                 = "unknown publication"
)

type sendDestinationCommand struct {
	correlationID             int64
	registrationID            int64
	destinationRegistrationID int64
	channel                   string
}

type sendDestinationAwaitOp struct {
	name      string
	asyncName string
	msgTypeID int
	call      func(*Client) error
	callAsync func(*Client) (int64, error)
	want      sendDestinationCommand
}

func newPublicationForDestinationAwait(client *Client) *Publication {
	return &Publication{
		registrationID:         sendDestinationAwaitRegistrationID,
		originalRegistrationID: sendDestinationAwaitOriginalRegistrationID,
		client:                 client,
	}
}

func newExclusivePublicationForDestinationAwait(client *Client) *ExclusivePublication {
	return &ExclusivePublication{
		registrationID:         sendDestinationAwaitRegistrationID,
		originalRegistrationID: sendDestinationAwaitOriginalRegistrationID,
		client:                 client,
	}
}

func sendDestinationAwaitOps() []sendDestinationAwaitOp {
	channelCommand := sendDestinationCommand{
		registrationID: sendDestinationAwaitOriginalRegistrationID,
		channel:        sendDestinationAwaitChannel,
	}
	byIDCommand := sendDestinationCommand{
		registrationID:            sendDestinationAwaitOriginalRegistrationID,
		destinationRegistrationID: sendDestinationAwaitDestinationID,
	}
	return []sendDestinationAwaitOp{
		{
			name:      "Publication.AddDestination",
			asyncName: "Publication.AsyncAddDestination",
			msgTypeID: conductor.AddDestination,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					AddDestination(sendDestinationAwaitChannel)
			},
			callAsync: func(client *Client) (int64, error) {
				return newPublicationForDestinationAwait(client).
					AsyncAddDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "Publication.RemoveDestination",
			asyncName: "Publication.AsyncRemoveDestination",
			msgTypeID: conductor.RemoveDestination,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					RemoveDestination(sendDestinationAwaitChannel)
			},
			callAsync: func(client *Client) (int64, error) {
				return newPublicationForDestinationAwait(client).
					AsyncRemoveDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "Publication.RemoveDestinationByID",
			asyncName: "Publication.AsyncRemoveDestinationByID",
			msgTypeID: conductor.RemoveDestinationByID,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					RemoveDestinationByID(sendDestinationAwaitDestinationID)
			},
			callAsync: func(client *Client) (int64, error) {
				return newPublicationForDestinationAwait(client).
					AsyncRemoveDestinationByID(sendDestinationAwaitDestinationID)
			},
			want: byIDCommand,
		},
		{
			name:      "ExclusivePublication.AddDestination",
			asyncName: "ExclusivePublication.AsyncAddDestination",
			msgTypeID: conductor.AddDestination,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					AddDestination(sendDestinationAwaitChannel)
			},
			callAsync: func(client *Client) (int64, error) {
				return newExclusivePublicationForDestinationAwait(client).
					AsyncAddDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "ExclusivePublication.RemoveDestination",
			asyncName: "ExclusivePublication.AsyncRemoveDestination",
			msgTypeID: conductor.RemoveDestination,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					RemoveDestination(sendDestinationAwaitChannel)
			},
			callAsync: func(client *Client) (int64, error) {
				return newExclusivePublicationForDestinationAwait(client).
					AsyncRemoveDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "ExclusivePublication.RemoveDestinationByID",
			asyncName: "ExclusivePublication.AsyncRemoveDestinationByID",
			msgTypeID: conductor.RemoveDestinationByID,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					RemoveDestinationByID(sendDestinationAwaitDestinationID)
			},
			callAsync: func(client *Client) (int64, error) {
				return newExclusivePublicationForDestinationAwait(client).
					AsyncRemoveDestinationByID(sendDestinationAwaitDestinationID)
			},
			want: byIDCommand,
		},
	}
}

func decodeSendDestinationInto(
	msgTypeID int,
	command *sendDestinationCommand,
) func(atomicbuffer.AtomicBuffer, int, int) (int64, error) {
	return func(buffer atomicbuffer.AtomicBuffer, index, length int) (int64, error) {
		if msgTypeID == conductor.RemoveDestinationByID {
			decoded, err := conductor.DecodeDestinationByIDMessage(buffer, index, length)
			if err != nil {
				return 0, err
			}
			*command = sendDestinationCommand{
				correlationID:             decoded.CorrelationID,
				registrationID:            decoded.ResourceRegistrationID,
				destinationRegistrationID: decoded.DestinationRegistrationID,
			}
			return decoded.CorrelationID, nil
		}
		decoded, err := conductor.DecodeDestinationMessage(buffer, index, length)
		if err != nil {
			return 0, err
		}
		*command = sendDestinationCommand{
			correlationID:  decoded.CorrelationID,
			registrationID: decoded.RegistrationCorrelationID,
			channel:        decoded.Channel,
		}
		return decoded.CorrelationID, nil
	}
}

func startSendDestinationAwait(client *Client, op sendDestinationAwaitOp) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- op.call(client)
	}()
	return resultCh
}

func receiveSendDestinationResult(t *testing.T, resultCh <-chan error) error {
	t.Helper()

	select {
	case err := <-resultCh:
		return err
	case <-time.After(clientConcurrentResultTimeout):
		require.FailNow(t, "timed out waiting for destination command result")
		return nil
	}
}

func respondToSendDestination(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	op sendDestinationAwaitOp,
	respond func(correlationID int64) (int, []byte),
) sendDestinationCommand {
	t.Helper()

	var command sendDestinationCommand
	responded, _ := pollAndRespondToAddCommand(
		t,
		fixture,
		op.msgTypeID,
		decodeSendDestinationInto(op.msgTypeID, &command),
		time.Now().Add(time.Second),
		respond,
		nil,
	)
	require.True(t, responded, "timed out waiting for destination command on ring")
	return command
}

func TestPublication_sendDestination_await(t *testing.T) {
	cases := []struct {
		name          string
		driverTimeout time.Duration
		setup         func(*testing.T, *Client, *testutil.ConductorFixture, func(int64))
		check         func(
			t *testing.T,
			fixture *testutil.ConductorFixture,
			op sendDestinationAwaitOp,
			resultCh <-chan error,
			wantCorrelationID int64,
		)
	}{
		{
			name:          "success",
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				op sendDestinationAwaitOp,
				resultCh <-chan error,
				wantCorrelationID int64,
			) {
				command := respondToSendDestination(
					t,
					fixture,
					op,
					func(correlationID int64) (int, []byte) {
						registration, ok := fixture.Conductor.PendingResult(correlationID)
						require.True(t, ok)
						require.Equal(t, conductor.PendingKindDestination, registration.Kind)
						return conductor.OnOperationSuccess,
							conductor.OperationSuccessBytes(correlationID)
					},
				)
				require.NoError(t, receiveSendDestinationResult(t, resultCh))

				want := op.want
				want.correlationID = wantCorrelationID
				require.Equal(t, want, command)
				requirePendingAbsent(t, fixture, wantCorrelationID)
			},
		},
		{
			name:          "driverError",
			driverTimeout: time.Second,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				op sendDestinationAwaitOp,
				resultCh <-chan error,
				wantCorrelationID int64,
			) {
				respondToSendDestination(
					t,
					fixture,
					op,
					func(correlationID int64) (int, []byte) {
						return conductor.OnError, conductor.ErrorResponseBytes(
							correlationID,
							sendDestinationAwaitErrorCode,
							sendDestinationAwaitErrorMessage,
						)
					},
				)
				err := receiveSendDestinationResult(t, resultCh)

				var registrationErr *RegistrationError
				require.ErrorAs(t, err, &registrationErr)
				require.Equal(t, wantCorrelationID, registrationErr.CorrelationID)
				require.Equal(t, sendDestinationAwaitErrorCode, registrationErr.ErrorCode)
				require.Equal(t, sendDestinationAwaitErrorMessage, registrationErr.Message)
				requirePendingAbsent(t, fixture, wantCorrelationID)
			},
		},
		{
			name:          "timeout",
			driverTimeout: 0,
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				op sendDestinationAwaitOp,
				resultCh <-chan error,
				wantCorrelationID int64,
			) {
				requireAwaitTimeout(
					t,
					fixture,
					receiveSendDestinationResult(t, resultCh),
					wantCorrelationID,
					0,
					fmt.Sprintf(
						"aeron: await response timeout: correlation=%d timeout=0s",
						wantCorrelationID,
					),
				)
				var command sendDestinationCommand
				require.Equal(t, wantCorrelationID, waitForAddCommand(
					t,
					fixture,
					op.msgTypeID,
					decodeSendDestinationInto(op.msgTypeID, &command),
					op.name,
				))
			},
		},
		{
			name:          "termination",
			driverTimeout: time.Second,
			setup: func(
				_ *testing.T,
				client *Client,
				fixture *testutil.ConductorFixture,
				setNanoNs func(int64),
			) {
				client.DoWork()
				setNanoNs(testutil.TestWorkNanoNs)
				fixture.Ring.SetConsumerHeartbeatTime(conductor.DriverShutdownHeartbeat)
			},
			check: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				_ sendDestinationAwaitOp,
				resultCh <-chan error,
				wantCorrelationID int64,
			) {
				err := receiveSendDestinationResult(t, resultCh)

				var timeoutErr *DriverTimeoutError
				require.ErrorAs(t, err, &timeoutErr)
				require.ErrorIs(t, timeoutErr.Unwrap(), conductor.ErrDriverShutdown)
				require.True(t, fixture.Conductor.IsTerminating())
				requirePendingAbsent(t, fixture, wantCorrelationID)
			},
		},
	}
	for _, op := range sendDestinationAwaitOps() {
		t.Run(op.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var nanoNs int64
					client, fixture := newClientAwaitFixture(
						t,
						func() int64 { return nanoNs },
						tc.driverTimeout,
						time.Millisecond,
					)
					if tc.setup != nil {
						tc.setup(t, client, fixture, func(ns int64) { nanoNs = ns })
					}

					wantCorrelationID := client.NextCorrelationID() + 1
					resultCh := startSendDestinationAwait(client, op)
					tc.check(t, fixture, op, resultCh, wantCorrelationID)
					requireActiveCorrelationCleared(t, fixture)
				})
			}
		})
	}
}

const (
	sendDestinationAsyncDriverTimeout = 100 * time.Millisecond
	sendDestinationAsyncSessionID     = int32(77)
)

// readSendDestinationCommand consumes the async command from the to-driver
// ring and asserts its fields.
func readSendDestinationCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	op sendDestinationAwaitOp,
	correlationID int64,
) {
	t.Helper()

	var command sendDestinationCommand
	require.Equal(t, correlationID, waitForAddCommand(
		t,
		fixture,
		op.msgTypeID,
		decodeSendDestinationInto(op.msgTypeID, &command),
		op.asyncName,
	))
	want := op.want
	want.correlationID = correlationID
	require.Equal(t, want, command)
}

func transmitSendDestinationSuccess(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	correlationID int64,
) {
	t.Helper()
	require.NoError(t, fixture.Transmit(
		conductor.OnOperationSuccess,
		conductor.OperationSuccessBytes(correlationID),
	))
}

func requirePendingAbsent(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	correlationID int64,
) {
	t.Helper()
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
}

func requirePollCommandDone(t *testing.T, client *Client, correlationID int64) error {
	t.Helper()
	done, err := client.PollCommand(correlationID)
	require.True(t, done)
	return err
}

// pollCommandEnv is one TestClient_PollCommand case's fixture.
type pollCommandEnv struct {
	client        *Client
	fixture       *testutil.ConductorFixture
	correlationID int64
	setNanoNs     func(int64)
}

// offerSendDestinationForTest issues op's async command. It asserts the ring
// command and the pending entry, then returns the correlation id.
func offerSendDestinationForTest(
	t *testing.T,
	client *Client,
	fixture *testutil.ConductorFixture,
	op sendDestinationAwaitOp,
) int64 {
	t.Helper()

	wantCorrelationID := client.NextCorrelationID() + 1
	correlationID, err := op.callAsync(client)
	require.NoError(t, err)
	require.Equal(t, wantCorrelationID, correlationID)
	readSendDestinationCommand(t, fixture, op, correlationID)
	registration, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok)
	require.Equal(t, conductor.PendingKindDestination, registration.Kind)
	require.Equal(t, sendDestinationAsyncDriverTimeout.Nanoseconds(), registration.DeadlineNs)
	requireActiveCorrelationCleared(t, fixture)
	return correlationID
}

func TestClient_asyncSendDestination_offer(t *testing.T) {
	for _, op := range sendDestinationAwaitOps() {
		t.Run(op.asyncName, func(t *testing.T) {
			client, fixture := newClientAwaitFixture(
				t,
				func() int64 { return 0 },
				sendDestinationAsyncDriverTimeout,
				time.Millisecond,
			)
			offerSendDestinationForTest(t, client, fixture, op)
		})
	}
}

func TestClient_PollCommand(t *testing.T) {
	timeoutMsg := func(correlationID int64) string {
		return fmt.Sprintf(
			"aeron: await response timeout: correlation=%d timeout=%s",
			correlationID,
			sendDestinationAsyncDriverTimeout,
		)
	}
	pastDeadlineNs := sendDestinationAsyncDriverTimeout.Nanoseconds()
	cases := []struct {
		name  string
		check func(t *testing.T, env pollCommandEnv)
	}{
		{
			name: "inFlight",
			check: func(t *testing.T, env pollCommandEnv) {
				for range 2 {
					done, err := env.client.PollCommand(env.correlationID)
					require.False(t, done)
					require.NoError(t, err)
					env.client.DoWork()
				}
				_, ok := env.fixture.Conductor.PendingResult(env.correlationID)
				require.True(t, ok)
			},
		},
		{
			name: "successThenSecondPoll",
			check: func(t *testing.T, env pollCommandEnv) {
				transmitSendDestinationSuccess(t, env.fixture, env.correlationID)
				env.client.DoWork()
				require.Equal(
					t,
					conductor.NullCorrelationID,
					env.fixture.Conductor.ReceivedCorrelationID(),
				)

				require.NoError(t, requirePollCommandDone(t, env.client, env.correlationID))
				requirePendingAbsent(t, env.fixture, env.correlationID)
				require.ErrorIs(
					t,
					requirePollCommandDone(t, env.client, env.correlationID),
					ErrAwaitInterrupted,
				)
			},
		},
		{
			name: "successDuringNextAvailableSessionID",
			check: func(t *testing.T, env pollCommandEnv) {
				transmitSendDestinationSuccess(t, env.fixture, env.correlationID)
				type result struct {
					sessionID int32
					err       error
				}
				resultCh := make(chan result, 1)
				go func() {
					sessionID, err := env.client.NextAvailableSessionID(clientAwaitTestStreamID)
					resultCh <- result{sessionID: sessionID, err: err}
				}()
				require.True(t, pollAndRespondToNextAvailableSessionIDCommand(
					t,
					env.fixture,
					sendDestinationAsyncSessionID,
					time.Now().Add(time.Second),
					nil,
				))
				got := receiveAddResult(
					t,
					resultCh,
					clientConcurrentResultTimeout,
					"timed out waiting for NextAvailableSessionID",
				)
				require.NoError(t, got.err)
				require.Equal(t, sendDestinationAsyncSessionID, got.sessionID)

				require.NoError(t, requirePollCommandDone(t, env.client, env.correlationID))
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "driverError",
			check: func(t *testing.T, env pollCommandEnv) {
				require.NoError(t, env.fixture.Transmit(
					conductor.OnError,
					conductor.ErrorResponseBytes(
						env.correlationID,
						sendDestinationAwaitErrorCode,
						sendDestinationAwaitErrorMessage,
					),
				))
				env.client.DoWork()

				err := requirePollCommandDone(t, env.client, env.correlationID)
				var registrationErr *RegistrationError
				require.ErrorAs(t, err, &registrationErr)
				require.Equal(t, env.correlationID, registrationErr.CorrelationID)
				require.Equal(t, sendDestinationAwaitErrorCode, registrationErr.ErrorCode)
				require.Equal(t, sendDestinationAwaitErrorMessage, registrationErr.Message)
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "deadlineFallback",
			check: func(t *testing.T, env pollCommandEnv) {
				env.setNanoNs(pastDeadlineNs)

				requireAwaitTimeout(
					t,
					env.fixture,
					requirePollCommandDone(t, env.client, env.correlationID),
					env.correlationID,
					sendDestinationAsyncDriverTimeout,
					timeoutMsg(env.correlationID),
				)
			},
		},
		{
			name: "conductorExpiry",
			check: func(t *testing.T, env pollCommandEnv) {
				env.setNanoNs(pastDeadlineNs)
				env.client.DoWork()
				registration, ok := env.fixture.Conductor.PendingResult(env.correlationID)
				require.True(t, ok)
				require.ErrorIs(t, registration.Err, conductor.ErrAwaitTimeout)

				requireAwaitTimeout(
					t,
					env.fixture,
					requirePollCommandDone(t, env.client, env.correlationID),
					env.correlationID,
					sendDestinationAsyncDriverTimeout,
					timeoutMsg(env.correlationID),
				)
			},
		},
		{
			name: "lateSuccessAfterExpiry",
			check: func(t *testing.T, env pollCommandEnv) {
				env.setNanoNs(pastDeadlineNs)
				env.client.DoWork()
				transmitSendDestinationSuccess(t, env.fixture, env.correlationID)
				env.client.DoWork()

				require.NoError(t, requirePollCommandDone(t, env.client, env.correlationID))
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "cancelPending",
			check: func(t *testing.T, env pollCommandEnv) {
				env.client.CancelPending(env.correlationID)

				require.ErrorIs(
					t,
					requirePollCommandDone(t, env.client, env.correlationID),
					ErrAwaitInterrupted,
				)
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "conductorTerminated",
			check: func(t *testing.T, env pollCommandEnv) {
				env.client.DoWork()
				env.setNanoNs(testutil.TestWorkNanoNs)
				env.fixture.Ring.SetConsumerHeartbeatTime(conductor.DriverShutdownHeartbeat)
				env.client.DoWork()
				require.True(t, env.fixture.Conductor.IsTerminating())
				requirePendingAbsent(t, env.fixture, env.correlationID)

				for range 2 {
					err := requirePollCommandDone(t, env.client, env.correlationID)
					var timeoutErr *DriverTimeoutError
					require.ErrorAs(t, err, &timeoutErr)
					require.ErrorIs(t, timeoutErr.Unwrap(), conductor.ErrDriverShutdown)
				}
			},
		},
		{
			name: "clientClosed",
			check: func(t *testing.T, env pollCommandEnv) {
				require.NoError(t, env.client.Close())

				done, err := env.client.PollCommand(env.correlationID)
				require.False(t, done)
				require.ErrorIs(t, err, ErrClientClosed)
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "abandoned",
			check: func(t *testing.T, env pollCommandEnv) {
				transmitSendDestinationSuccess(t, env.fixture, env.correlationID)
				for range 3 {
					env.client.DoWork()
				}
				registration, ok := env.fixture.Conductor.PendingResult(env.correlationID)
				require.True(t, ok)
				require.True(t, registration.Ready)

				require.NoError(t, env.client.Close())
				requirePendingAbsent(t, env.fixture, env.correlationID)
			},
		},
		{
			name: "otherKindCorrelation",
			check: func(t *testing.T, env pollCommandEnv) {
				addPublicationID, err := env.client.OfferAddPublication(
					clientAddStreamID,
					clientAddChannel,
				)
				require.NoError(t, err)

				done, err := env.client.PollCommand(addPublicationID)
				require.False(t, done)
				require.EqualError(t, err, fmt.Sprintf(
					"aeron: poll command: correlation id %d is not an async destination command",
					addPublicationID,
				))
				registration, ok := env.fixture.Conductor.PendingResult(addPublicationID)
				require.True(t, ok)
				require.Equal(t, conductor.PendingKindAddPublication, registration.Kind)
				registration, ok = env.fixture.Conductor.PendingResult(env.correlationID)
				require.True(t, ok)
				require.Equal(t, conductor.PendingKindDestination, registration.Kind)
				require.False(t, registration.Ready)
				require.NoError(t, registration.Err)
			},
		},
	}
	op := sendDestinationAwaitOps()[0]
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAwaitFixture(
				t,
				func() int64 { return nanoNs },
				sendDestinationAsyncDriverTimeout,
				time.Millisecond,
			)
			correlationID := offerSendDestinationForTest(t, client, fixture, op)

			tc.check(t, pollCommandEnv{
				client:        client,
				fixture:       fixture,
				correlationID: correlationID,
				setNanoNs:     func(ns int64) { nanoNs = ns },
			})
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_asyncSendDestination_ringFull(t *testing.T) {
	for _, op := range sendDestinationAwaitOps() {
		t.Run(op.asyncName, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAwaitFixture(
				t,
				func() int64 { return nanoNs },
				sendDestinationAsyncDriverTimeout,
				time.Millisecond,
			)
			seedFullClientAddRing(t, fixture.Ring)

			wantCorrelationID := client.NextCorrelationID() + 1
			correlationID, err := op.callAsync(client)
			require.Zero(t, correlationID)
			require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
			requirePendingAbsent(t, fixture, wantCorrelationID)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_PollCommand_nilClient(t *testing.T) {
	var client *Client
	done, err := client.PollCommand(sendDestinationAwaitDestinationID)
	require.False(t, done)
	require.ErrorIs(t, err, ErrClientNotReady)
}
