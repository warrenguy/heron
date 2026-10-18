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
	msgTypeID int
	call      func(*Client) error
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
			msgTypeID: conductor.AddDestination,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					AddDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "Publication.RemoveDestination",
			msgTypeID: conductor.RemoveDestination,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					RemoveDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "Publication.RemoveDestinationByID",
			msgTypeID: conductor.RemoveDestinationByID,
			call: func(client *Client) error {
				return newPublicationForDestinationAwait(client).
					RemoveDestinationByID(sendDestinationAwaitDestinationID)
			},
			want: byIDCommand,
		},
		{
			name:      "ExclusivePublication.AddDestination",
			msgTypeID: conductor.AddDestination,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					AddDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "ExclusivePublication.RemoveDestination",
			msgTypeID: conductor.RemoveDestination,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					RemoveDestination(sendDestinationAwaitChannel)
			},
			want: channelCommand,
		},
		{
			name:      "ExclusivePublication.RemoveDestinationByID",
			msgTypeID: conductor.RemoveDestinationByID,
			call: func(client *Client) error {
				return newExclusivePublicationForDestinationAwait(client).
					RemoveDestinationByID(sendDestinationAwaitDestinationID)
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
				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
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
				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
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
				_, ok := fixture.Conductor.PendingResult(wantCorrelationID)
				require.False(t, ok)
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
