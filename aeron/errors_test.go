package aeron_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestDriverTimeoutError_connectHeartbeatCauses(t *testing.T) {
	tests := []struct {
		name    string
		cause   error
		wantMsg string
	}{
		{
			name:    "not ready",
			cause:   aeron.ErrDriverHeartbeatNotReady,
			wantMsg: "aeron: media driver timeout: aeron: driver consumer heartbeat not ready",
		},
		{
			name:  "stale at connect",
			cause: aeron.ErrDriverHeartbeatStaleAtConnect,
			wantMsg: "aeron: media driver timeout: " +
				"aeron: driver consumer heartbeat stale at connect",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := &aeron.DriverTimeoutError{Cause: testCase.cause}
			require.ErrorIs(t, err, testCase.cause)
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestControlProtocolVersionError(t *testing.T) {
	err := &aeron.ControlProtocolVersionError{Got: 2, Want: 1}
	require.EqualError(
		t,
		err,
		"aeron: control protocol version mismatch: got 2 want 1",
	)
}

func TestRegistrationError(t *testing.T) {
	cause := errors.New("conductor: ring insufficient capacity")

	tests := []struct {
		name      string
		err       *aeron.RegistrationError
		want      string
		checkWrap bool
	}{
		{
			name: "driverRegistration",
			err: &aeron.RegistrationError{
				CorrelationID: 9,
				ErrorCode:     3,
				Message:       "channel invalid",
			},
			want: "aeron: driver registration error: correlation=9 code=3 message=channel invalid",
		},
		{
			name: "kindCause",
			err: &aeron.RegistrationError{
				Kind:          "publication",
				CorrelationID: 9,
				Cause:         cause,
			},
			want: "aeron: publication registration error: correlation=9: " +
				"conductor: ring insufficient capacity",
			checkWrap: true,
		},
		{
			name: "kindOnly",
			err: &aeron.RegistrationError{
				Kind:          "subscription",
				CorrelationID: 3,
			},
			want: "aeron: subscription registration error: correlation=3",
		},
		{
			name: "correlationOnly",
			err: &aeron.RegistrationError{
				CorrelationID: 11,
			},
			want: "aeron: registration error: correlation=11",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.checkWrap {
				require.ErrorIs(t, testCase.err, cause)
				require.Equal(t, cause, testCase.err.Unwrap())
			}
			require.EqualError(t, testCase.err, testCase.want)
		})
	}
}

func TestRegistrationTimeoutError(t *testing.T) {
	err := aeron.NewRegistrationTimeoutErrorForTest(
		"publication",
		1001,
		7,
		"aeron:ipc",
	)
	require.EqualError(
		t,
		err,
		"aeron: publication registration timeout: correlation=1001 stream=7 channel=aeron:ipc",
	)
}

func TestAwaitTimeoutError(t *testing.T) {
	err := &aeron.AwaitTimeoutError{
		CorrelationID: 5,
		Timeout:       2 * time.Second,
	}
	require.EqualError(
		t,
		err,
		"aeron: await response timeout: correlation=5 timeout=2s",
	)
}

func TestErrClientClosed(t *testing.T) {
	require.EqualError(t, aeron.ErrClientClosed, "aeron: client is closed")
}

func TestErrClientNotReady(t *testing.T) {
	require.EqualError(t, aeron.ErrClientNotReady, "aeron: client is not ready")
}

func TestMapRegistrationError(t *testing.T) {
	tests := []struct {
		name     string
		input    error
		wantSame bool
		wantErr  error
		wantMsg  string
	}{
		{
			name:    "conductor terminating",
			input:   conductor.ErrConductorTerminating,
			wantErr: aeron.ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name:    "pending already registered",
			input:   conductor.ErrPendingAlreadyRegistered,
			wantErr: aeron.ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
		{
			name:     "pass through",
			input:    errors.New("aeron: other failure"),
			wantSame: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapRegistrationErrorForTest(testCase.input)
			if testCase.wantSame {
				require.Same(t, testCase.input, mapped)
				return
			}
			require.ErrorIs(t, mapped, testCase.wantErr)
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
}

func TestMapAwaitError(t *testing.T) {
	registrationErr := &conductor.DriverRegistrationError{
		CorrelationID: 4,
		ErrorCode:     2,
		Message:       "failed",
	}
	otherErr := errors.New("aeron: other failure")

	tests := []struct {
		name     string
		input    error
		wantSame bool
		wantErr  error
		wantMsg  string
	}{
		{
			name:    "driver registration error",
			input:   registrationErr,
			wantMsg: "aeron: driver registration error: correlation=4 code=2 message=failed",
		},
		{
			name:    "pending not found",
			input:   conductor.ErrPendingNotFound,
			wantErr: aeron.ErrAwaitInterrupted,
			wantMsg: "aeron: await response interrupted",
		},
		{
			name:     "pass through",
			input:    otherErr,
			wantSame: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapAwaitErrorForTest(testCase.input)
			if testCase.wantSame {
				require.Same(t, testCase.input, mapped)
				return
			}
			if testCase.wantErr != nil {
				require.ErrorIs(t, mapped, testCase.wantErr)
				require.EqualError(t, mapped, testCase.wantMsg)
				return
			}

			var got *aeron.RegistrationError
			require.ErrorAs(t, mapped, &got)
			require.Equal(t, int64(4), got.CorrelationID)
			require.Equal(t, int32(2), got.ErrorCode)
			require.Equal(t, "failed", got.Message)
			require.ErrorIs(t, mapped, registrationErr)
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
}

func TestMapPollPendingError(t *testing.T) {
	const correlationID int64 = 5
	const timeout = 2 * time.Second
	registrationErr := &conductor.DriverRegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     2,
		Message:       "failed",
	}
	otherErr := errors.New("aeron: other failure")

	tests := []struct {
		name     string
		input    error
		wantSame bool
		wantErr  error
		wantMsg  string
	}{
		{
			name:    "await timeout",
			input:   conductor.ErrAwaitTimeout,
			wantMsg: "aeron: await response timeout: correlation=5 timeout=2s",
		},
		{
			name:    "driver registration error",
			input:   registrationErr,
			wantMsg: "aeron: driver registration error: correlation=5 code=2 message=failed",
		},
		{
			name:    "pending not found",
			input:   conductor.ErrPendingNotFound,
			wantErr: aeron.ErrAwaitInterrupted,
			wantMsg: "aeron: await response interrupted",
		},
		{
			name:     "pass through",
			input:    otherErr,
			wantSame: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapPollPendingErrorForTest(
				correlationID,
				timeout,
				testCase.input,
			)
			if testCase.wantSame {
				require.Same(t, testCase.input, mapped)
				return
			}
			if testCase.input == conductor.ErrAwaitTimeout {
				var timeoutErr *aeron.AwaitTimeoutError
				require.ErrorAs(t, mapped, &timeoutErr)
				require.Equal(t, correlationID, timeoutErr.CorrelationID)
				require.Equal(t, timeout, timeoutErr.Timeout)
				require.EqualError(t, mapped, testCase.wantMsg)
				return
			}
			if testCase.wantErr != nil {
				require.ErrorIs(t, mapped, testCase.wantErr)
				require.EqualError(t, mapped, testCase.wantMsg)
				return
			}

			var got *aeron.RegistrationError
			require.ErrorAs(t, mapped, &got)
			require.Equal(t, correlationID, got.CorrelationID)
			require.Equal(t, int32(2), got.ErrorCode)
			require.Equal(t, "failed", got.Message)
			require.ErrorIs(t, mapped, registrationErr)
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
}

func TestMapAddOfferError(t *testing.T) {
	const correlationID int64 = 42
	otherErr := errors.New("aeron: other failure")

	for _, kind := range []string{"publication", "subscription"} {
		t.Run(kind+"/ringBackpressure", func(t *testing.T) {
			mapped := aeron.MapAddOfferErrorForTest(
				kind,
				correlationID,
				conductor.ErrRingInsufficientCapacity,
			)

			var got *aeron.RegistrationError
			require.ErrorAs(t, mapped, &got)
			require.Equal(t, kind, got.Kind)
			require.Equal(t, correlationID, got.CorrelationID)
			require.ErrorIs(t, mapped, conductor.ErrRingInsufficientCapacity)
			require.EqualError(
				t,
				mapped,
				"aeron: "+kind+" registration error: correlation=42: "+
					"conductor: ring insufficient capacity",
			)
		})
	}

	t.Run("passThrough", func(t *testing.T) {
		mapped := aeron.MapAddOfferErrorForTest("publication", correlationID, otherErr)
		require.Same(t, otherErr, mapped)
	})
}

func TestMapAddPollPendingError(t *testing.T) {
	const (
		correlationID int64 = 5
		streamID      int32 = 7
		channel             = "aeron:ipc"
	)
	registrationErr := &conductor.DriverRegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     2,
		Message:       "failed",
	}
	otherErr := errors.New("aeron: other failure")

	for _, kind := range []string{"publication", "subscription"} {
		t.Run(kind+"/awaitTimeout", func(t *testing.T) {
			mapped := aeron.MapAddPollPendingErrorForTest(
				kind,
				correlationID,
				streamID,
				channel,
				conductor.ErrAwaitTimeout,
			)

			var timeoutErr *aeron.RegistrationTimeoutError
			require.ErrorAs(t, mapped, &timeoutErr)
			require.Equal(t, kind, timeoutErr.Kind)
			require.Equal(t, correlationID, timeoutErr.CorrelationID)
			require.EqualError(
				t,
				mapped,
				"aeron: "+kind+" registration timeout: correlation=5 "+
					"stream=7 channel=aeron:ipc",
			)
		})

		t.Run(kind+"/driverRegistrationError", func(t *testing.T) {
			mapped := aeron.MapAddPollPendingErrorForTest(
				kind,
				correlationID,
				streamID,
				channel,
				registrationErr,
			)

			var got *aeron.RegistrationError
			require.ErrorAs(t, mapped, &got)
			require.Equal(t, kind, got.Kind)
			require.Equal(t, correlationID, got.CorrelationID)
			require.Equal(t, int32(2), got.ErrorCode)
			require.Equal(t, "failed", got.Message)
			require.ErrorIs(t, got, registrationErr)
			require.EqualError(
				t,
				mapped,
				"aeron: driver registration error: correlation=5 code=2 message=failed",
			)
		})

		t.Run(kind+"/pendingNotFound", func(t *testing.T) {
			mapped := aeron.MapAddPollPendingErrorForTest(
				kind,
				correlationID,
				streamID,
				channel,
				conductor.ErrPendingNotFound,
			)
			require.ErrorIs(t, mapped, aeron.ErrAwaitInterrupted)
			require.EqualError(t, mapped, "aeron: await response interrupted")
		})

		t.Run(kind+"/passThrough", func(t *testing.T) {
			mapped := aeron.MapAddPollPendingErrorForTest(
				kind,
				correlationID,
				streamID,
				channel,
				otherErr,
			)

			var got *aeron.RegistrationError
			require.ErrorAs(t, mapped, &got)
			require.Equal(t, kind, got.Kind)
			require.ErrorIs(t, mapped, otherErr)
			require.EqualError(
				t,
				mapped,
				"aeron: "+kind+" registration error: correlation=5: aeron: other failure",
			)
		})
	}
}

func TestPollHelperTimeoutErrors_errorStrings(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "pollUntilConnected",
			err: &aeron.PollUntilConnectedTimeoutError{
				PublicationRegID: 42,
				StreamID:         1001,
				SessionID:        2002,
				Timeout:          10 * time.Second,
				Position:         0,
			},
			want: "aeron: publication 42 (stream 1001 session 2002) not connected within 10s; position=0",
		},
		{
			name: "pollUntilImageCount",
			err: &aeron.PollUntilImageCountTimeoutError{
				RegistrationID: 77,
				StreamID:       1001,
				Want:           1,
				Have:           0,
				Timeout:        10 * time.Second,
				Detail:         "no images",
			},
			want: "aeron: subscription 77 (stream 1001) active image count=0 want=1; no images within 10s",
		},
		{
			name: "pollOneMessage",
			err: &aeron.PollOneMessageTimeoutError{
				RegistrationID: 77,
				StreamID:       1001,
				Timeout:        10 * time.Second,
				Detail:         "no images",
			},
			want: "aeron: subscription 77 (stream 1001) received no message within 10s; no images",
		},
		{
			name: "pollReassembledPrecondition",
			err: &aeron.PollReassembledPreconditionError{
				RegistrationID: 77,
				Detail:         "no active images",
			},
			want: "aeron: subscription 77 reassembled 0/1 messages; fragments=0; no active images",
		},
		{
			name: "pollReassembledLoopTimeout",
			err: &aeron.PollReassembledTimeoutError{
				RegistrationID: 77,
				Timeout:        10 * time.Second,
				Detail:         "active=1 [0 stream=1001 session=2002 position=128]",
			},
			want: "aeron: subscription 77 reassembled 0/1 messages; fragments=0; " +
				"active=1 [0 stream=1001 session=2002 position=128] within 10s",
		},
		{
			name: "runDuringClientWork",
			err: &aeron.RunDuringClientWorkTimeoutError{
				Timeout: 10 * time.Second,
			},
			want: "aeron: RunDuringClientWork timed out within 10s",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			require.EqualError(t, testCase.err, testCase.want)
		})
	}
}

func TestOfferHelperErrors(t *testing.T) {
	const (
		publicationRegID int64 = 42
		streamID         int32 = 1001
		sessionID        int32 = 2002
	)
	pub := "aeron: publication 42 (stream 1001 session 2002)"

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "terminal",
			err: &aeron.OfferTerminalError{
				Status:           aeron.Closed,
				PublicationRegID: publicationRegID,
				StreamID:         streamID,
				SessionID:        sessionID,
				Position:         128,
			},
			want: pub + " offer failed: status=Closed; position=128",
		},
		{
			name: "timeout",
			err: &aeron.OfferTimeoutError{
				Status:           aeron.BackPressured,
				PublicationRegID: publicationRegID,
				StreamID:         streamID,
				SessionID:        sessionID,
				Position:         0,
				Timeout:          10 * time.Second,
			},
			want: pub + " offer timed out within 10s; position=0; status=BackPressured",
		},
		{
			name: "stall",
			err: &aeron.OfferStallError{
				Status:           aeron.BackPressured,
				PublicationRegID: publicationRegID,
				StreamID:         streamID,
				SessionID:        sessionID,
				Position:         0,
				StallTimeout:     2 * time.Second,
			},
			want: pub + " offer stalled within 2s; position=0; status=BackPressured",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			require.EqualError(t, testCase.err, testCase.want)
		})
	}
}

func TestPollBufferTooSmallError_is(t *testing.T) {
	err := &aeron.PollBufferTooSmallError{Required: 8, Have: 4}
	require.ErrorIs(t, err, aeron.ErrPollBufferTooSmall)
}

func TestErrInvalidTimeout(t *testing.T) {
	require.EqualError(t, aeron.ErrInvalidTimeout, "aeron: invalid timeout")
}

func TestErrInvalidWant(t *testing.T) {
	require.EqualError(t, aeron.ErrInvalidWant, "aeron: invalid want")
}

func TestMapConductorError(t *testing.T) {
	staleErr := &conductor.DriverKeepaliveStaleError{
		AgeMs:     11_000,
		TimeoutMs: 10_000,
	}
	otherErr := errors.New("aeron: other failure")

	tests := []struct {
		name     string
		input    error
		wantSame bool
		wantMsg  string
	}{
		{
			name:  "driver keepalive stale",
			input: staleErr,
			wantMsg: "aeron: media driver timeout: conductor: media driver keepalive stale: " +
				"age=11000ms timeout=10000ms",
		},
		{
			name:    "driver shutdown",
			input:   conductor.ErrDriverShutdown,
			wantMsg: "aeron: media driver timeout: conductor: media driver has been shutdown",
		},
		{
			name:     "client timeout",
			input:    conductor.ErrClientTimeout,
			wantSame: true,
		},
		{
			name:     "conductor service timeout",
			input:    conductor.ErrConductorServiceTimeout,
			wantSame: true,
		},
		{
			name: "heartbeat counter closed",
			input: &conductor.HeartbeatCounterClosedError{
				CounterID: 3,
			},
			wantSame: true,
		},
		{
			name:     "pass through",
			input:    otherErr,
			wantSame: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapConductorErrorForTest(testCase.input)
			if testCase.wantSame {
				require.Same(t, testCase.input, mapped)
				return
			}

			var timeoutErr *aeron.DriverTimeoutError
			require.ErrorAs(t, mapped, &timeoutErr)
			require.Equal(t, testCase.input, timeoutErr.Unwrap())
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
}
