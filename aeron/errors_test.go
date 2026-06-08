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
		name  string
		cause error
	}{
		{name: "not ready", cause: aeron.ErrDriverHeartbeatNotReady},
		{name: "stale at connect", cause: aeron.ErrDriverHeartbeatStaleAtConnect},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := &aeron.DriverTimeoutError{Cause: testCase.cause}
			require.ErrorIs(t, err, testCase.cause)
		})
	}
}

func TestRegistrationError(t *testing.T) {
	err := &aeron.RegistrationError{
		CorrelationID: 9,
		ErrorCode:     3,
		Message:       "channel invalid",
	}
	require.EqualError(
		t,
		err,
		"aeron: driver registration error: correlation=9 code=3 message=channel invalid",
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
		name    string
		input   error
		wantErr error
		wantMsg string
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
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapRegistrationErrorForTest(testCase.input)
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
				require.Equal(t, testCase.input, mapped)
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
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
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
			name:     "pass through",
			input:    otherErr,
			wantSame: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped := aeron.MapConductorErrorForTest(testCase.input)
			if testCase.wantSame {
				require.Equal(t, testCase.input, mapped)
				return
			}

			var timeoutErr *aeron.DriverTimeoutError
			require.ErrorAs(t, mapped, &timeoutErr)
			require.Equal(t, testCase.input, timeoutErr.Unwrap())
			require.EqualError(t, mapped, testCase.wantMsg)
		})
	}
}
