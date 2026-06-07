package aeron_test

import (
	"errors"
	"testing"

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
