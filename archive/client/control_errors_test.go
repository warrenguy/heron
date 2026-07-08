package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestControlTerminalError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int64
		want   string
	}{
		{
			name:   "backPressured",
			status: aeron.BackPressured,
			want:   "archive client: control offer failed: status=BackPressured",
		},
		{
			name:   "notConnected",
			status: aeron.NotConnected,
			want:   "archive client: control offer failed: status=NotConnected",
		},
		{
			name:   "closed",
			status: aeron.Closed,
			want:   "archive client: control offer failed: status=Closed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &ControlTerminalError{Status: tc.status}
			require.Equal(t, tc.want, err.Error())
		})
	}
}

func TestControlOfferTimeoutError_Error(t *testing.T) {
	t.Parallel()

	err := &ControlOfferTimeoutError{
		Status:  aeron.BackPressured,
		Timeout: 10 * time.Second,
	}
	require.Equal(
		t,
		"archive client: control offer timed out within 10s: status=BackPressured",
		err.Error(),
	)
}

func TestControlResponseTimeoutError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		want      string
	}{
		{
			name:      "controlResponse",
			operation: ControlResponseAwaitOperation,
			want:      "archive client: control response timeout within 5s: correlation_id=1234",
		},
		{
			name:      "listRecordingDescriptor",
			operation: ListRecordingDescriptorAwaitOperation,
			want:      "archive client: list recording descriptor timeout within 5s: correlation_id=1234",
		},
		{
			name:      "stopRecording",
			operation: StopRecordingAwaitOperation,
			want:      "archive client: stop recording timeout within 5s: correlation_id=1234",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &ControlResponseTimeoutError{
				Operation:     tc.operation,
				CorrelationID: 1234,
				Timeout:       5 * time.Second,
			}
			require.Equal(t, tc.want, err.Error())
		})
	}
}
