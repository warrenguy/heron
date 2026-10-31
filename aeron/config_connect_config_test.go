package aeron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestConductorConnectConfig_errorHandlerWrapsMappedErrors(t *testing.T) {
	t.Parallel()

	var received error
	cfg := Config{
		ErrorHandler: func(err error) { received = err },
	}
	config := ConductorConnectConfigForTest(cfg, testutil.TestClientID)
	require.NotNil(t, config.ErrorHandler)

	staleErr := &conductor.DriverKeepaliveStaleError{
		AgeMs:     11_000,
		TimeoutMs: 10_000,
	}
	config.ErrorHandler(staleErr)

	var timeoutErr *DriverTimeoutError
	require.ErrorAs(t, received, &timeoutErr)
	require.ErrorIs(t, received, staleErr)
	require.NotSame(t, staleErr, received)
	require.Equal(t, MapConductorErrorForTest(staleErr), received)
}

func TestConductorConnectConfig_resourceLinger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		linger *time.Duration
		want   int64
	}{
		{
			name: "nil uses the default",
			want: (3 * time.Second).Nanoseconds(),
		},
		{
			name:   "zero unmaps at release",
			linger: new(time.Duration(0)),
			want:   0,
		},
		{
			name:   "explicit value passes through",
			linger: new(7 * time.Second),
			want:   (7 * time.Second).Nanoseconds(),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			config := ConductorConnectConfigForTest(Config{
				ResourceLingerDuration: testCase.linger,
			}, testutil.TestClientID)
			require.Equal(t, testCase.want, config.ResourceLingerNs)
		})
	}
}

func TestConductorConnectConfig_publicationErrorFrameHandler(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		config := ConductorConnectConfigForTest(Config{}, testutil.TestClientID)
		require.Nil(t, config.PublicationErrorFrameHandler)
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()

		var frames []PublicationErrorFrame
		cfg := Config{
			PublicationErrorFrameHandler: func(frame PublicationErrorFrame) {
				frames = append(frames, frame)
			},
		}
		config := ConductorConnectConfigForTest(cfg, testutil.TestClientID)
		require.NotNil(t, config.PublicationErrorFrameHandler)

		event := publicationErrorFrameTestEvent(clientAddPublicationRegistrationID)
		config.PublicationErrorFrameHandler(event)
		require.Equal(t, []PublicationErrorFrame{publicationErrorFrameFrom(event)}, frames)
	})
}
