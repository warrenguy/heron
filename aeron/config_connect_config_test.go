package aeron

import (
	"testing"

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
