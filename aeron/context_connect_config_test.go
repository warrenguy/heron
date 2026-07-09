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
	ctx := Context{
		ErrorHandler: func(err error) { received = err },
	}
	config := ConductorConnectConfigForTest(ctx, testutil.TestClientID)
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
