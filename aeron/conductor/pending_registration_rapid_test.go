package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
)

func newConductorForRapid(rt *rapid.T) *conductor.Conductor {
	rt.Helper()

	var nanoNs int64
	config := conductor.Config{
		ClientID:            conductorTestClientID,
		DriverTimeout:       conductorTestDriverTimeout,
		KeepAliveInterval:   conductorTestKeepAliveInterval,
		IdleSleepDuration:   conductorTestIdleSleepDuration,
		InterServiceTimeout: conductorTestInterServiceTimeout,
		EpochClock: func() int64 {
			return 0
		},
		NanoClock: func() int64 {
			return nanoNs
		},
	}
	conductorInstance, err := conductor.NewConductorUnchecked(config, nil)
	require.NoError(rt, err)
	return conductorInstance
}

func TestConductor_RegisterPending_correlationUniqueness_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		conductorInstance := newConductorForRapid(rt)
		count := rapid.IntRange(1, 32).Draw(rt, "count")
		seen := make(map[int64]struct{}, count)

		for range count {
			correlationID := rapid.Int64().Draw(rt, "correlationID")
			if _, exists := seen[correlationID]; exists {
				continue
			}
			seen[correlationID] = struct{}{}

			err := conductorInstance.RegisterPending(
				conductor.PendingKindNextSessionID,
				correlationID,
				time.Second,
			)
			require.NoError(rt, err)
		}

		for correlationID := range seen {
			got, ok := conductorInstance.PendingResult(correlationID)
			require.True(rt, ok)
			require.Equal(rt, correlationID, got.CorrelationID)
		}

		for correlationID := range seen {
			err := conductorInstance.RegisterPending(
				conductor.PendingKindOperationSuccess,
				correlationID,
				time.Second,
			)
			require.ErrorIs(rt, err, conductor.ErrPendingAlreadyRegistered)
			require.EqualError(rt, err, "conductor: correlation already pending")
		}
	})
}
