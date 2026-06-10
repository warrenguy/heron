package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
)

var rapidPendingKinds = []conductor.PendingKind{
	conductor.PendingKindNextSessionID,
	conductor.PendingKindOperationSuccess,
	conductor.PendingKindAddPublication,
	conductor.PendingKindAddSubscription,
	conductor.PendingKindRemovePublication,
	conductor.PendingKindRemoveSubscription,
}

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

type pendingRapidSeen struct {
	kind     conductor.PendingKind
	streamID int32
	channel  string
}

func registerPendingForRapid(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID int64,
	streamID int32,
	channel string,
) error {
	rt.Helper()

	return registerPendingKindWithDeadlineForTest(
		conductorInstance,
		kind,
		correlationID,
		streamID,
		channel,
		time.Second,
	)
}

func TestConductor_RegisterPending_correlationUniqueness_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		conductorInstance := newConductorForRapid(rt)
		count := rapid.IntRange(1, 32).Draw(rt, "count")
		seen := make(map[int64]pendingRapidSeen, count)

		for range count {
			correlationID := rapid.Int64().Draw(rt, "correlationID")
			if _, exists := seen[correlationID]; exists {
				continue
			}

			kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, "kind")
			var streamID int32
			var channel string
			switch kind {
			case conductor.PendingKindAddPublication, conductor.PendingKindAddSubscription:
				streamID = rapid.Int32().Draw(rt, "streamID")
				channel = rapid.String().Draw(rt, "channel")
			}
			require.NoError(rt, registerPendingForRapid(
				rt,
				conductorInstance,
				kind,
				correlationID,
				streamID,
				channel,
			))
			seen[correlationID] = pendingRapidSeen{
				kind:     kind,
				streamID: streamID,
				channel:  channel,
			}
		}

		for correlationID, entry := range seen {
			got, ok := conductorInstance.PendingResult(correlationID)
			require.True(rt, ok)
			require.Equal(rt, correlationID, got.CorrelationID)
			require.Equal(rt, entry.kind, got.Kind)
			switch entry.kind {
			case conductor.PendingKindAddPublication, conductor.PendingKindAddSubscription:
				require.Equal(rt, entry.streamID, got.StreamID)
				require.Equal(rt, entry.channel, got.Channel)
			}
		}

		for correlationID, entry := range seen {
			err := registerPendingForRapid(
				rt,
				conductorInstance,
				entry.kind,
				correlationID,
				rapid.Int32().Draw(rt, "dupStreamID"),
				rapid.String().Draw(rt, "dupChannel"),
			)
			require.ErrorIs(rt, err, conductor.ErrPendingAlreadyRegistered)
			require.EqualError(rt, err, "conductor: correlation already pending")
		}
	})
}
