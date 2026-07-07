package aeron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestNormalizeContext_defaults(t *testing.T) {
	normalized := aeron.NormalizeContextForTest(aeron.Context{})

	require.Equal(t, 10*time.Second, normalized.DriverTimeout)
	require.Equal(t, 500*time.Millisecond, normalized.KeepAliveInterval)
	require.Equal(t, time.Second, normalized.InterServiceTimeout)
	require.Equal(t, 16*time.Millisecond, normalized.IdleSleepDuration)
	require.Equal(t, 3*time.Second, normalized.CloseLingerDuration)
	require.Equal(t, 10*time.Second, normalized.PollTimeout)
	require.Equal(t, 10*time.Second, normalized.PublicationOfferTimeout)
	require.NotNil(t, normalized.EpochClock)
	require.NotNil(t, normalized.NanoClock)
	require.Positive(t, normalized.EpochClock())
	require.Positive(t, normalized.NanoClock())
}

func TestNormalizeContext_zeroCloseLingerUsesDefault(t *testing.T) {
	normalized := aeron.NormalizeContextForTest(aeron.Context{
		CloseLingerDuration: 0,
	})

	require.Equal(t, 3*time.Second, normalized.CloseLingerDuration)
}

func TestNormalizeContext_preservesExplicitValues(t *testing.T) {
	epochMs := int64(42)
	nanoNs := int64(99)
	useInvoker := false

	normalized := aeron.NormalizeContextForTest(aeron.Context{
		DriverTimeout:           3 * time.Second,
		KeepAliveInterval:       250 * time.Millisecond,
		InterServiceTimeout:     2 * time.Second,
		IdleSleepDuration:       8 * time.Millisecond,
		CloseLingerDuration:     7 * time.Second,
		PollTimeout:             4 * time.Second,
		PublicationOfferTimeout: 3 * time.Second,
		EpochClock: func() int64 {
			return epochMs
		},
		NanoClock: func() int64 {
			return nanoNs
		},
		UseConductorInvoker: &useInvoker,
	})

	require.Equal(t, 3*time.Second, normalized.DriverTimeout)
	require.Equal(t, 250*time.Millisecond, normalized.KeepAliveInterval)
	require.Equal(t, 2*time.Second, normalized.InterServiceTimeout)
	require.Equal(t, 8*time.Millisecond, normalized.IdleSleepDuration)
	require.Equal(t, 7*time.Second, normalized.CloseLingerDuration)
	require.Equal(t, 4*time.Second, normalized.PollTimeout)
	require.Equal(t, 3*time.Second, normalized.PublicationOfferTimeout)
	require.Equal(t, epochMs, normalized.EpochClock())
	require.Equal(t, nanoNs, normalized.NanoClock())
	require.False(t, normalized.UseConductorInvokerEnabledForTest())
}
