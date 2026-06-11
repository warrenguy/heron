package aeron_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

const rapidMaxDuration = time.Hour

func drawOptionalDuration(t *rapid.T, name string) time.Duration {
	if rapid.Bool().Draw(t, name+"_zero") {
		return 0
	}
	n := rapid.Int64Range(1, int64(rapidMaxDuration)).Draw(t, name)
	return time.Duration(n)
}

func drawPartialContext(t *rapid.T) (
	ctx aeron.Context,
	epochMs int64,
	nanoNs int64,
	customEpoch bool,
	customNano bool,
) {
	ctx.AeronDir = rapid.StringN(0, 32, 64).Draw(t, "aeron_dir")
	ctx.DriverTimeout = drawOptionalDuration(t, "driver_timeout")
	ctx.KeepAliveInterval = drawOptionalDuration(t, "keep_alive")
	ctx.InterServiceTimeout = drawOptionalDuration(t, "inter_service")
	ctx.IdleSleepDuration = drawOptionalDuration(t, "idle_sleep")
	ctx.CloseLingerDuration = drawOptionalDuration(t, "close_linger")

	customEpoch = rapid.Bool().Draw(t, "custom_epoch")
	if customEpoch {
		epochMs = rapid.Int64Range(1, 1<<40).Draw(t, "epoch_ms")
		ctx.EpochClock = func() int64 {
			return epochMs
		}
	}

	customNano = rapid.Bool().Draw(t, "custom_nano")
	if customNano {
		nanoNs = rapid.Int64Range(1, 1<<40).Draw(t, "nano_ns")
		ctx.NanoClock = func() int64 {
			return nanoNs
		}
	}

	if rapid.Bool().Draw(t, "use_conductor_invoker") {
		enabled := rapid.Bool().Draw(t, "conductor_invoker_enabled")
		ctx.UseConductorInvoker = &enabled
	}

	return ctx, epochMs, nanoNs, customEpoch, customNano
}

func TestNormalizeContext_durationFields_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx, _, _, _, _ := drawPartialContext(t)
		normalized := aeron.NormalizeContextForTest(ctx)
		want := aeron.NormalizeContextForTest(aeron.Context{
			DriverTimeout:       ctx.DriverTimeout,
			KeepAliveInterval:   ctx.KeepAliveInterval,
			InterServiceTimeout: ctx.InterServiceTimeout,
			IdleSleepDuration:   ctx.IdleSleepDuration,
			CloseLingerDuration: ctx.CloseLingerDuration,
		})

		require.Equal(t, want.DriverTimeout, normalized.DriverTimeout)
		require.Equal(t, want.KeepAliveInterval, normalized.KeepAliveInterval)
		require.Equal(t, want.InterServiceTimeout, normalized.InterServiceTimeout)
		require.Equal(t, want.IdleSleepDuration, normalized.IdleSleepDuration)
		require.Equal(t, want.CloseLingerDuration, normalized.CloseLingerDuration)
	})
}

func TestNormalizeContext_clocks_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx, epochMs, nanoNs, customEpoch, customNano := drawPartialContext(t)
		normalized := aeron.NormalizeContextForTest(ctx)

		require.NotNil(t, normalized.EpochClock)
		require.NotNil(t, normalized.NanoClock)

		if customEpoch {
			require.Equal(t, epochMs, normalized.EpochClock())
		} else {
			require.Positive(t, normalized.EpochClock())
		}

		if customNano {
			require.Equal(t, nanoNs, normalized.NanoClock())
		} else {
			require.Positive(t, normalized.NanoClock())
		}
	})
}

func requireNormalizedContextsEqual(
	t *rapid.T,
	want, got aeron.Context,
	customEpoch, customNano bool,
	epochMs, nanoNs int64,
) {
	require.Equal(t, want.AeronDir, got.AeronDir)
	require.Equal(t, want.DriverTimeout, got.DriverTimeout)
	require.Equal(t, want.KeepAliveInterval, got.KeepAliveInterval)
	require.Equal(t, want.InterServiceTimeout, got.InterServiceTimeout)
	require.Equal(t, want.IdleSleepDuration, got.IdleSleepDuration)
	require.Equal(t, want.CloseLingerDuration, got.CloseLingerDuration)
	require.Equal(t, want.UseConductorInvoker, got.UseConductorInvoker)
	require.NotNil(t, want.EpochClock)
	require.NotNil(t, got.EpochClock)
	require.NotNil(t, want.NanoClock)
	require.NotNil(t, got.NanoClock)
	require.Equal(
		t,
		reflect.ValueOf(want.EpochClock).Pointer(),
		reflect.ValueOf(got.EpochClock).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(want.NanoClock).Pointer(),
		reflect.ValueOf(got.NanoClock).Pointer(),
	)
	if customEpoch {
		require.Equal(t, epochMs, want.EpochClock())
		require.Equal(t, epochMs, got.EpochClock())
	}
	if customNano {
		require.Equal(t, nanoNs, want.NanoClock())
		require.Equal(t, nanoNs, got.NanoClock())
	}
}

func TestNormalizeContext_idempotent_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx, epochMs, nanoNs, customEpoch, customNano := drawPartialContext(t)
		once := aeron.NormalizeContextForTest(ctx)
		twice := aeron.NormalizeContextForTest(once)
		requireNormalizedContextsEqual(t, once, twice, customEpoch, customNano, epochMs, nanoNs)
	})
}
