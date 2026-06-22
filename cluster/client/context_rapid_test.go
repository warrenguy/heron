//go:build rapid

package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/client"
)

const rapidMaxDuration = time.Hour

func drawOptionalDuration(t *rapid.T, name string) time.Duration {
	if rapid.Bool().Draw(t, name+"_zero") {
		return 0
	}
	n := rapid.Int64Range(1, int64(rapidMaxDuration)).Draw(t, name)
	return time.Duration(n)
}

func drawOptionalInt32(t *rapid.T, zeroLabel, valueLabel string) int32 {
	if rapid.Bool().Draw(t, zeroLabel) {
		return 0
	}
	return rapid.Int32Range(1, 1<<20).Draw(t, valueLabel)
}

func drawPartialClusterContext(t *rapid.T) client.ClusterContext {
	ctx := validClusterContext()

	ctx.IngressStreamID = drawOptionalInt32(t, "ingress_stream_zero", "ingress_stream_id")
	ctx.EgressStreamID = drawOptionalInt32(t, "egress_stream_zero", "egress_stream_id")
	ctx.ResponseStreamID = drawOptionalInt32(t, "response_stream_zero", "response_stream_id")
	ctx.MessageTimeout = drawOptionalDuration(t, "message_timeout")
	ctx.NewLeaderTimeout = drawOptionalDuration(t, "new_leader_timeout")

	switch rapid.IntRange(0, 2).Draw(t, "ingress_exclusive_mode") {
	case 0:
		// nil -> true at normalize
	case 1:
		exclusive := true
		ctx.IngressExclusive = &exclusive
	case 2:
		exclusive := false
		ctx.IngressExclusive = &exclusive
	}

	ctx.AeronDir = rapid.StringN(0, 8, 64).Draw(t, "aeron_dir")

	return ctx
}

func normalizeDrawnContext(t *rapid.T) (drawn, normalized client.ClusterContext) {
	drawn = drawPartialClusterContext(t)
	normalized = drawn.Normalize()
	return drawn, normalized
}

func requireZeroOrPreserved[T comparable](t *rapid.T, drawn, got, defaultIfZero T) {
	var zero T
	if drawn == zero {
		require.Equal(t, defaultIfZero, got)
		return
	}
	require.Equal(t, drawn, got)
}

func TestNormalizeClusterContext_idempotent_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ctx := drawPartialClusterContext(t)
		once := ctx.Normalize()
		twice := once.Normalize()
		require.Equal(t, once, twice)
	})
}

func TestNormalizeClusterContext_streamDefaults_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ctx, normalized := normalizeDrawnContext(t)

		requireZeroOrPreserved(
			t, ctx.IngressStreamID, normalized.IngressStreamID, client.DefaultIngressStreamID,
		)
		requireZeroOrPreserved(
			t, ctx.EgressStreamID, normalized.EgressStreamID, client.DefaultEgressStreamID,
		)
		requireZeroOrPreserved(
			t, ctx.ResponseStreamID, normalized.ResponseStreamID, normalized.EgressStreamID,
		)
	})
}

func TestNormalizeClusterContext_timeouts_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ctx, normalized := normalizeDrawnContext(t)

		requireZeroOrPreserved(
			t, ctx.MessageTimeout, normalized.MessageTimeout, client.DefaultMessageTimeout,
		)
		requireZeroOrPreserved(
			t, ctx.NewLeaderTimeout, normalized.NewLeaderTimeout, normalized.MessageTimeout,
		)
	})
}

func TestNormalizeClusterContext_ingressExclusive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ctx, normalized := normalizeDrawnContext(t)

		require.NotNil(t, normalized.IngressExclusive)
		if ctx.IngressExclusive == nil {
			require.True(t, *normalized.IngressExclusive)
		} else {
			require.Equal(t, *ctx.IngressExclusive, *normalized.IngressExclusive)
		}
	})
}

func TestNormalizeClusterContext_credentialsAndIdleStrategy_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		_, normalized := normalizeDrawnContext(t)

		require.IsType(t, client.NullCredentialsSupplier{}, normalized.CredentialsSupplier)
		require.NotNil(t, normalized.IdleStrategy)
	})
}

func TestNormalizeClusterContext_aeronDirMerge_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ctx, normalized := normalizeDrawnContext(t)

		if ctx.AeronDir != "" {
			require.Equal(t, ctx.AeronDir, normalized.AeronContext.AeronDir)
		}
	})
}
