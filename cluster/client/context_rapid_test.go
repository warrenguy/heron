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
	clusterCtx := validClusterContext()

	clusterCtx.IngressStreamID = drawOptionalInt32(t, "ingress_stream_zero", "ingress_stream_id")
	clusterCtx.EgressStreamID = drawOptionalInt32(t, "egress_stream_zero", "egress_stream_id")
	clusterCtx.ResponseStreamID = drawOptionalInt32(t, "response_stream_zero", "response_stream_id")
	clusterCtx.MessageTimeout = drawOptionalDuration(t, "message_timeout")
	clusterCtx.NewLeaderTimeout = drawOptionalDuration(t, "new_leader_timeout")
	clusterCtx.IngressRetryTimeout = drawOptionalDuration(t, "ingress_retry_timeout")

	switch rapid.IntRange(0, 2).Draw(t, "ingress_exclusive_mode") {
	case 0:
		// nil -> true at normalize
	case 1:
		exclusive := true
		clusterCtx.IngressExclusive = &exclusive
	case 2:
		exclusive := false
		clusterCtx.IngressExclusive = &exclusive
	}

	clusterCtx.AeronDir = rapid.StringN(0, 8, 64).Draw(t, "aeron_dir")

	return clusterCtx
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
		clusterCtx := drawPartialClusterContext(t)
		once := clusterCtx.Normalize()
		twice := once.Normalize()
		require.Equal(t, once, twice)
	})
}

func TestNormalizeClusterContext_streamDefaults_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		clusterCtx, normalized := normalizeDrawnContext(t)

		requireZeroOrPreserved(
			t, clusterCtx.IngressStreamID, normalized.IngressStreamID, client.DefaultIngressStreamID,
		)
		requireZeroOrPreserved(
			t, clusterCtx.EgressStreamID, normalized.EgressStreamID, client.DefaultEgressStreamID,
		)
		requireZeroOrPreserved(
			t, clusterCtx.ResponseStreamID, normalized.ResponseStreamID, normalized.EgressStreamID,
		)
	})
}

func TestNormalizeClusterContext_timeouts_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		clusterCtx, normalized := normalizeDrawnContext(t)

		requireZeroOrPreserved(
			t, clusterCtx.MessageTimeout, normalized.MessageTimeout, client.DefaultMessageTimeout,
		)
		requireZeroOrPreserved(
			t, clusterCtx.NewLeaderTimeout, normalized.NewLeaderTimeout, normalized.MessageTimeout,
		)
		requireZeroOrPreserved(
			t,
			clusterCtx.IngressRetryTimeout,
			normalized.IngressRetryTimeout,
			client.DefaultIngressRetryTimeout,
		)
	})
}

func TestNormalizeClusterContext_ingressExclusive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		clusterCtx, normalized := normalizeDrawnContext(t)

		require.NotNil(t, normalized.IngressExclusive)
		if clusterCtx.IngressExclusive == nil {
			require.True(t, *normalized.IngressExclusive)
		} else {
			require.Equal(t, *clusterCtx.IngressExclusive, *normalized.IngressExclusive)
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
		clusterCtx, normalized := normalizeDrawnContext(t)

		if clusterCtx.AeronDir != "" {
			require.Equal(t, clusterCtx.AeronDir, normalized.AeronContext.AeronDir)
		}
	})
}
