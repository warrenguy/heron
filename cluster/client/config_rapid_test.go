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

func drawPartialConfig(t *rapid.T) client.Config {
	cfg := validConfig()

	cfg.IngressStreamID = drawOptionalInt32(t, "ingress_stream_zero", "ingress_stream_id")
	cfg.EgressStreamID = drawOptionalInt32(t, "egress_stream_zero", "egress_stream_id")
	cfg.ResponseStreamID = drawOptionalInt32(t, "response_stream_zero", "response_stream_id")
	cfg.MessageTimeout = drawOptionalDuration(t, "message_timeout")
	cfg.NewLeaderTimeout = drawOptionalDuration(t, "new_leader_timeout")
	cfg.IngressRetryTimeout = drawOptionalDuration(t, "ingress_retry_timeout")

	switch rapid.IntRange(0, 2).Draw(t, "ingress_exclusive_mode") {
	case 0:
		// nil -> true at normalize
	case 1:
		exclusive := true
		cfg.IngressExclusive = &exclusive
	case 2:
		exclusive := false
		cfg.IngressExclusive = &exclusive
	}

	cfg.AeronDir = rapid.StringN(0, 8, 64).Draw(t, "aeron_dir")

	return cfg
}

func normalizeDrawnConfig(t *rapid.T) (drawn, normalized client.Config) {
	drawn = drawPartialConfig(t)
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

func TestNormalizeConfig_idempotent_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg := drawPartialConfig(t)
		once := cfg.Normalize()
		twice := once.Normalize()
		require.Equal(t, once, twice)
	})
}

func TestNormalizeConfig_streamDefaults_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, normalized := normalizeDrawnConfig(t)

		requireZeroOrPreserved(
			t, cfg.IngressStreamID, normalized.IngressStreamID, client.DefaultIngressStreamID,
		)
		requireZeroOrPreserved(
			t, cfg.EgressStreamID, normalized.EgressStreamID, client.DefaultEgressStreamID,
		)
		requireZeroOrPreserved(
			t, cfg.ResponseStreamID, normalized.ResponseStreamID, normalized.EgressStreamID,
		)
	})
}

func TestNormalizeConfig_timeouts_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, normalized := normalizeDrawnConfig(t)

		requireZeroOrPreserved(
			t, cfg.MessageTimeout, normalized.MessageTimeout, client.DefaultMessageTimeout,
		)
		requireZeroOrPreserved(
			t, cfg.NewLeaderTimeout, normalized.NewLeaderTimeout, normalized.MessageTimeout,
		)
		requireZeroOrPreserved(
			t,
			cfg.IngressRetryTimeout,
			normalized.IngressRetryTimeout,
			client.DefaultIngressRetryTimeout,
		)
	})
}

func TestNormalizeConfig_ingressExclusive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, normalized := normalizeDrawnConfig(t)

		require.NotNil(t, normalized.IngressExclusive)
		if cfg.IngressExclusive == nil {
			require.True(t, *normalized.IngressExclusive)
		} else {
			require.Equal(t, *cfg.IngressExclusive, *normalized.IngressExclusive)
		}
	})
}

func TestNormalizeConfig_credentialsAndIdleStrategy_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		_, normalized := normalizeDrawnConfig(t)

		require.IsType(t, client.NullCredentialsSupplier{}, normalized.CredentialsSupplier)
		require.NotNil(t, normalized.IdleStrategy)
	})
}

func TestNormalizeConfig_aeronDirMerge_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, normalized := normalizeDrawnConfig(t)

		if cfg.AeronDir != "" {
			require.Equal(t, cfg.AeronDir, normalized.AeronConfig.AeronDir)
		}
	})
}
