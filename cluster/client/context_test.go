package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

func validClusterContext() client.ClusterContext {
	return client.ClusterContext{
		IngressChannel:      "aeron:udp?endpoint=localhost:20110",
		EgressChannel:       "aeron:udp?endpoint=localhost:20111",
		EgressHandler:       &client.EgressHandlers{},
		CredentialsSupplier: client.NullCredentialsSupplier{},
		IdleStrategy:        client.NewDefaultIdleStrategy(),
	}
}

func clusterContext(mutate func(*client.ClusterContext)) client.ClusterContext {
	ctx := validClusterContext()
	if mutate != nil {
		mutate(&ctx)
	}
	return ctx
}

func normalizedClusterContext(mutate func(*client.ClusterContext)) client.ClusterContext {
	return clusterContext(mutate).Normalize()
}

func TestNormalizeClusterContext_defaults(t *testing.T) {
	ctx := normalizedClusterContext(nil)

	require.Equal(t, client.DefaultIngressStreamID, ctx.IngressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, ctx.EgressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, ctx.ResponseStreamID)
	require.Equal(t, client.DefaultMessageTimeout, ctx.MessageTimeout)
	require.Equal(t, client.DefaultMessageTimeout, ctx.NewLeaderTimeout)
	require.IsType(t, client.NullCredentialsSupplier{}, ctx.CredentialsSupplier)
	require.NotNil(t, ctx.IdleStrategy)
	require.NotNil(t, ctx.IngressExclusive)
	require.True(t, *ctx.IngressExclusive)
}

func TestNormalizeClusterContext_newLeaderTimeoutZeroUsesMessageTimeout(t *testing.T) {
	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.MessageTimeout = 7 * time.Second
	})

	require.Equal(t, 7*time.Second, ctx.NewLeaderTimeout)
}

func TestNormalizeClusterContext_responseStreamIDDefaultsToEgress(t *testing.T) {
	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.EgressStreamID = 200
		ctx.ResponseStreamID = 0
	})

	require.Equal(t, int32(200), ctx.ResponseStreamID)
}

func TestNormalizeClusterContext_preservesExplicitValues(t *testing.T) {
	exclusive := false
	customTimeout := 11 * time.Second
	customNewLeader := 22 * time.Second

	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.IngressStreamID = 111
		ctx.EgressStreamID = 222
		ctx.ResponseStreamID = 333
		ctx.MessageTimeout = customTimeout
		ctx.NewLeaderTimeout = customNewLeader
		ctx.IngressExclusive = &exclusive
	})

	require.Equal(t, int32(111), ctx.IngressStreamID)
	require.Equal(t, int32(222), ctx.EgressStreamID)
	require.Equal(t, int32(333), ctx.ResponseStreamID)
	require.Equal(t, customTimeout, ctx.MessageTimeout)
	require.Equal(t, customNewLeader, ctx.NewLeaderTimeout)
	require.NotNil(t, ctx.IngressExclusive)
	require.False(t, *ctx.IngressExclusive)
}

func TestNormalizeClusterContext_mergesAeronDirOnly(t *testing.T) {
	customEpoch := func() int64 { return 42 }
	customNano := func() int64 { return 99 }

	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.AeronDir = "/tmp/heron-aeron"
		ctx.AeronContext = aeron.Context{
			EpochClock: customEpoch,
			NanoClock:  customNano,
		}
	})

	require.Equal(t, "/tmp/heron-aeron", ctx.AeronContext.AeronDir)
	require.NotNil(t, ctx.AeronContext.EpochClock)
	require.Equal(t, int64(42), ctx.AeronContext.EpochClock())
	require.NotNil(t, ctx.AeronContext.NanoClock)
	require.Equal(t, int64(99), ctx.AeronContext.NanoClock())
}

func TestNormalizeClusterContext_preservesNestedAeronDir(t *testing.T) {
	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.AeronDir = "/tmp/top-level"
		ctx.AeronContext = aeron.Context{
			AeronDir: "/tmp/nested",
		}
	})

	require.Equal(t, "/tmp/nested", ctx.AeronContext.AeronDir)
}

func TestNormalizeClusterContext_ingressExclusiveMutationDoesNotAffectFreshDefault(
	t *testing.T,
) {
	normalized := normalizedClusterContext(nil)
	require.NotNil(t, normalized.IngressExclusive)

	falseVal := false
	*normalized.IngressExclusive = falseVal

	fresh := client.ClusterContext{}.Normalize()
	require.NotNil(t, fresh.IngressExclusive)
	require.True(t, *fresh.IngressExclusive)
}

func TestNormalizeClusterContext_idempotent(t *testing.T) {
	initial := validClusterContext()
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateClusterContext_errors(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(*client.ClusterContext)
		afterNormalize func(*client.ClusterContext)
		wantErr        string
	}{
		{
			name: "missing ingress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = ""
			},
			wantErr: "cluster client: IngressChannel is required",
		},
		{
			name: "missing egress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = ""
			},
			wantErr: "cluster client: EgressChannel is required",
		},
		{
			name: "ipc ingress with endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron:ipc"
				ctx.IngressEndpoints = "0=localhost:20110"
			},
			wantErr: "cluster client: IngressEndpoints must be empty for IPC ingress",
		},
		{
			name: "ipc ingress with query and endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron:ipc?alias=ingress"
				ctx.IngressEndpoints = "0=localhost:20110"
			},
			wantErr: "cluster client: IngressEndpoints must be empty for IPC ingress",
		},
		{
			name: "spy ipc ingress with endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron-spy:aeron:ipc"
				ctx.IngressEndpoints = "0=localhost:20110"
			},
			wantErr: "cluster client: IngressEndpoints must be empty for IPC ingress",
		},
		{
			name: "client name too long",
			mutate: func(ctx *client.ClusterContext) {
				ctx.ClientName = string(make([]byte, client.MaxClientNameLength+1))
			},
			wantErr: "cluster client: ClientName length must be <= 100",
		},
		{
			name: "non positive message timeout",
			mutate: func(ctx *client.ClusterContext) {
				ctx.MessageTimeout = -time.Second
			},
			wantErr: "cluster client: MessageTimeout must be positive",
		},
		{
			name: "negative new leader timeout",
			mutate: func(ctx *client.ClusterContext) {
				ctx.NewLeaderTimeout = -time.Second
			},
			wantErr: "cluster client: NewLeaderTimeout must not be negative",
		},
		{
			name: "nil egress handler",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressHandler = nil
			},
			wantErr: "cluster client: EgressHandler is required",
		},
		{
			name: "typed nil egress handlers",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressHandler = (*client.EgressHandlers)(nil)
			},
			wantErr: "cluster client: EgressHandler is required",
		},
		{
			name: "nil credentials supplier",
			mutate: func(ctx *client.ClusterContext) {
				ctx.CredentialsSupplier = nil
			},
			afterNormalize: func(ctx *client.ClusterContext) {
				// Bypass normalize default that would mask the validation branch.
				ctx.CredentialsSupplier = nil
			},
			wantErr: "cluster client: CredentialsSupplier is required",
		},
		{
			name: "nil idle strategy",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IdleStrategy = nil
			},
			afterNormalize: func(ctx *client.ClusterContext) {
				ctx.IdleStrategy = nil
			},
			wantErr: "cluster client: IdleStrategy is required",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := clusterContext(testCase.mutate)
			ctx = ctx.Normalize()
			if testCase.afterNormalize != nil {
				testCase.afterNormalize(&ctx)
			}

			err := ctx.Validate()
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateClusterContext_zeroMessageTimeoutWithoutNormalize(t *testing.T) {
	ctx := validClusterContext()
	ctx.MessageTimeout = 0

	err := ctx.Validate()
	require.EqualError(t, err, "cluster client: MessageTimeout must be positive")
}

func TestValidateClusterContext_zeroNewLeaderTimeoutWithoutNormalize(t *testing.T) {
	ctx := validClusterContext()
	ctx.MessageTimeout = 7 * time.Second
	ctx.NewLeaderTimeout = 0

	require.NoError(t, ctx.Validate())
}

func TestValidateClusterContext_validCases(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.ClusterContext)
	}{
		{
			name: "client name at max length",
			mutate: func(ctx *client.ClusterContext) {
				ctx.ClientName = string(make([]byte, client.MaxClientNameLength))
			},
		},
		{
			name: "ipc ingress empty endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron:ipc"
				ctx.IngressEndpoints = ""
			},
		},
		{
			name: "ipc ingress with query empty endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron:ipc?alias=ingress"
				ctx.IngressEndpoints = ""
			},
		},
		{
			name: "ipcx ingress with endpoints",
			mutate: func(ctx *client.ClusterContext) {
				ctx.IngressChannel = "aeron:ipcx"
				ctx.IngressEndpoints = "0=localhost:20110"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedClusterContext(testCase.mutate)
			require.NoError(t, ctx.Validate())
		})
	}
}

func TestOwnsAeronClient(t *testing.T) {
	external := &aeron.Client{}
	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.Aeron = external
	})

	require.False(t, ctx.OwnsAeronClient())

	owned := normalizedClusterContext(nil)
	require.True(t, owned.OwnsAeronClient())
}

func TestProtocolSemanticVersion(t *testing.T) {
	version := client.ProtocolSemanticVersion()
	require.Equal(t, int32(768), version)
	require.Equal(t, cnc.ComposeSemanticVersion(0, 3, 0), version)

	actingVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)
	require.GreaterOrEqual(t, actingVersion, uint16(2))

	fields := codecs.SessionConnectRequestFields{
		CorrelationID:    1,
		ResponseStreamID: client.DefaultEgressStreamID,
		Version:          version,
	}
	buf := make([]byte, 512)
	end, ok := codecs.EncodeSessionConnectRequest(buf, 0, actingVersion, fields)
	require.True(t, ok)
	require.Positive(t, end)

	decoded, ok := codecs.TryDecodeByTemplate(buf, 0, codecs.ClusterMaxVarLen)
	require.True(t, ok)
	require.Equal(t, gensbe.SessionConnectRequestSbeTemplateID, decoded.TemplateID)

	gotFields, ok := decoded.Fields.(codecs.SessionConnectRequestFields)
	require.True(t, ok)
	require.Equal(t, int32(768), gotFields.Version)
}

func TestSessionHeaderLength(t *testing.T) {
	const golden = 32

	require.Equal(t, golden, client.SessionHeaderLength())
	require.Equal(t, golden, codecs.SessionMessageHeaderEncodedLen())
}
