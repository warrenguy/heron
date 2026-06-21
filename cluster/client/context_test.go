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

const (
	testIngressEndpointsMap      = "0=localhost:20110"
	errPassthroughIngressWithMap = "cluster client: IngressEndpoints must be empty for " +
		"ipc/ipcx ingress"
	errUDPIngressEndpointWithMap = "cluster client: IngressChannel must not use ?endpoint= " +
		"when IngressEndpoints is configured"
	errEgressHandlerRequired    = "cluster client: EgressHandler is required"
	errUDPEgressResponseChannel = "cluster client: UDP egress response channel not " +
		"supported; use ipc or ipcx"
)

func withIngressChannelAndEndpoints(channel, endpoints string) func(*client.ClusterContext) {
	return func(ctx *client.ClusterContext) {
		ctx.IngressChannel = channel
		ctx.IngressEndpoints = endpoints
	}
}

func withNilEgressHandler(ctx *client.ClusterContext) {
	ctx.EgressHandler = nil
}

func withTypedNilEgressHandlers(ctx *client.ClusterContext) {
	ctx.EgressHandler = (*client.EgressHandlers)(nil)
}

func validClusterContext() client.ClusterContext {
	return client.ClusterContext{
		IngressChannel:      "aeron:udp?endpoint=localhost:20110",
		EgressChannel:       "aeron:ipc",
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

	*normalized.IngressExclusive = false

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
			name: "udp egress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = "aeron:udp?endpoint=localhost:20111"
			},
			wantErr: errUDPEgressResponseChannel,
		},
		{
			name: "spy udp egress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = "aeron-spy:aeron:udp?endpoint=localhost:0"
			},
			wantErr: errUDPEgressResponseChannel,
		},
		{
			name: "unknown egress media",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = "aeron:unknown"
			},
			wantErr: "cluster client: unknown egress response channel not supported; use ipc or ipcx",
		},
		{
			name: "invalid egress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = "not-a-channel"
			},
			wantErr: "cluster client: invalid egress channel: not-a-channel",
		},
		{
			name:    "ipc ingress with endpoints",
			mutate:  withIngressChannelAndEndpoints("aeron:ipc", testIngressEndpointsMap),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name: "ipc ingress with query and endpoints",
			mutate: withIngressChannelAndEndpoints(
				"aeron:ipc?alias=ingress", testIngressEndpointsMap,
			),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name: "spy ipc ingress with endpoints",
			mutate: withIngressChannelAndEndpoints(
				"aeron-spy:aeron:ipc", testIngressEndpointsMap,
			),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name:    "ipcx ingress with endpoints",
			mutate:  withIngressChannelAndEndpoints("aeron:ipcx", testIngressEndpointsMap),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name: "ipcx ingress endpoint query with endpoints map",
			mutate: withIngressChannelAndEndpoints(
				"aeron:ipcx?endpoint=localhost:0", testIngressEndpointsMap,
			),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name: "spy ipcx ingress with endpoints",
			mutate: withIngressChannelAndEndpoints(
				"aeron-spy:aeron:ipcx", testIngressEndpointsMap,
			),
			wantErr: errPassthroughIngressWithMap,
		},
		{
			name: "udp ingress endpoint with ingress endpoints map",
			mutate: withIngressChannelAndEndpoints(
				"aeron:udp?endpoint=localhost:0", testIngressEndpointsMap,
			),
			wantErr: errUDPIngressEndpointWithMap,
		},
		{
			name: "spy udp ingress endpoint with ingress endpoints map",
			mutate: withIngressChannelAndEndpoints(
				"aeron-spy:aeron:udp?endpoint=localhost:0", testIngressEndpointsMap,
			),
			wantErr: errUDPIngressEndpointWithMap,
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
			name:    "nil egress handler",
			mutate:  withNilEgressHandler,
			wantErr: errEgressHandlerRequired,
		},
		{
			name:    "typed nil egress handlers",
			mutate:  withTypedNilEgressHandlers,
			wantErr: errEgressHandlerRequired,
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
			ctx := normalizedClusterContext(testCase.mutate)
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

func TestValidateClusterContext_unparseableIngressWithEndpoints(t *testing.T) {
	tests := []struct {
		name    string
		channel string
	}{
		{name: "unknown media", channel: "aeron:unknown"},
		{name: "missing aeron prefix", channel: "not-a-channel"},
		{name: "empty media", channel: "aeron:"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedClusterContext(
				withIngressChannelAndEndpoints(testCase.channel, testIngressEndpointsMap),
			)
			wantErr := "cluster client: invalid ingress channel: " + testCase.channel

			for _, validate := range []func() error{ctx.Validate, ctx.ValidateForConnect} {
				require.EqualError(t, validate(), wantErr)
			}
		})
	}
}

func TestValidateForConnect_udpEgressRejected(t *testing.T) {
	ctx := normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.EgressChannel = "aeron:udp?endpoint=localhost:20111"
		ctx.EgressHandler = nil
	})

	require.EqualError(t, ctx.ValidateForConnect(), errUDPEgressResponseChannel)
}

func TestValidateForConnect_egressHandlerNotRequired(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.ClusterContext)
	}{
		{name: "nil", mutate: withNilEgressHandler},
		{name: "typed nil", mutate: withTypedNilEgressHandlers},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedClusterContext(testCase.mutate)

			require.NoError(t, ctx.ValidateForConnect())
			require.EqualError(t, ctx.Validate(), errEgressHandlerRequired)
		})
	}
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
			name:   "ipc ingress empty endpoints",
			mutate: withIngressChannelAndEndpoints("aeron:ipc", ""),
		},
		{
			name:   "ipc ingress with query empty endpoints",
			mutate: withIngressChannelAndEndpoints("aeron:ipc?alias=ingress", ""),
		},
		{
			name:   "udp ingress without endpoint with ingress endpoints map",
			mutate: withIngressChannelAndEndpoints("aeron:udp?ttl=16", testIngressEndpointsMap),
		},
		{
			name: "udp ingress piped endpoint with ingress endpoints map",
			mutate: withIngressChannelAndEndpoints(
				"aeron:udp?ttl=16|endpoint=localhost:0", testIngressEndpointsMap,
			),
		},
		{
			name: "udp ingress alias false positive endpoint with ingress endpoints map",
			mutate: withIngressChannelAndEndpoints(
				"aeron:udp?alias=endpoint=localhost:0", testIngressEndpointsMap,
			),
		},
		{
			name: "ipcx egress channel",
			mutate: func(ctx *client.ClusterContext) {
				ctx.EgressChannel = "aeron:ipcx?alias=egress"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedClusterContext(testCase.mutate)
			for _, validate := range []func() error{ctx.Validate, ctx.ValidateForConnect} {
				require.NoError(t, validate())
			}
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
