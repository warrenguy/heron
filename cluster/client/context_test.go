package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const (
	testIngressEndpointsMap      = "0=localhost:20110"
	errPassthroughIngressWithMap = "cluster client: IngressEndpoints must be empty for " +
		"ipc/ipcx ingress"
	errUDPIngressEndpointWithMap = "cluster client: IngressChannel must not use ?endpoint= " +
		"when IngressEndpoints is configured"
	errEgressHandlerRequired = "cluster client: EgressHandler is required"
)

func withIngressChannelAndEndpoints(channel, endpoints string) func(*client.ClusterContext) {
	return func(clusterCtx *client.ClusterContext) {
		clusterCtx.IngressChannel = channel
		clusterCtx.IngressEndpoints = endpoints
	}
}

func withNilEgressHandler(clusterCtx *client.ClusterContext) {
	clusterCtx.EgressHandler = nil
}

func withTypedNilEgressHandlers(clusterCtx *client.ClusterContext) {
	clusterCtx.EgressHandler = (*client.EgressHandlers)(nil)
}

func withNilCredentialsSupplier(clusterCtx *client.ClusterContext) {
	clusterCtx.CredentialsSupplier = nil
}

func withNilIdleStrategy(clusterCtx *client.ClusterContext) {
	clusterCtx.IdleStrategy = nil
}

func validClusterContext() client.ClusterContext {
	return client.ClusterContext{
		IngressChannel:      "aeron:udp?endpoint=localhost:20110",
		EgressChannel:       "aeron:ipc",
		MessageTimeout:      client.DefaultMessageTimeout,
		IngressRetryTimeout: client.DefaultIngressRetryTimeout,
		EgressHandler:       &client.EgressHandlers{},
		CredentialsSupplier: client.NullCredentialsSupplier{},
		IdleStrategy:        client.NewDefaultIdleStrategy(),
	}
}

func normalizedClusterContext(mutate func(*client.ClusterContext)) client.ClusterContext {
	clusterCtx := validClusterContext()
	if mutate != nil {
		mutate(&clusterCtx)
	}
	return clusterCtx.Normalize()
}

func requireNoValidationErrors(t *testing.T, clusterCtx client.ClusterContext) {
	t.Helper()
	require.NoError(t, clusterCtx.Validate())
	require.NoError(t, clusterCtx.ValidateForConnect())
}

func requireValidationErrors(t *testing.T, clusterCtx client.ClusterContext, wantErr string) {
	t.Helper()
	require.EqualError(t, clusterCtx.Validate(), wantErr)
	require.EqualError(t, clusterCtx.ValidateForConnect(), wantErr)
}

func requireValidateErrorButConnectOK(
	t *testing.T,
	clusterCtx client.ClusterContext,
	wantErr string,
) {
	t.Helper()
	require.EqualError(t, clusterCtx.Validate(), wantErr)
	require.NoError(t, clusterCtx.ValidateForConnect())
}

func TestNormalizeClusterContext_defaults(t *testing.T) {
	clusterCtx := normalizedClusterContext(nil)

	require.Equal(t, client.DefaultIngressStreamID, clusterCtx.IngressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, clusterCtx.EgressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, clusterCtx.ResponseStreamID)
	require.Equal(t, client.DefaultMessageTimeout, clusterCtx.MessageTimeout)
	require.Equal(t, client.DefaultIngressRetryTimeout, clusterCtx.IngressRetryTimeout)
	require.Equal(t, client.DefaultMessageTimeout, clusterCtx.NewLeaderTimeout)
	require.IsType(t, client.NullCredentialsSupplier{}, clusterCtx.CredentialsSupplier)
	require.NotNil(t, clusterCtx.IdleStrategy)
	require.NotNil(t, clusterCtx.IngressExclusive)
	require.True(t, *clusterCtx.IngressExclusive)
}

func TestNormalizeClusterContext_newLeaderTimeoutZeroUsesMessageTimeout(t *testing.T) {
	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.MessageTimeout = 7 * time.Second
	})

	require.Equal(t, 7*time.Second, clusterCtx.NewLeaderTimeout)
}

func TestNormalizeClusterContext_responseStreamIDDefaultsToEgress(t *testing.T) {
	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.EgressStreamID = 200
		clusterCtx.ResponseStreamID = 0
	})

	require.Equal(t, int32(200), clusterCtx.ResponseStreamID)
}

func TestNormalizeClusterContext_preservesExplicitValues(t *testing.T) {
	exclusive := false
	customTimeout := 11 * time.Second
	customNewLeader := 22 * time.Second

	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.IngressStreamID = 111
		clusterCtx.EgressStreamID = 222
		clusterCtx.ResponseStreamID = 333
		clusterCtx.MessageTimeout = customTimeout
		clusterCtx.NewLeaderTimeout = customNewLeader
		clusterCtx.IngressExclusive = &exclusive
	})

	require.Equal(t, int32(111), clusterCtx.IngressStreamID)
	require.Equal(t, int32(222), clusterCtx.EgressStreamID)
	require.Equal(t, int32(333), clusterCtx.ResponseStreamID)
	require.Equal(t, customTimeout, clusterCtx.MessageTimeout)
	require.Equal(t, customNewLeader, clusterCtx.NewLeaderTimeout)
	require.NotNil(t, clusterCtx.IngressExclusive)
	require.False(t, *clusterCtx.IngressExclusive)
}

func TestNormalizeClusterContext_mergesAeronDirOnly(t *testing.T) {
	customEpoch := func() int64 { return 42 }
	customNano := func() int64 { return 99 }

	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.AeronDir = "/tmp/heron-aeron"
		clusterCtx.AeronContext = aeron.Context{
			EpochClock: customEpoch,
			NanoClock:  customNano,
		}
	})

	require.Equal(t, "/tmp/heron-aeron", clusterCtx.AeronContext.AeronDir)
	require.NotNil(t, clusterCtx.AeronContext.EpochClock)
	require.Equal(t, int64(42), clusterCtx.AeronContext.EpochClock())
	require.NotNil(t, clusterCtx.AeronContext.NanoClock)
	require.Equal(t, int64(99), clusterCtx.AeronContext.NanoClock())
}

func TestNormalizeClusterContext_preservesNestedAeronDir(t *testing.T) {
	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.AeronDir = "/tmp/top-level"
		clusterCtx.AeronContext = aeron.Context{
			AeronDir: "/tmp/nested",
		}
	})

	require.Equal(t, "/tmp/nested", clusterCtx.AeronContext.AeronDir)
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
		validateOnly   bool
		wantErr        string
	}{
		{
			name: "missing ingress channel",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.IngressChannel = ""
			},
			wantErr: "cluster client: IngressChannel is required",
		},
		{
			name: "missing egress channel",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = ""
			},
			wantErr: "cluster client: EgressChannel is required",
		},
		{
			name: "unknown egress media",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = "aeron:unknown"
			},
			wantErr: "cluster client: unknown egress response channel not supported; use ipc, ipcx, or udp",
		},
		{
			name: "invalid egress channel",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = "not-a-channel"
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
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.ClientName = string(make([]byte, client.MaxClientNameLength+1))
			},
			wantErr: "cluster client: ClientName length must be <= 100",
		},
		{
			name: "non positive message timeout",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.MessageTimeout = -time.Second
			},
			wantErr: "cluster client: MessageTimeout must be positive",
		},
		{
			name: "non positive ingress retry timeout",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.IngressRetryTimeout = -time.Second
			},
			wantErr: "cluster client: IngressRetryTimeout must be positive",
		},
		{
			name: "negative new leader timeout",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.NewLeaderTimeout = -time.Second
			},
			wantErr: "cluster client: NewLeaderTimeout must not be negative",
		},
		{
			name:         "nil egress handler",
			mutate:       withNilEgressHandler,
			validateOnly: true,
			wantErr:      errEgressHandlerRequired,
		},
		{
			name:         "typed nil egress handlers",
			mutate:       withTypedNilEgressHandlers,
			validateOnly: true,
			wantErr:      errEgressHandlerRequired,
		},
		{
			name:           "nil credentials supplier",
			mutate:         withNilCredentialsSupplier,
			afterNormalize: withNilCredentialsSupplier,
			wantErr:        "cluster client: CredentialsSupplier is required",
		},
		{
			name:           "nil idle strategy",
			mutate:         withNilIdleStrategy,
			afterNormalize: withNilIdleStrategy,
			wantErr:        "cluster client: IdleStrategy is required",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clusterCtx := normalizedClusterContext(testCase.mutate)
			if testCase.afterNormalize != nil {
				testCase.afterNormalize(&clusterCtx)
			}

			if testCase.validateOnly {
				require.EqualError(t, clusterCtx.Validate(), testCase.wantErr)
			} else {
				requireValidationErrors(t, clusterCtx, testCase.wantErr)
			}
		})
	}
}

func TestValidateClusterContext_timeoutsWithoutNormalize(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*client.ClusterContext)
		wantErr     string
		wantNoError bool
	}{
		{
			name: "zero message timeout",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.MessageTimeout = 0
			},
			wantErr: "cluster client: MessageTimeout must be positive",
		},
		{
			name: "zero new leader timeout",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.MessageTimeout = 7 * time.Second
				clusterCtx.NewLeaderTimeout = 0
			},
			wantNoError: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clusterCtx := validClusterContext()
			testCase.mutate(&clusterCtx)

			err := clusterCtx.Validate()
			if testCase.wantNoError {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.wantErr)
			}
		})
	}
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
			clusterCtx := normalizedClusterContext(
				withIngressChannelAndEndpoints(testCase.channel, testIngressEndpointsMap),
			)
			requireValidationErrors(
				t, clusterCtx, "cluster client: invalid ingress channel: "+testCase.channel,
			)
		})
	}
}

func TestValidateForConnect_egressHandlerNotRequired(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.ClusterContext)
	}{
		{name: "nil", mutate: withNilEgressHandler},
		{name: "typed nil", mutate: withTypedNilEgressHandlers},
		{
			name: "nil with udp egress",
			mutate: func(clusterCtx *client.ClusterContext) {
				withNilEgressHandler(clusterCtx)
				clusterCtx.EgressChannel = "aeron:udp?endpoint=localhost:20111"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clusterCtx := normalizedClusterContext(testCase.mutate)

			requireValidateErrorButConnectOK(t, clusterCtx, errEgressHandlerRequired)
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
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.ClientName = string(make([]byte, client.MaxClientNameLength))
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
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = "aeron:ipcx?alias=egress"
			},
		},
		{
			name: "udp egress channel",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = "aeron:udp?endpoint=localhost:0|term-length=64k"
			},
		},
		{
			name: "spy udp egress channel",
			mutate: func(clusterCtx *client.ClusterContext) {
				clusterCtx.EgressChannel = "aeron-spy:aeron:udp?endpoint=localhost:0"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			requireNoValidationErrors(t, normalizedClusterContext(testCase.mutate))
		})
	}
}

func TestOwnsAeronClient(t *testing.T) {
	external := &aeron.Client{}
	clusterCtx := normalizedClusterContext(func(clusterCtx *client.ClusterContext) {
		clusterCtx.Aeron = external
	})

	require.False(t, clusterCtx.OwnsAeronClient())

	owned := normalizedClusterContext(nil)
	require.True(t, owned.OwnsAeronClient())
}

func TestProtocolSemanticVersion(t *testing.T) {
	version := client.ProtocolSemanticVersion()
	require.Equal(t, int32(768), version)
	require.Equal(t, cnc.ComposeSemanticVersion(0, 3, 0), version)

	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
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

	decoded, ok := codecs.TryDecodeByTemplate(buf, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, clusterwire.SessionConnectRequestSbeTemplateID, decoded.TemplateID)

	gotFields, ok := decoded.Fields.(codecs.SessionConnectRequestFields)
	require.True(t, ok)
	require.Equal(t, int32(768), gotFields.Version)
}

func TestSessionHeaderLength(t *testing.T) {
	const golden = 32

	require.Equal(t, golden, client.SessionHeaderLength())
	require.Equal(t, golden, codecs.SessionMessageHeaderEncodedLen())
}
