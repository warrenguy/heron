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

func withIngressChannelAndEndpoints(channel, endpoints string) func(*client.Config) {
	return func(cfg *client.Config) {
		cfg.IngressChannel = channel
		cfg.IngressEndpoints = endpoints
	}
}

func withNilEgressHandler(cfg *client.Config) {
	cfg.EgressHandler = nil
}

func withTypedNilEgressHandlers(cfg *client.Config) {
	cfg.EgressHandler = (*client.EgressHandlers)(nil)
}

func withNilCredentialsSupplier(cfg *client.Config) {
	cfg.CredentialsSupplier = nil
}

func withNilIdleStrategy(cfg *client.Config) {
	cfg.IdleStrategy = nil
}

func validConfig() client.Config {
	return client.Config{
		IngressChannel:      "aeron:udp?endpoint=localhost:20110",
		EgressChannel:       "aeron:ipc",
		MessageTimeout:      client.DefaultMessageTimeout,
		IngressRetryTimeout: client.DefaultIngressRetryTimeout,
		EgressHandler:       &client.EgressHandlers{},
		CredentialsSupplier: client.NullCredentialsSupplier{},
		IdleStrategy:        client.NewDefaultIdleStrategy(),
	}
}

func normalizedConfig(mutate func(*client.Config)) client.Config {
	cfg := validConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg.Normalize()
}

func requireNoValidationErrors(t *testing.T, cfg client.Config) {
	t.Helper()
	require.NoError(t, cfg.Validate())
	require.NoError(t, cfg.ValidateForConnect())
}

func requireValidationErrors(t *testing.T, cfg client.Config, wantErr string) {
	t.Helper()
	require.EqualError(t, cfg.Validate(), wantErr)
	require.EqualError(t, cfg.ValidateForConnect(), wantErr)
}

func requireValidateErrorButConnectOK(
	t *testing.T,
	cfg client.Config,
	wantErr string,
) {
	t.Helper()
	require.EqualError(t, cfg.Validate(), wantErr)
	require.NoError(t, cfg.ValidateForConnect())
}

func TestNormalizeConfig_defaults(t *testing.T) {
	cfg := normalizedConfig(nil)

	require.Equal(t, client.DefaultIngressStreamID, cfg.IngressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, cfg.EgressStreamID)
	require.Equal(t, client.DefaultEgressStreamID, cfg.ResponseStreamID)
	require.Equal(t, client.DefaultMessageTimeout, cfg.MessageTimeout)
	require.Equal(t, client.DefaultIngressRetryTimeout, cfg.IngressRetryTimeout)
	require.Equal(t, client.DefaultMessageTimeout, cfg.NewLeaderTimeout)
	require.IsType(t, client.NullCredentialsSupplier{}, cfg.CredentialsSupplier)
	require.NotNil(t, cfg.IdleStrategy)
	require.NotNil(t, cfg.IngressExclusive)
	require.True(t, *cfg.IngressExclusive)
}

func TestNormalizeConfig_newLeaderTimeoutZeroUsesMessageTimeout(t *testing.T) {
	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.MessageTimeout = 7 * time.Second
	})

	require.Equal(t, 7*time.Second, cfg.NewLeaderTimeout)
}

func TestNormalizeConfig_responseStreamIDDefaultsToEgress(t *testing.T) {
	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.EgressStreamID = 200
		cfg.ResponseStreamID = 0
	})

	require.Equal(t, int32(200), cfg.ResponseStreamID)
}

func TestNormalizeConfig_preservesExplicitValues(t *testing.T) {
	exclusive := false
	customTimeout := 11 * time.Second
	customNewLeader := 22 * time.Second

	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.IngressStreamID = 111
		cfg.EgressStreamID = 222
		cfg.ResponseStreamID = 333
		cfg.MessageTimeout = customTimeout
		cfg.NewLeaderTimeout = customNewLeader
		cfg.IngressExclusive = &exclusive
	})

	require.Equal(t, int32(111), cfg.IngressStreamID)
	require.Equal(t, int32(222), cfg.EgressStreamID)
	require.Equal(t, int32(333), cfg.ResponseStreamID)
	require.Equal(t, customTimeout, cfg.MessageTimeout)
	require.Equal(t, customNewLeader, cfg.NewLeaderTimeout)
	require.NotNil(t, cfg.IngressExclusive)
	require.False(t, *cfg.IngressExclusive)
}

func TestNormalizeConfig_mergesAeronDirOnly(t *testing.T) {
	customEpoch := func() int64 { return 42 }
	customNano := func() int64 { return 99 }

	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.AeronDir = "/tmp/heron-aeron"
		cfg.AeronConfig = aeron.Config{
			EpochClock: customEpoch,
			NanoClock:  customNano,
		}
	})

	require.Equal(t, "/tmp/heron-aeron", cfg.AeronConfig.AeronDir)
	require.NotNil(t, cfg.AeronConfig.EpochClock)
	require.Equal(t, int64(42), cfg.AeronConfig.EpochClock())
	require.NotNil(t, cfg.AeronConfig.NanoClock)
	require.Equal(t, int64(99), cfg.AeronConfig.NanoClock())
}

func TestNormalizeConfig_preservesNestedAeronDir(t *testing.T) {
	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.AeronDir = "/tmp/top-level"
		cfg.AeronConfig = aeron.Config{
			AeronDir: "/tmp/nested",
		}
	})

	require.Equal(t, "/tmp/nested", cfg.AeronConfig.AeronDir)
}

func TestNormalizeConfig_ingressExclusiveMutationDoesNotAffectFreshDefault(
	t *testing.T,
) {
	normalized := normalizedConfig(nil)
	require.NotNil(t, normalized.IngressExclusive)

	*normalized.IngressExclusive = false

	fresh := client.Config{}.Normalize()
	require.NotNil(t, fresh.IngressExclusive)
	require.True(t, *fresh.IngressExclusive)
}

func TestNormalizeConfig_idempotent(t *testing.T) {
	initial := validConfig()
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateConfig_errors(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(*client.Config)
		afterNormalize func(*client.Config)
		validateOnly   bool
		wantErr        string
	}{
		{
			name: "missing ingress channel",
			mutate: func(cfg *client.Config) {
				cfg.IngressChannel = ""
			},
			wantErr: "cluster client: IngressChannel is required",
		},
		{
			name: "missing egress channel",
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = ""
			},
			wantErr: "cluster client: EgressChannel is required",
		},
		{
			name: "unknown egress media",
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = "aeron:unknown"
			},
			wantErr: "cluster client: unknown egress response channel not supported; use ipc, ipcx, or udp",
		},
		{
			name: "invalid egress channel",
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = "not-a-channel"
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
			mutate: func(cfg *client.Config) {
				cfg.ClientName = string(make([]byte, client.MaxClientNameLength+1))
			},
			wantErr: "cluster client: ClientName length must be <= 100",
		},
		{
			name: "non positive message timeout",
			mutate: func(cfg *client.Config) {
				cfg.MessageTimeout = -time.Second
			},
			wantErr: "cluster client: MessageTimeout must be positive",
		},
		{
			name: "non positive ingress retry timeout",
			mutate: func(cfg *client.Config) {
				cfg.IngressRetryTimeout = -time.Second
			},
			wantErr: "cluster client: IngressRetryTimeout must be positive",
		},
		{
			name: "negative new leader timeout",
			mutate: func(cfg *client.Config) {
				cfg.NewLeaderTimeout = -time.Second
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
			cfg := normalizedConfig(testCase.mutate)
			if testCase.afterNormalize != nil {
				testCase.afterNormalize(&cfg)
			}

			if testCase.validateOnly {
				require.EqualError(t, cfg.Validate(), testCase.wantErr)
			} else {
				requireValidationErrors(t, cfg, testCase.wantErr)
			}
		})
	}
}

func TestValidateConfig_timeoutsWithoutNormalize(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*client.Config)
		wantErr     string
		wantNoError bool
	}{
		{
			name: "zero message timeout",
			mutate: func(cfg *client.Config) {
				cfg.MessageTimeout = 0
			},
			wantErr: "cluster client: MessageTimeout must be positive",
		},
		{
			name: "zero new leader timeout",
			mutate: func(cfg *client.Config) {
				cfg.MessageTimeout = 7 * time.Second
				cfg.NewLeaderTimeout = 0
			},
			wantNoError: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := validConfig()
			testCase.mutate(&cfg)

			err := cfg.Validate()
			if testCase.wantNoError {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.wantErr)
			}
		})
	}
}

func TestValidateConfig_unparseableIngressWithEndpoints(t *testing.T) {
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
			cfg := normalizedConfig(
				withIngressChannelAndEndpoints(testCase.channel, testIngressEndpointsMap),
			)
			requireValidationErrors(
				t, cfg, "cluster client: invalid ingress channel: "+testCase.channel,
			)
		})
	}
}

func TestValidateForConnect_egressHandlerNotRequired(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.Config)
	}{
		{name: "nil", mutate: withNilEgressHandler},
		{name: "typed nil", mutate: withTypedNilEgressHandlers},
		{
			name: "nil with udp egress",
			mutate: func(cfg *client.Config) {
				withNilEgressHandler(cfg)
				cfg.EgressChannel = "aeron:udp?endpoint=localhost:20111"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := normalizedConfig(testCase.mutate)

			requireValidateErrorButConnectOK(t, cfg, errEgressHandlerRequired)
		})
	}
}

func TestValidateConfig_validCases(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.Config)
	}{
		{
			name: "client name at max length",
			mutate: func(cfg *client.Config) {
				cfg.ClientName = string(make([]byte, client.MaxClientNameLength))
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
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = "aeron:ipcx?alias=egress"
			},
		},
		{
			name: "udp egress channel",
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = "aeron:udp?endpoint=localhost:0|term-length=64k"
			},
		},
		{
			name: "spy udp egress channel",
			mutate: func(cfg *client.Config) {
				cfg.EgressChannel = "aeron-spy:aeron:udp?endpoint=localhost:0"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			requireNoValidationErrors(t, normalizedConfig(testCase.mutate))
		})
	}
}

func TestOwnsAeronClient(t *testing.T) {
	external := &aeron.Client{}
	cfg := normalizedConfig(func(cfg *client.Config) {
		cfg.Aeron = external
	})

	require.False(t, cfg.OwnsAeronClient())

	owned := normalizedConfig(nil)
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
