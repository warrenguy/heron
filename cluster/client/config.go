package client

import (
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/idlestrategy"
)

// Config holds cluster client configuration. Normalize before Validate and
// before connect.
//
// Heron transport is invoker-only: during AsyncConnect.Poll() and the session duty
// cycle, the caller must invoke client.DoWork() on the transport *aeron.Client
// between idle/backoff steps. There is no AgentInvoker field on Config.
type Config struct {
	// IngressChannel is required for cluster connect.
	IngressChannel string

	// EgressChannel is required for cluster connect. Supported: ipc, ipcx, and udp.
	EgressChannel string

	// IngressEndpoints is "0=host:port,1=host:port"; empty means unset (Java null).
	IngressEndpoints string

	IngressStreamID  int32
	EgressStreamID   int32
	ResponseStreamID int32

	MessageTimeout time.Duration
	// IngressRetryTimeout bounds IngressOfferUntilPositive and
	// SendKeepAliveUntilPositive. Zero uses DefaultIngressRetryTimeout after
	// Normalize.
	IngressRetryTimeout time.Duration
	NewLeaderTimeout    time.Duration

	AeronDir string
	Aeron    *aeron.Client
	// AeronConfig is passed to aeron.Connect when connect creates the transport
	// client. Transport defaults are applied by aeron.Connect, not
	// Config.Normalize.
	AeronConfig aeron.Config

	// IngressExclusive nil means true (Java isIngressExclusive default).
	// Explicit false disables exclusive ingress publications.
	IngressExclusive *bool

	// ClientName is sent in SessionConnectRequest.clientInfo; empty defaults to
	// cluster-client at connect.
	ClientName string

	CredentialsSupplier     CredentialsSupplier
	EgressHandler           EgressHandler
	ControlledEgressHandler ControlledEgressHandler

	// IdleStrategy paces connect, SpinOnce, and ingress retry. Use one instance
	// across the session lifetime; Connect resets it on exit before the duty cycle.
	IdleStrategy idlestrategy.IdleStrategy

	// DirectAssemblers mirrors Java isDirectAssemblers; passed to EgressAdapter reassembly.
	DirectAssemblers bool
}

// Normalize applies cluster client defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (cfg Config) Normalize() Config {
	if cfg.IngressStreamID == 0 {
		cfg.IngressStreamID = DefaultIngressStreamID
	}
	if cfg.EgressStreamID == 0 {
		cfg.EgressStreamID = DefaultEgressStreamID
	}
	if cfg.ResponseStreamID == 0 {
		cfg.ResponseStreamID = cfg.EgressStreamID
	}
	if cfg.MessageTimeout == 0 {
		cfg.MessageTimeout = DefaultMessageTimeout
	}
	if cfg.IngressRetryTimeout == 0 {
		cfg.IngressRetryTimeout = DefaultIngressRetryTimeout
	}
	if cfg.NewLeaderTimeout == 0 {
		cfg.NewLeaderTimeout = cfg.MessageTimeout
	}
	if cfg.CredentialsSupplier == nil {
		cfg.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if cfg.IdleStrategy == nil {
		cfg.IdleStrategy = NewDefaultIdleStrategy()
	}
	if cfg.IngressExclusive == nil {
		v := true
		cfg.IngressExclusive = &v
	}
	if cfg.AeronDir != "" && cfg.AeronConfig.AeronDir == "" {
		cfg.AeronConfig.AeronDir = cfg.AeronDir
	}
	return cfg
}

// ValidateForConnect checks cluster config after Normalize for async connect.
// Same as Validate except EgressHandler is not required. ControlledEgressHandler
// is not validated by either method. All errors use the cluster client: prefix.
// Call Normalize before ValidateForConnect and connect.
func (cfg Config) ValidateForConnect() error {
	return cfg.validateConfig(false)
}

// Validate checks cluster config after Normalize. Same as ValidateForConnect but
// requires EgressHandler. Call Normalize before Validate and connect.
func (cfg Config) Validate() error {
	return cfg.validateConfig(true)
}

func (cfg Config) validateConfig(requireEgressHandler bool) error {
	if cfg.IngressChannel == "" {
		return errors.New("cluster client: IngressChannel is required")
	}
	if cfg.EgressChannel == "" {
		return errors.New("cluster client: EgressChannel is required")
	}
	err := validateClusterIngressEndpoints(
		cfg.IngressChannel,
		cfg.IngressEndpoints,
	)
	if err != nil {
		return err
	}
	if err := validateClusterEgressChannel(cfg.EgressChannel); err != nil {
		return err
	}
	if len(cfg.ClientName) > MaxClientNameLength {
		return errors.New(
			"cluster client: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if cfg.MessageTimeout <= 0 {
		return errors.New("cluster client: MessageTimeout must be positive")
	}
	if cfg.IngressRetryTimeout <= 0 {
		return errors.New("cluster client: IngressRetryTimeout must be positive")
	}
	if cfg.NewLeaderTimeout < 0 {
		return errors.New("cluster client: NewLeaderTimeout must not be negative")
	}
	if requireEgressHandler && egressHandlerMissing(cfg.EgressHandler) {
		return errors.New("cluster client: EgressHandler is required")
	}
	if cfg.CredentialsSupplier == nil {
		return errors.New("cluster client: CredentialsSupplier is required")
	}
	if cfg.IdleStrategy == nil {
		return errors.New("cluster client: IdleStrategy is required")
	}
	return nil
}

func validateClusterIngressEndpoints(ingressChannel, ingressEndpoints string) error {
	if IngressChannelPassthrough(ingressChannel) && ingressEndpoints != "" {
		return errors.New("cluster client: IngressEndpoints must be empty for ipc/ipcx ingress")
	}
	if ingressEndpoints == "" {
		return nil
	}
	if IngressChannelIsUDP(ingressChannel) &&
		ingressChannelQueryHasEndpointParam(ingressChannel) {
		return errors.New(
			"cluster client: IngressChannel must not use ?endpoint= when IngressEndpoints is configured",
		)
	}
	return validateIngressChannelForMemberEndpoints(ingressChannel)
}

func egressHandlerMissing(handler EgressHandler) bool {
	if handler == nil {
		return true
	}
	handlers, isHandlers := handler.(*EgressHandlers)
	return isHandlers && handlers == nil
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when cfg.Aeron is set; true when connect creates an owned client.
func (cfg Config) OwnsAeronClient() bool {
	return cfg.Aeron == nil
}
