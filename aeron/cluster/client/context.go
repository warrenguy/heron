package client

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// ClusterContext holds cluster client configuration. Normalize before Validate and
// before connect.
//
// Heron transport is invoker-only: during AsyncConnect.Poll() and the session duty
// cycle, the caller must invoke client.DoWork() on the transport *aeron.Client
// between idle/backoff steps. There is no AgentInvoker context field.
type ClusterContext struct {
	// IngressChannel is required for cluster connect.
	IngressChannel string

	// EgressChannel is required for cluster connect.
	// UDP rejoin=false injection is applied at connect time; no channel URI parser
	// exists in transport today.
	EgressChannel string

	// IngressEndpoints is "0=host:port,1=host:port"; empty means unset (Java null).
	IngressEndpoints string

	IngressStreamID  int32
	EgressStreamID   int32
	ResponseStreamID int32

	MessageTimeout   time.Duration
	NewLeaderTimeout time.Duration

	AeronDir string
	Aeron    *aeron.Client
	// AeronContext is passed to aeron.Connect when connect creates the transport
	// client. Transport defaults are applied by aeron.Connect, not
	// ClusterContext.Normalize.
	AeronContext aeron.Context

	// IngressExclusive nil means true (Java isIngressExclusive default).
	// Explicit false disables exclusive ingress publications.
	IngressExclusive *bool

	// ClientName is sent in SessionConnectRequest.clientInfo; empty defaults to
	// cluster-client at connect.
	ClientName string

	CredentialsSupplier     CredentialsSupplier
	EgressHandler           EgressHandler
	ControlledEgressHandler ControlledEgressHandler

	IdleStrategy idlestrategy.IdleStrategy

	// DirectAssemblers mirrors Java isDirectAssemblers; consumed by egress FragmentAssembler.
	DirectAssemblers bool
}

// Normalize applies cluster client defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (ctx ClusterContext) Normalize() ClusterContext {
	if ctx.IngressStreamID == 0 {
		ctx.IngressStreamID = DefaultIngressStreamID
	}
	if ctx.EgressStreamID == 0 {
		ctx.EgressStreamID = DefaultEgressStreamID
	}
	if ctx.ResponseStreamID == 0 {
		ctx.ResponseStreamID = ctx.EgressStreamID
	}
	if ctx.MessageTimeout == 0 {
		ctx.MessageTimeout = DefaultMessageTimeout
	}
	if ctx.NewLeaderTimeout == 0 {
		ctx.NewLeaderTimeout = ctx.MessageTimeout
	}
	if ctx.CredentialsSupplier == nil {
		ctx.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if ctx.IdleStrategy == nil {
		ctx.IdleStrategy = NewDefaultIdleStrategy()
	}
	if ctx.IngressExclusive == nil {
		v := true
		ctx.IngressExclusive = &v
	}
	if ctx.AeronDir != "" && ctx.AeronContext.AeronDir == "" {
		ctx.AeronContext.AeronDir = ctx.AeronDir
	}
	return ctx
}

// Validate checks cluster context after Normalize. All errors use the cluster
// client: prefix. Call Normalize before Validate and connect.
func (ctx ClusterContext) Validate() error {
	if ctx.IngressChannel == "" {
		return errors.New("cluster client: IngressChannel is required")
	}
	if ctx.EgressChannel == "" {
		return errors.New("cluster client: EgressChannel is required")
	}
	if ingressChannelIsIPC(ctx.IngressChannel) && ctx.IngressEndpoints != "" {
		return errors.New("cluster client: IngressEndpoints must be empty for IPC ingress")
	}
	if len(ctx.ClientName) > MaxClientNameLength {
		return errors.New(
			"cluster client: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if ctx.MessageTimeout <= 0 {
		return errors.New("cluster client: MessageTimeout must be positive")
	}
	if ctx.NewLeaderTimeout < 0 {
		return errors.New("cluster client: NewLeaderTimeout must not be negative")
	}
	if ctx.EgressHandler == nil {
		return errors.New("cluster client: EgressHandler is required")
	}
	handlers, typed := ctx.EgressHandler.(*EgressHandlers)
	if typed && handlers == nil {
		return errors.New("cluster client: EgressHandler is required")
	}
	if ctx.CredentialsSupplier == nil {
		return errors.New("cluster client: CredentialsSupplier is required")
	}
	if ctx.IdleStrategy == nil {
		return errors.New("cluster client: IdleStrategy is required")
	}
	return nil
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when ctx.Aeron is set; true when connect creates an owned client.
func (ctx ClusterContext) OwnsAeronClient() bool {
	return ctx.Aeron == nil
}

func ingressChannelIsIPC(channel string) bool {
	channel = strings.TrimPrefix(channel, "aeron-spy:")
	if !strings.HasPrefix(channel, "aeron:") {
		return false
	}
	media, _, _ := strings.Cut(channel[len("aeron:"):], "?")
	return media == "ipc"
}
