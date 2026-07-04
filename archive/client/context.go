package client

import (
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/idlestrategy"
)

// ArchiveContext holds invoker-only Aeron Archive control client configuration.
// Normalize before ValidateForConnect and before connect.
//
// During AsyncConnect.Poll and the post-connect session duty cycle, the caller
// must invoke DoWork on the transport aeron.Client between idle or backoff steps.
// There is no AgentInvoker field.
type ArchiveContext struct {
	// ControlRequestChannel is the archive control request URI.
	// When LocalControlChannel is set, AsyncConnect prefers local IPC first (Java parity).
	ControlRequestChannel string

	// ControlResponseChannel is the subscription channel for control responses.
	// Empty defaults to DefaultControlResponseChannel after Normalize.
	ControlResponseChannel string

	// LocalControlChannel is the co-located archive IPC control URI.
	// Non-empty causes connect to use this as the request channel (IPC-first).
	// Normalize defaults this to DefaultLocalControlChannel unless WithRemoteControl()
	// was called first.
	LocalControlChannel string

	ControlRequestStreamID  int32
	ControlResponseStreamID int32
	LocalControlStreamID    int32

	ConnectTimeout       time.Duration
	MessageTimeout       time.Duration
	MessageRetryAttempts int

	AeronDir     string
	Aeron        *aeron.Client
	AeronContext aeron.Context

	CredentialsSupplier CredentialsSupplier

	// RecordingSignalHandler receives recording signals on Poll and during blocking
	// control awaits that use this handler (StartRecording, StopRecording, StartReplay,
	// StopReplay, ListRecording). StopRecordingAwaitSignal polls internally but routes STOP proofs
	// to the per-call StopSignalTracker, not this handler. Nil drops signals until
	// AeronArchive.SetRecordingSignalHandler installs a handler.
	RecordingSignalHandler RecordingSignalHandler

	// ClientName is sent in AuthConnectRequest.clientInfo; empty defaults to
	// archive-client at connect.
	ClientName string

	IdleStrategy idlestrategy.IdleStrategy

	remoteControlExplicit bool

	effectiveControlRequestChannel   string
	effectiveControlResponseChannel  string
	effectiveControlRequestStreamID  int32
	effectiveControlResponseStreamID int32
}

// WithRemoteControl opts out of the IPC-first LocalControlChannel default applied by
// Normalize. Remote archive connect uses ControlRequestChannel and
// ControlResponseChannel after Normalize.
func (ctx ArchiveContext) WithRemoteControl() ArchiveContext {
	ctx.remoteControlExplicit = true
	ctx.LocalControlChannel = ""
	return ctx
}

// Normalize applies archive client defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (ctx ArchiveContext) Normalize() ArchiveContext {
	if !ctx.remoteControlExplicit && ctx.LocalControlChannel == "" {
		ctx.LocalControlChannel = DefaultLocalControlChannel
	}
	if ctx.ControlRequestChannel == "" {
		ctx.ControlRequestChannel = DefaultControlRequestChannel
	}
	if ctx.ControlResponseChannel == "" {
		ctx.ControlResponseChannel = DefaultControlResponseChannel
	}
	if ctx.LocalControlStreamID == 0 {
		ctx.LocalControlStreamID = DefaultLocalControlStreamID
	}
	if ctx.ControlRequestStreamID == 0 {
		ctx.ControlRequestStreamID = DefaultControlRequestStreamID
	}
	if ctx.ControlResponseStreamID == 0 {
		ctx.ControlResponseStreamID = DefaultControlResponseStreamID
	}
	if ctx.ConnectTimeout == 0 {
		ctx.ConnectTimeout = DefaultConnectTimeout
	}
	if ctx.MessageTimeout == 0 {
		ctx.MessageTimeout = DefaultMessageTimeout
	}
	if ctx.MessageRetryAttempts == 0 {
		ctx.MessageRetryAttempts = DefaultMessageRetryAttempts
	}
	if ctx.CredentialsSupplier == nil {
		ctx.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if ctx.IdleStrategy == nil {
		ctx.IdleStrategy = NewDefaultIdleStrategy()
	}
	if ctx.ClientName == "" {
		ctx.ClientName = defaultArchiveClientName
	}
	if ctx.AeronDir != "" && ctx.AeronContext.AeronDir == "" {
		ctx.AeronContext.AeronDir = ctx.AeronDir
	}
	return ctx
}

// ValidateForConnect checks archive context for async connect. Applies Normalize and
// effective channel selection on a stack copy without mutating the receiver. All
// errors use the archive client: prefix.
func (ctx ArchiveContext) ValidateForConnect() error {
	ctx = applyEffectiveChannels(ctx.Normalize())

	requestChannel := ctx.effectiveControlRequestChannel
	responseChannel := ctx.effectiveControlResponseChannel
	if err := validateArchiveIngressChannel(requestChannel, "request"); err != nil {
		return err
	}
	if requestChannel != responseChannel {
		if err := validateArchiveIngressChannel(responseChannel, "response"); err != nil {
			return err
		}
	}

	if ctx.ConnectTimeout <= 0 {
		return errors.New("archive client: ConnectTimeout must be positive")
	}
	if ctx.MessageTimeout <= 0 {
		return errors.New("archive client: MessageTimeout must be positive")
	}
	if ctx.MessageRetryAttempts <= 0 {
		return errors.New("archive client: MessageRetryAttempts must be positive")
	}
	if len(ctx.ClientName) > MaxClientNameLength {
		return errors.New(
			"archive client: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if ctx.Aeron == nil && archiveAeronDir(ctx) == "" {
		return errors.New("archive client: AeronDir is required when Aeron is not set")
	}
	return nil
}

// EffectiveControlRequestChannel returns the request URI after IPC-first selection.
func (ctx ArchiveContext) EffectiveControlRequestChannel() string {
	return ctx.effectiveControlRequestChannel
}

// EffectiveControlResponseChannel returns the response subscription URI after
// IPC-first selection.
func (ctx ArchiveContext) EffectiveControlResponseChannel() string {
	return ctx.effectiveControlResponseChannel
}

// EffectiveControlRequestStreamID returns the request stream id after IPC-first
// selection.
func (ctx ArchiveContext) EffectiveControlRequestStreamID() int32 {
	return ctx.effectiveControlRequestStreamID
}

// EffectiveControlResponseStreamID returns the response stream id after IPC-first
// selection.
func (ctx ArchiveContext) EffectiveControlResponseStreamID() int32 {
	return ctx.effectiveControlResponseStreamID
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when ctx.Aeron is set; true when connect creates an owned client.
func (ctx ArchiveContext) OwnsAeronClient() bool {
	return ctx.Aeron == nil
}

func applyEffectiveChannels(ctx ArchiveContext) ArchiveContext {
	if ctx.LocalControlChannel != "" {
		requestStreamID := ctx.LocalControlStreamID
		if requestStreamID == 0 {
			requestStreamID = ctx.ControlRequestStreamID
		}
		ctx.effectiveControlRequestChannel = ctx.LocalControlChannel
		ctx.effectiveControlResponseChannel = ctx.LocalControlChannel
		ctx.effectiveControlRequestStreamID = requestStreamID
		ctx.effectiveControlResponseStreamID = requestStreamID
		return ctx
	}

	responseChannel := ctx.ControlResponseChannel
	if responseChannel == "" {
		responseChannel = ctx.ControlRequestChannel
	}
	requestStreamID := ctx.ControlRequestStreamID
	responseStreamID := ctx.ControlResponseStreamID
	if responseStreamID == 0 {
		responseStreamID = requestStreamID
	}
	ctx.effectiveControlRequestChannel = ctx.ControlRequestChannel
	ctx.effectiveControlResponseChannel = responseChannel
	ctx.effectiveControlRequestStreamID = requestStreamID
	ctx.effectiveControlResponseStreamID = responseStreamID
	return ctx
}

func archiveAeronDir(ctx ArchiveContext) string {
	if ctx.AeronDir != "" {
		return ctx.AeronDir
	}
	return ctx.AeronContext.AeronDir
}

func concludeArchiveContext(ctx ArchiveContext) (ArchiveContext, *aeron.Client, bool, error) {
	ctx = applyEffectiveChannels(ctx)
	responseChannel, err := applyUDPRejoinFalse(ctx.effectiveControlResponseChannel)
	if err != nil {
		return ArchiveContext{}, nil, false, err
	}
	ctx.effectiveControlResponseChannel = responseChannel

	if ctx.Aeron != nil {
		return ctx, ctx.Aeron, false, nil
	}
	aeronCtx := ctx.AeronContext
	if dir := archiveAeronDir(ctx); dir != "" {
		aeronCtx.AeronDir = dir
	}
	if aeronCtx.ErrorHandler == nil {
		aeronCtx.ErrorHandler = defaultArchiveConnectErrorHandler
	}
	var client *aeron.Client
	if factory, ok := ownedConnectClientFactoryForContext(ctx); ok {
		client, err = factory(ctx)
	} else {
		client, err = aeron.Connect(aeronCtx)
	}
	if err != nil {
		return ArchiveContext{}, nil, false, err
	}
	ctx.Aeron = client
	return ctx, client, true, nil
}

func defaultArchiveConnectErrorHandler(err error) {
	panic(err)
}
