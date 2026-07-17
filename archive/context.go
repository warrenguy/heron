package archive

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/idlestrategy"
)

// Context holds invoker-only Aeron Archive control client configuration.
// Normalize before ValidateForConnect and before connect.
//
// During AsyncConnect.Poll and the post-connect session duty cycle, the caller
// must invoke DoWork on the transport aeron.Client between idle or backoff steps.
// There is no AgentInvoker field.
type Context struct {
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

	ConnectTimeout time.Duration
	// ControlResponseTimeout bounds control RPC response awaits. Zero uses
	// DefaultControlResponseTimeout after Normalize. Java parity: messageTimeoutNs.
	ControlResponseTimeout time.Duration
	// ControlOfferRetryTimeout bounds control offer retries, including
	// [Client.ControlOfferUntilPositive]. Zero uses
	// DefaultControlOfferRetryTimeout after Normalize.
	ControlOfferRetryTimeout time.Duration
	MessageRetryAttempts     int

	AeronDir     string
	Aeron        *aeron.Client
	AeronContext aeron.Context

	CredentialsSupplier CredentialsSupplier

	// RecordingSignalHandler receives recording signals on Poll and during blocking
	// control awaits that use this handler (StartRecording, StopRecording, StartReplay,
	// StopReplay, ListRecording). StopRecordingAwaitSignal polls internally and routes
	// STOP proofs to the per-call StopSignalTracker, not this handler. Nil drops signals
	// until Client.SetRecordingSignalHandler installs a handler.
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
func (archiveCtx Context) WithRemoteControl() Context {
	archiveCtx.remoteControlExplicit = true
	archiveCtx.LocalControlChannel = ""
	return archiveCtx
}

// Normalize applies archive defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (archiveCtx Context) Normalize() Context {
	if !archiveCtx.remoteControlExplicit && archiveCtx.LocalControlChannel == "" {
		archiveCtx.LocalControlChannel = DefaultLocalControlChannel
	}
	if archiveCtx.ControlRequestChannel == "" {
		archiveCtx.ControlRequestChannel = DefaultControlRequestChannel
	}
	if archiveCtx.ControlResponseChannel == "" {
		archiveCtx.ControlResponseChannel = DefaultControlResponseChannel
	}
	if archiveCtx.LocalControlStreamID == 0 {
		archiveCtx.LocalControlStreamID = DefaultLocalControlStreamID
	}
	if archiveCtx.ControlRequestStreamID == 0 {
		archiveCtx.ControlRequestStreamID = DefaultControlRequestStreamID
	}
	if archiveCtx.ControlResponseStreamID == 0 {
		archiveCtx.ControlResponseStreamID = DefaultControlResponseStreamID
	}
	if archiveCtx.ConnectTimeout == 0 {
		archiveCtx.ConnectTimeout = DefaultConnectTimeout
	}
	if archiveCtx.ControlResponseTimeout == 0 {
		archiveCtx.ControlResponseTimeout = DefaultControlResponseTimeout
	}
	if archiveCtx.ControlOfferRetryTimeout == 0 {
		archiveCtx.ControlOfferRetryTimeout = DefaultControlOfferRetryTimeout
	}
	if archiveCtx.MessageRetryAttempts == 0 {
		archiveCtx.MessageRetryAttempts = DefaultMessageRetryAttempts
	}
	if archiveCtx.CredentialsSupplier == nil {
		archiveCtx.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if archiveCtx.IdleStrategy == nil {
		archiveCtx.IdleStrategy = NewDefaultIdleStrategy()
	}
	if archiveCtx.ClientName == "" {
		archiveCtx.ClientName = defaultClientName
	}
	if archiveCtx.AeronDir != "" && archiveCtx.AeronContext.AeronDir == "" {
		archiveCtx.AeronContext.AeronDir = archiveCtx.AeronDir
	}
	return archiveCtx
}

// ValidateForConnect checks archive context for async connect. Applies Normalize and
// effective channel selection on a stack copy without mutating the receiver. All
// errors use the archive: prefix.
func (archiveCtx Context) ValidateForConnect() error {
	archiveCtx = applyEffectiveChannels(archiveCtx.Normalize())

	requestChannel := archiveCtx.effectiveControlRequestChannel
	responseChannel := archiveCtx.effectiveControlResponseChannel
	if err := validateArchiveIngressChannel(requestChannel, "request"); err != nil {
		return err
	}
	if requestChannel != responseChannel {
		if err := validateArchiveIngressChannel(responseChannel, "response"); err != nil {
			return err
		}
	}

	if archiveCtx.ConnectTimeout <= 0 {
		return errors.New("archive: ConnectTimeout must be positive")
	}
	if archiveCtx.ControlResponseTimeout <= 0 {
		return errors.New("archive: ControlResponseTimeout must be positive")
	}
	if archiveCtx.ControlOfferRetryTimeout <= 0 {
		return errors.New("archive: ControlOfferRetryTimeout must be positive")
	}
	if archiveCtx.MessageRetryAttempts <= 0 {
		return errors.New("archive: MessageRetryAttempts must be positive")
	}
	if len(archiveCtx.ClientName) > MaxClientNameLength {
		return errors.New(
			"archive: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if archiveCtx.Aeron == nil && archiveAeronDir(archiveCtx) == "" {
		return errors.New("archive: AeronDir is required when Aeron is not set")
	}
	return nil
}

// EffectiveControlRequestChannel returns the request URI after IPC-first selection.
func (archiveCtx Context) EffectiveControlRequestChannel() string {
	return archiveCtx.effectiveControlRequestChannel
}

// EffectiveControlResponseChannel returns the response subscription URI after
// IPC-first selection.
func (archiveCtx Context) EffectiveControlResponseChannel() string {
	return archiveCtx.effectiveControlResponseChannel
}

// EffectiveControlRequestStreamID returns the request stream id after IPC-first
// selection.
func (archiveCtx Context) EffectiveControlRequestStreamID() int32 {
	return archiveCtx.effectiveControlRequestStreamID
}

// EffectiveControlResponseStreamID returns the response stream id after IPC-first
// selection.
func (archiveCtx Context) EffectiveControlResponseStreamID() int32 {
	return archiveCtx.effectiveControlResponseStreamID
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when archiveCtx.Aeron is set; true when connect creates an owned client.
func (archiveCtx Context) OwnsAeronClient() bool {
	return archiveCtx.Aeron == nil
}

func applyEffectiveChannels(archiveCtx Context) Context {
	if archiveCtx.LocalControlChannel != "" {
		requestStreamID := archiveCtx.LocalControlStreamID
		if requestStreamID == 0 {
			requestStreamID = archiveCtx.ControlRequestStreamID
		}
		archiveCtx.effectiveControlRequestChannel = archiveCtx.LocalControlChannel
		archiveCtx.effectiveControlResponseChannel = archiveCtx.LocalControlChannel
		archiveCtx.effectiveControlRequestStreamID = requestStreamID
		archiveCtx.effectiveControlResponseStreamID = requestStreamID
		return archiveCtx
	}

	responseChannel := archiveCtx.ControlResponseChannel
	if responseChannel == "" {
		responseChannel = archiveCtx.ControlRequestChannel
	}
	requestStreamID := archiveCtx.ControlRequestStreamID
	responseStreamID := archiveCtx.ControlResponseStreamID
	if responseStreamID == 0 {
		responseStreamID = requestStreamID
	}
	archiveCtx.effectiveControlRequestChannel = archiveCtx.ControlRequestChannel
	archiveCtx.effectiveControlResponseChannel = responseChannel
	archiveCtx.effectiveControlRequestStreamID = requestStreamID
	archiveCtx.effectiveControlResponseStreamID = responseStreamID
	return archiveCtx
}

func archiveAeronDir(archiveCtx Context) string {
	if archiveCtx.AeronDir != "" {
		return archiveCtx.AeronDir
	}
	return archiveCtx.AeronContext.AeronDir
}

// concludeContext finalizes channel defaults and resolves the transport client.
//
// ctx bounds owned transport attach via aeron.Connect only. Shared Aeron and
// test factories ignore ctx. Do not store ctx on Context.
func concludeContext(
	ctx context.Context,
	archiveCtx Context,
) (Context, *aeron.Client, bool, error) {
	archiveCtx = applyEffectiveChannels(archiveCtx)
	responseChannel, err := applyUDPRejoinFalse(archiveCtx.effectiveControlResponseChannel)
	if err != nil {
		return Context{}, nil, false, err
	}
	archiveCtx.effectiveControlResponseChannel = responseChannel

	if archiveCtx.Aeron != nil {
		return archiveCtx, archiveCtx.Aeron, false, nil
	}
	aeronCtx := archiveCtx.AeronContext
	if dir := archiveAeronDir(archiveCtx); dir != "" {
		aeronCtx.AeronDir = dir
	}
	if aeronCtx.ErrorHandler == nil {
		aeronCtx.ErrorHandler = defaultArchiveConnectErrorHandler
	}
	var client *aeron.Client
	if factory, ok := ownedConnectClientFactoryForContext(archiveCtx); ok {
		client, err = factory(archiveCtx)
	} else {
		client, err = aeron.Connect(ctx, aeronCtx)
	}
	if err != nil {
		return Context{}, nil, false, err
	}
	archiveCtx.Aeron = client
	return archiveCtx, client, true, nil
}

func defaultArchiveConnectErrorHandler(err error) {
	panic(err)
}
