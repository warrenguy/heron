package archive

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/idlestrategy"
)

// Config holds invoker-only Aeron Archive control client configuration.
//
// Normalize before ValidateForConnect and before connect.
//
// During AsyncConnect.Poll and the post-connect session duty cycle, the caller
// must invoke DoWork on the transport aeron.Client between idle or backoff steps.
//
// There is no AgentInvoker field.
type Config struct {
	// ControlRequestChannel is the archive control request URI.
	//
	// When LocalControlChannel is set, AsyncConnect prefers local IPC first (Java parity).
	ControlRequestChannel string

	// ControlResponseChannel is the subscription channel for control responses.
	//
	// Empty defaults to DefaultControlResponseChannel after Normalize.
	ControlResponseChannel string

	// LocalControlChannel is the co-located archive IPC control URI.
	//
	// Non-empty causes connect to use this as the request channel (IPC-first).
	//
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

	AeronDir    string
	Aeron       *aeron.Client
	AeronConfig aeron.Config

	CredentialsSupplier CredentialsSupplier

	// RecordingSignalHandler receives recording signals on Poll and during blocking
	// control awaits that use this handler:
	//   - StartRecording
	//   - StopRecording
	//   - StartReplay
	//   - StopReplay
	//   - ListRecording
	//
	// StartRecordingAwaitSignal and StopRecordingAwaitSignal poll internally.
	//
	// Those methods route START and STOP proofs to the per-call tracker, not this
	// handler.
	//
	// Nil drops signals until Client.SetRecordingSignalHandler installs a handler.
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
func (cfg Config) WithRemoteControl() Config {
	cfg.remoteControlExplicit = true
	cfg.LocalControlChannel = ""
	return cfg
}

// Normalize applies archive defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (cfg Config) Normalize() Config {
	if !cfg.remoteControlExplicit && cfg.LocalControlChannel == "" {
		cfg.LocalControlChannel = DefaultLocalControlChannel
	}
	if cfg.ControlRequestChannel == "" {
		cfg.ControlRequestChannel = DefaultControlRequestChannel
	}
	if cfg.ControlResponseChannel == "" {
		cfg.ControlResponseChannel = DefaultControlResponseChannel
	}
	if cfg.LocalControlStreamID == 0 {
		cfg.LocalControlStreamID = DefaultLocalControlStreamID
	}
	if cfg.ControlRequestStreamID == 0 {
		cfg.ControlRequestStreamID = DefaultControlRequestStreamID
	}
	if cfg.ControlResponseStreamID == 0 {
		cfg.ControlResponseStreamID = DefaultControlResponseStreamID
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = DefaultConnectTimeout
	}
	if cfg.ControlResponseTimeout == 0 {
		cfg.ControlResponseTimeout = DefaultControlResponseTimeout
	}
	if cfg.ControlOfferRetryTimeout == 0 {
		cfg.ControlOfferRetryTimeout = DefaultControlOfferRetryTimeout
	}
	if cfg.MessageRetryAttempts == 0 {
		cfg.MessageRetryAttempts = DefaultMessageRetryAttempts
	}
	if cfg.CredentialsSupplier == nil {
		cfg.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if cfg.IdleStrategy == nil {
		cfg.IdleStrategy = NewDefaultIdleStrategy()
	}
	if cfg.ClientName == "" {
		cfg.ClientName = defaultClientName
	}
	if cfg.AeronDir != "" && cfg.AeronConfig.AeronDir == "" {
		cfg.AeronConfig.AeronDir = cfg.AeronDir
	}
	return cfg
}

// ValidateForConnect checks archive config for async connect. Applies Normalize and
// effective channel selection on a stack copy without mutating the receiver. All
// errors use the archive: prefix.
func (cfg Config) ValidateForConnect() error {
	cfg = applyEffectiveChannels(cfg.Normalize())

	requestChannel := cfg.effectiveControlRequestChannel
	responseChannel := cfg.effectiveControlResponseChannel
	if err := validateArchiveIngressChannel(requestChannel, "request"); err != nil {
		return err
	}
	if requestChannel != responseChannel {
		if err := validateArchiveIngressChannel(responseChannel, "response"); err != nil {
			return err
		}
	}

	if cfg.ConnectTimeout <= 0 {
		return errors.New("archive: ConnectTimeout must be positive")
	}
	if cfg.ControlResponseTimeout <= 0 {
		return errors.New("archive: ControlResponseTimeout must be positive")
	}
	if cfg.ControlOfferRetryTimeout <= 0 {
		return errors.New("archive: ControlOfferRetryTimeout must be positive")
	}
	if cfg.MessageRetryAttempts <= 0 {
		return errors.New("archive: MessageRetryAttempts must be positive")
	}
	if len(cfg.ClientName) > MaxClientNameLength {
		return errors.New(
			"archive: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if cfg.Aeron == nil && archiveAeronDir(cfg) == "" {
		return errors.New("archive: AeronDir is required when Aeron is not set")
	}
	return nil
}

// EffectiveControlRequestChannel returns the request URI after IPC-first selection.
func (cfg Config) EffectiveControlRequestChannel() string {
	return cfg.effectiveControlRequestChannel
}

// EffectiveControlResponseChannel returns the response subscription URI after
// IPC-first selection.
func (cfg Config) EffectiveControlResponseChannel() string {
	return cfg.effectiveControlResponseChannel
}

// EffectiveControlRequestStreamID returns the request stream id after IPC-first
// selection.
func (cfg Config) EffectiveControlRequestStreamID() int32 {
	return cfg.effectiveControlRequestStreamID
}

// EffectiveControlResponseStreamID returns the response stream id after IPC-first
// selection.
func (cfg Config) EffectiveControlResponseStreamID() int32 {
	return cfg.effectiveControlResponseStreamID
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when cfg.Aeron is set; true when connect creates an owned client.
func (cfg Config) OwnsAeronClient() bool {
	return cfg.Aeron == nil
}

func applyEffectiveChannels(cfg Config) Config {
	if cfg.LocalControlChannel != "" {
		requestStreamID := cfg.LocalControlStreamID
		if requestStreamID == 0 {
			requestStreamID = cfg.ControlRequestStreamID
		}
		cfg.effectiveControlRequestChannel = cfg.LocalControlChannel
		cfg.effectiveControlResponseChannel = cfg.LocalControlChannel
		cfg.effectiveControlRequestStreamID = requestStreamID
		cfg.effectiveControlResponseStreamID = requestStreamID
		return cfg
	}

	responseChannel := cfg.ControlResponseChannel
	if responseChannel == "" {
		responseChannel = cfg.ControlRequestChannel
	}
	requestStreamID := cfg.ControlRequestStreamID
	responseStreamID := cfg.ControlResponseStreamID
	if responseStreamID == 0 {
		responseStreamID = requestStreamID
	}
	cfg.effectiveControlRequestChannel = cfg.ControlRequestChannel
	cfg.effectiveControlResponseChannel = responseChannel
	cfg.effectiveControlRequestStreamID = requestStreamID
	cfg.effectiveControlResponseStreamID = responseStreamID
	return cfg
}

func archiveAeronDir(cfg Config) string {
	if cfg.AeronDir != "" {
		return cfg.AeronDir
	}
	return cfg.AeronConfig.AeronDir
}

// concludeConfig finalizes channel defaults and resolves the transport client.
//
// ctx bounds owned transport attach via aeron.Connect only. Shared Aeron and
// test factories ignore ctx. Do not store ctx on Config.
func concludeConfig(
	ctx context.Context,
	cfg Config,
) (Config, *aeron.Client, bool, error) {
	cfg = applyEffectiveChannels(cfg)
	responseChannel, err := applyUDPRejoinFalse(cfg.effectiveControlResponseChannel)
	if err != nil {
		return Config{}, nil, false, err
	}
	cfg.effectiveControlResponseChannel = responseChannel

	if cfg.Aeron != nil {
		return cfg, cfg.Aeron, false, nil
	}
	aeronCfg := cfg.AeronConfig
	if dir := archiveAeronDir(cfg); dir != "" {
		aeronCfg.AeronDir = dir
	}
	if aeronCfg.ErrorHandler == nil {
		aeronCfg.ErrorHandler = defaultArchiveConnectErrorHandler
	}
	var client *aeron.Client
	if factory, ok := ownedConnectClientFactoryForConfig(cfg); ok {
		client, err = factory(cfg)
	} else {
		client, err = aeron.Connect(ctx, aeronCfg)
	}
	if err != nil {
		return Config{}, nil, false, err
	}
	cfg.Aeron = client
	return cfg, client, true, nil
}

func defaultArchiveConnectErrorHandler(err error) {
	panic(err)
}
