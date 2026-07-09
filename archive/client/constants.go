package client

import (
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
)

const (
	// DefaultLocalControlChannel is the co-located archive IPC control URI applied by
	// Normalize for IPC-first connect. Call WithRemoteControl() before Normalize for
	// remote UDP archive connect.
	DefaultLocalControlChannel = "aeron:ipc?term-length=64k"

	// DefaultControlRequestChannel matches Java CONTROL_REQUEST_CHANNEL_DEFAULT.
	DefaultControlRequestChannel = "aeron:udp?endpoint=localhost:8010"

	// DefaultControlResponseChannel matches Java CONTROL_RESPONSE_CHANNEL_DEFAULT.
	DefaultControlResponseChannel = "aeron:udp?endpoint=localhost:0"

	// DefaultLocalControlStreamID matches Java localControlStreamId.
	DefaultLocalControlStreamID int32 = 10

	// DefaultControlRequestStreamID matches Java CONTROL_STREAM_ID_DEFAULT.
	DefaultControlRequestStreamID int32 = 10

	// DefaultControlResponseStreamID is zero; effective selection uses the request
	// stream id when unset.
	DefaultControlResponseStreamID int32 = 0

	// DefaultConnectTimeout matches Java CONNECT_TIMEOUT_DEFAULT_NS (5 seconds).
	DefaultConnectTimeout = 5 * time.Second

	// DefaultControlResponseTimeout matches Java messageTimeoutNs for control response
	// awaits (5 seconds).
	DefaultControlResponseTimeout = 5 * time.Second

	// DefaultControlOfferRetryTimeout bounds ControlOfferUntilPositive when
	// ArchiveContext.ControlOfferRetryTimeout is zero.
	DefaultControlOfferRetryTimeout = 10 * time.Second

	// DefaultMessageRetryAttempts matches Java MESSAGE_RETRY_ATTEMPTS_DEFAULT.
	DefaultMessageRetryAttempts = 3

	// MaxClientNameLength caps ArchiveContext.ClientName at connect validation;
	// wire clientInfo encodes as "name=<ClientName> heron".
	MaxClientNameLength = 100

	// Control-response poller fragment limits (connect vs steady-state polling).
	DefaultConnectControlResponseFragmentLimit = 1 // matches cluster connect egress limit
	// matches Java ControlResponsePoller.FRAGMENT_LIMIT
	DefaultControlResponseFragmentLimit = 10

	defaultArchiveClientName = "archive-client"
)

// ProtocolSemanticVersion returns the int32 wire value for AuthConnectRequest.version (1.12.0).
// Matches Java AeronArchive.Configuration PROTOCOL_*.
//
// Distinct from the SBE archive schema semantic version 5.2 (archivewire acting version): wire
// version negotiates archive protocol compatibility; SBE version selects control-channel
// message layout. Do not substitute ComposeSemanticVersion(5, 2, 0) here.
//
// Packing: (major << 16) | (minor << 8) | patch; Agrona SemanticVersion.compose.
func ProtocolSemanticVersion() int32 {
	return cnc.ComposeSemanticVersion(1, 12, 0)
}
