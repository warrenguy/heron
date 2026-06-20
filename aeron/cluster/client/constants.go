package client

import (
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/sbe"
)

const (
	// DefaultIngressStreamID is the Java AeronCluster ingress stream default (101).
	DefaultIngressStreamID int32 = 101

	// DefaultEgressStreamID is the Java AeronCluster egress stream default (102).
	DefaultEgressStreamID int32 = 102

	// DefaultMessageTimeout matches Java messageTimeoutNs (5 seconds).
	DefaultMessageTimeout = 5 * time.Second

	// MaxClientNameLength is the wire limit for SessionConnectRequest.clientInfo.
	MaxClientNameLength = 100
)

// ProtocolSemanticVersion returns the int32 wire value for SessionConnectRequest.version (0.3.0).
// Packing: (major << 16) | (minor << 8) | patch; Agrona SemanticVersion.compose.
func ProtocolSemanticVersion() int32 {
	return cnc.ComposeSemanticVersion(0, 3, 0)
}

// SessionHeaderLength equals Java AeronCluster.SESSION_HEADER_LENGTH.
func SessionHeaderLength() int {
	return sbe.SessionMessageHeaderEncodedLen()
}
