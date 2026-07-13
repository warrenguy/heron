package archive

import "github.com/warrenguy/heron/aeron"

// ControlPublication is the archive control request handle.
//
// Connect path uses single-frame Offer only; TryClaim fragmentation is not required.
// Implemented by *aeron.ExclusivePublication (always used on connect path).
type ControlPublication interface {
	Offer(buffer []byte) int64
	IsConnected() bool
	IsClosed() bool
	Close() error
	MaxMessageLength() int
	MaxPayloadLength() int
}

var _ ControlPublication = (*aeron.ExclusivePublication)(nil)
