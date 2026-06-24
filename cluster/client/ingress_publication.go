package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

// IngressPublication is the connect-path ingress handle.
//
// Implemented by *aeron.ExclusivePublication (default) and *aeron.Publication.
type IngressPublication interface {
	Offer(buffer []byte) int64
	OfferWithOffset(buffer []byte, offset, length int) int64
	TryClaim(length int) (termIndex, frameOffset int, position int64, err error)
	Commit(termIndex, frameOffset int) error
	Abort(termIndex, frameOffset int) error
	MaxPayloadLength() int
	MaxMessageLength() int
	TermBuffer(termIndex int) atomic.AtomicBuffer
	IsConnected() bool
	IsClosed() bool
	Close() error
}

var (
	_ IngressPublication = (*aeron.ExclusivePublication)(nil)
	_ IngressPublication = (*aeron.Publication)(nil)
)

// ingressSessionLimits returns ingress scratch buffer length and max single-frame
// length for a connect-path ingress publication. Exclusive publications use
// header+maxPayload scratch and enforce maxPayload per frame; concurrent
// publications size scratch to MaxMessageLength (total Offer length) and
// allow fragmentation (maxFrameLen 0).
func ingressSessionLimits(pub IngressPublication) (scratchLen, maxFrameLen int) {
	headerLen := SessionHeaderLength()
	maxPayload := pub.MaxPayloadLength()
	if _, ok := pub.(*aeron.ExclusivePublication); ok {
		return headerLen + maxPayload, maxPayload
	}
	return pub.MaxMessageLength(), 0
}
