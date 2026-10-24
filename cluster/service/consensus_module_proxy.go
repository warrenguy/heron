package service

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/buffer"
)

// ConsensusModuleProxy publishes service-to-module control frames on the
// consensus module stream (default 105). Frames are ServiceAck (template 33),
// ScheduleTimer (template 31), CancelTimer (template 32), and CloseSession
// (template 30).
//
// Publish path: encode into scratch then Offer. Fixed-length frames stay
// within MaxPayloadLength under normal publication sizes, so the offer path
// replaces upstream tryClaim for these frames.
//
// Concurrency: invoker-only.
//
// Ownership: the caller owns the Publication. ConsensusModuleProxy does not
// close the publication on replace.
type ConsensusModuleProxy struct {
	publication *aeron.Publication

	// scratch is the encode buffer. SetPublication sizes it to
	// MaxPayloadLength for the single-frame hot path. Larger totals grow it up
	// to MaxMessageLength. Retained across publishes.
	scratch []byte
}

// NewConsensusModuleProxy returns a proxy with no publication bound. Bind via
// SetPublication.
func NewConsensusModuleProxy() *ConsensusModuleProxy {
	return &ConsensusModuleProxy{}
}

// SetPublication binds or replaces the consensus module control publication
// (stream 105).
//
// Behaviour: when pub is non-nil, grows scratch to at least MaxPayloadLength so
// later single-frame encodes avoid growing on the hot path. Totals above
// MaxPayloadLength still grow scratch up to MaxMessageLength on publish.
//
// Ownership: does not close the previous publication. The caller owns
// lifecycle.
func (p *ConsensusModuleProxy) SetPublication(pub *aeron.Publication) {
	p.publication = pub
	if pub == nil {
		return
	}
	p.scratch = buffer.GrowScratch(p.scratch, pub.MaxPayloadLength())
}

// Publication returns the bound publication, or nil.
func (p *ConsensusModuleProxy) Publication() *aeron.Publication {
	return p.publication
}

// scratchFor returns a buffer of length total for encoding. Totals above the
// current capacity grow scratch geometrically. The grown buffer is retained.
func (p *ConsensusModuleProxy) scratchFor(total int) []byte {
	p.scratch = buffer.GrowScratch(p.scratch, total)
	return p.scratch
}

// offerEncoded offers the encoded bytes in buf under [aeron.SendAttempts].
//
// The caller must bind the publication and encode into buf before calling.
// Retry policy is [aeron.OfferAttempts].
func (p *ConsensusModuleProxy) offerEncoded(buf []byte) int64 {
	return aeron.OfferAttempts(p.publication, buf, aeron.SendAttempts)
}

// publishEncoded checks MaxMessageLength, encodes into scratch, and offers.
//
// encode returns false when the encoder fails after the length check.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when total exceeds MaxMessageLength
//   - NotConnected when encode returns false
//   - Offer codes from [ConsensusModuleProxy.offerEncoded]
func (p *ConsensusModuleProxy) publishEncoded(
	total int,
	encode func(scratch []byte) bool,
) int64 {
	if p == nil || p.publication == nil {
		return aeron.Closed
	}
	if total > p.publication.MaxMessageLength() {
		return aeron.UnsupportedMessageLength
	}
	scratch := p.scratchFor(total)
	if !encode(scratch) {
		return aeron.NotConnected
	}
	return p.offerEncoded(scratch)
}

// publishFixed encodes a typed fixed-length frame into scratch and offers the
// frame. Returns the result of [ConsensusModuleProxy.publishEncoded].
func publishFixed[F any](
	p *ConsensusModuleProxy,
	total int,
	actingVersion uint16,
	fields F,
	encode func([]byte, int, uint16, F) (int, bool),
) int64 {
	return p.publishEncoded(total, func(scratch []byte) bool {
		_, encOK := encode(scratch, 0, actingVersion, fields)
		return encOK
	})
}

// PublishServiceAck encodes ServiceAck (template 33) into scratch and offers.
// Fixed length. Stays within MaxPayloadLength under normal publication sizes.
//
// Behaviour: encodes all ServiceAckFields. Does not validate field semantics.
// Scratch-then-offer; not upstream tryClaim.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when encoded length exceeds MaxMessageLength
//     (does not call Offer)
//   - NotConnected when encode fails after the length check
//   - Offer codes from [ConsensusModuleProxy.offerEncoded]
func (p *ConsensusModuleProxy) PublishServiceAck(fields codecs.ServiceAckFields) int64 {
	return publishFixed(
		p,
		codecs.ServiceAckEncodedLen(),
		uint16(clusterwire.ServiceAckSbeSchemaVersion),
		fields,
		codecs.EncodeServiceAck,
	)
}

// PublishScheduleTimer encodes ScheduleTimer (template 31) into scratch and
// offers one fixed-length frame.
//
// Behaviour: encodes all ScheduleTimerFields. Does not validate field
// semantics. Scratch-then-offer. Does not use tryClaim.
//
// Returns the same codes as [ConsensusModuleProxy.PublishServiceAck].
func (p *ConsensusModuleProxy) PublishScheduleTimer(fields codecs.ScheduleTimerFields) int64 {
	return publishFixed(
		p,
		codecs.ScheduleTimerEncodedLen(),
		uint16(clusterwire.ScheduleTimerSbeSchemaVersion),
		fields,
		codecs.EncodeScheduleTimer,
	)
}

// PublishCancelTimer encodes CancelTimer (template 32) into scratch and offers
// one fixed-length frame.
//
// Behaviour: encodes all CancelTimerFields. Does not validate field semantics.
// Scratch-then-offer. Does not use tryClaim.
//
// Returns the same codes as [ConsensusModuleProxy.PublishServiceAck].
func (p *ConsensusModuleProxy) PublishCancelTimer(fields codecs.CancelTimerFields) int64 {
	return publishFixed(
		p,
		codecs.CancelTimerEncodedLen(),
		uint16(clusterwire.CancelTimerSbeSchemaVersion),
		fields,
		codecs.EncodeCancelTimer,
	)
}

// PublishCloseSession encodes CloseSession (template 30) into scratch and
// offers one fixed-length frame.
//
// Behaviour: encodes all CloseSessionFields. Does not validate field
// semantics. Scratch-then-offer. Does not use tryClaim.
//
// Returns the same codes as [ConsensusModuleProxy.PublishServiceAck].
func (p *ConsensusModuleProxy) PublishCloseSession(fields codecs.CloseSessionFields) int64 {
	return publishFixed(
		p,
		codecs.CloseSessionEncodedLen(),
		uint16(clusterwire.CloseSessionSbeSchemaVersion),
		fields,
		codecs.EncodeCloseSession,
	)
}
