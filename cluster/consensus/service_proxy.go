package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/buffer"
)

// ServiceProxy publishes module-to-service control frames on the service
// stream (default 104). Frames are JoinLog (template 40) and
// RequestServiceAck (template 108).
//
// Concurrency: invoker-only.
//
// Ownership: the caller owns the Publication. ServiceProxy does not close the
// publication on replace.
type ServiceProxy struct {
	publication *aeron.Publication

	// scratch is the encode buffer. SetPublication sizes it to
	// MaxPayloadLength for the single-frame hot path. Larger totals grow it up
	// to MaxMessageLength. Retained across publishes.
	scratch []byte
}

// NewServiceProxy returns a proxy with no publication bound. Bind via
// SetPublication.
func NewServiceProxy() *ServiceProxy {
	return &ServiceProxy{}
}

// SetPublication binds or replaces the service control publication.
//
// Behaviour: when pub is non-nil, grows scratch to at least MaxPayloadLength so
// later single-frame encodes avoid growing on the hot path. Totals above
// MaxPayloadLength still grow scratch up to MaxMessageLength on publish.
//
// Ownership: does not close the previous publication. The caller owns
// lifecycle.
func (p *ServiceProxy) SetPublication(pub *aeron.Publication) {
	p.publication = pub
	if pub == nil {
		return
	}
	p.scratch = buffer.GrowScratch(p.scratch, pub.MaxPayloadLength())
}

// Publication returns the bound publication, or nil.
func (p *ServiceProxy) Publication() *aeron.Publication {
	return p.publication
}

// scratchFor returns a buffer of length total for encoding. Totals above the
// current capacity grow scratch geometrically. The grown buffer is retained.
func (p *ServiceProxy) scratchFor(total int) []byte {
	p.scratch = buffer.GrowScratch(p.scratch, total)
	return p.scratch
}

// serviceProxySendAttempts is the module-to-service control offer budget.
// Matches Java io.aeron.cluster.ServiceProxy SEND_ATTEMPTS (5).
const serviceProxySendAttempts = 5

// offerEncoded offers the encoded bytes in buf under serviceProxySendAttempts.
//
// The caller must bind the publication and encode into buf before calling.
// Retry policy is [aeron.OfferAttempts].
func (p *ServiceProxy) offerEncoded(buf []byte) int64 {
	return aeron.OfferAttempts(p.publication, buf, serviceProxySendAttempts)
}

// publishEncoded checks MaxMessageLength, encodes into scratch, and offers.
//
// encode returns false when the encoder fails after the length check.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when total exceeds MaxMessageLength
//   - NotConnected when encode returns false
//   - Offer codes from [ServiceProxy.offerEncoded]
func (p *ServiceProxy) publishEncoded(
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

// PublishJoinLog encodes JoinLog (template 40) into scratch and offers.
// Totals above MaxPayloadLength fragment when they fit MaxMessageLength.
//
// Behaviour: copies LogChannel into scratch. Does not retain the caller slice.
// Does not validate field semantics. Checks nil receiver and unbound
// publication before any length check.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when ClusterWireMessageLen fails or total
//     exceeds MaxMessageLength (does not call Offer)
//   - NotConnected when encode fails after the length checks
//   - Offer codes from [ServiceProxy.offerEncoded] (same retry budget and
//     terminal-stop policy as PublishRequestServiceAck)
func (p *ServiceProxy) PublishJoinLog(fields codecs.JoinLogFields) int64 {
	if p == nil || p.publication == nil {
		return aeron.Closed
	}
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)
	total, ok := codecs.ClusterWireMessageLen(
		codecs.JoinLogBlockLength(actingVersion),
		len(fields.LogChannel),
	)
	if !ok {
		return aeron.UnsupportedMessageLength
	}
	return p.publishEncoded(total, func(scratch []byte) bool {
		_, encOK := codecs.EncodeJoinLog(scratch, 0, actingVersion, fields)
		return encOK
	})
}

// PublishRequestServiceAck encodes RequestServiceAck (template 108) into
// scratch and offers. Fixed length. Stays within MaxPayloadLength under
// normal publication sizes.
//
// Behaviour: encodes LogPosition only. Does not validate field semantics.
//
// Returns stream position (>0) on success, or an aeron publication code:
//   - Closed when the receiver or publication is nil
//   - UnsupportedMessageLength when encoded length exceeds MaxMessageLength
//     (does not call Offer)
//   - NotConnected when encode fails after the length check
//   - Offer codes from [ServiceProxy.offerEncoded] (same retry budget and
//     terminal-stop policy as PublishJoinLog)
func (p *ServiceProxy) PublishRequestServiceAck(logPosition int64) int64 {
	return p.publishEncoded(
		codecs.RequestServiceAckEncodedLen(),
		func(scratch []byte) bool {
			_, encOK := codecs.EncodeRequestServiceAck(
				scratch,
				0,
				uint16(clusterwire.RequestServiceAckSbeSchemaVersion),
				codecs.RequestServiceAckFields{LogPosition: logPosition},
			)
			return encOK
		},
	)
}
