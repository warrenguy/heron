package codecs

import (
	"errors"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var sessionMessageHeaderEncodedLen = int((&clusterwire.SessionMessageHeader{}).
	SbeBlockAndHeaderLength())

// SessionMessageHeaderEncodedLen is the wire size of MessageHeader plus SessionMessageHeader.
func SessionMessageHeaderEncodedLen() int {
	return sessionMessageHeaderEncodedLen
}

// ErrIngressHeaderBufferTooSmall is returned when buf lacks room for the ingress
// SessionMessageHeader at frameOffset+logbuffer.OffsetFramePayload.
var ErrIngressHeaderBufferTooSmall = errors.New("sbe: ingress header buffer too small")

type IngressHeaderFields struct {
	LeadershipTermID int64
	ClusterSessionID int64
	Timestamp        int64
}

// EncodeIngressHeader matches Java wrapAndApplyHeader layout at
// frameOffset+logbuffer.OffsetFramePayload. Use it when writing into a
// logbuffer term frame (TryClaim/CommitClaim) where the Aeron frame header
// precedes the payload.
//
// buf must provide at least SessionMessageHeaderEncodedLen bytes from that offset.
//
// For frameless scratch buffers (Offer), use EncodeIngressHeaderScratch instead.
func EncodeIngressHeader(buf []byte, frameOffset int, fields IngressHeaderFields) (int, error) {
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	bufLen := len(buf)
	if payloadOffset < 0 || payloadOffset > bufLen-sessionMessageHeaderEncodedLen {
		return 0, ErrIngressHeaderBufferTooSmall
	}

	var header clusterwire.SessionMessageHeader
	header.WrapAndApplyHeader(buf, uint64(payloadOffset), uint64(bufLen)).
		SetLeadershipTermId(fields.LeadershipTermID).
		SetClusterSessionId(fields.ClusterSessionID).
		SetTimestamp(fields.Timestamp)
	return sessionMessageHeaderEncodedLen, nil
}

// EncodeIngressHeaderScratch writes SessionMessageHeader at buf offset 0 for
// frameless ingress scratch buffers used by Offer/OfferWithOffset. It is
// equivalent to EncodeIngressHeader with frameOffset -logbuffer.OffsetFramePayload
// so the header lands at the start of buf without an Aeron frame prefix.
//
// buf must provide at least SessionMessageHeaderEncodedLen bytes.
func EncodeIngressHeaderScratch(buf []byte, fields IngressHeaderFields) (int, error) {
	return EncodeIngressHeader(buf, -logbuffer.OffsetFramePayload, fields)
}
