package sbe

import (
	"errors"

	"github.com/warrenguy/heron/aeron/logbuffer"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

var sessionMessageHeaderEncodedLen = messageHeaderEncodedLength +
	int(gensbe.SessionMessageHeaderSbeBlockLength)

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
// frameOffset+logbuffer.OffsetFramePayload.
//
// buf must provide at least SessionMessageHeaderEncodedLen bytes from that offset.
func EncodeIngressHeader(buf []byte, frameOffset int, fields IngressHeaderFields) (int, error) {
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	bufLen := len(buf)
	if payloadOffset < 0 || payloadOffset > bufLen-sessionMessageHeaderEncodedLen {
		return 0, ErrIngressHeaderBufferTooSmall
	}

	var header gensbe.SessionMessageHeader
	header.WrapAndApplyHeader(buf, uint64(payloadOffset), uint64(bufLen)).
		SetLeadershipTermId(fields.LeadershipTermID).
		SetClusterSessionId(fields.ClusterSessionID).
		SetTimestamp(fields.Timestamp)
	return sessionMessageHeaderEncodedLen, nil
}
