package client

import (
	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/codecs"
)

// handleEgressFragment decodes cluster egress SBE and dispatches to handler.
// SessionEvent, NewLeader, and AdminResponse pass []byte views cloned by
// TryDecodeFromClusterLayout (retain-safe); Message uses the live fragment buffer.
func handleEgressFragment(
	handler EgressHandler,
	clusterSessionID int64,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	fragment, ok := egressFragmentPayload(buffer, offset, length)
	if !ok {
		return
	}

	layout, ok := codecs.ClusterMessageLayoutAt(fragment, 0, codecs.ClusterDecodeMaxVarLen)
	if !ok {
		return
	}

	switch layout.TemplateID {
	case gensbe.SessionMessageHeaderSbeTemplateID:
		fields, ok := codecs.SessionMessageHeaderFieldsFromLayout(fragment, layout)
		if !ok || fields.ClusterSessionID != clusterSessionID {
			return
		}
		hdrLen := SessionHeaderLength()
		handler.Message(
			fields.ClusterSessionID,
			fields.Timestamp,
			buffer,
			offset+hdrLen,
			length-hdrLen,
			header,
		)

	case gensbe.ChallengeSbeTemplateID:
		// Connect egress poller owns Challenge.
		return

	case gensbe.SessionEventSbeTemplateID:
		decoded, ok := codecs.TryDecodeFromClusterLayout(fragment, layout, codecs.ClusterDecodeMaxVarLen)
		if !ok {
			return
		}
		ev, ok := decoded.Fields.(codecs.SessionEventFields)
		if !ok || ev.ClusterSessionID != clusterSessionID {
			return
		}
		handler.SessionEvent(
			ev.CorrelationID,
			ev.ClusterSessionID,
			ev.LeadershipTermID,
			ev.LeaderMemberID,
			ev.Code,
			ev.Detail,
		)

	case gensbe.NewLeaderEventSbeTemplateID:
		decoded, ok := codecs.TryDecodeFromClusterLayout(fragment, layout, codecs.ClusterDecodeMaxVarLen)
		if !ok {
			return
		}
		leader, ok := decoded.Fields.(codecs.NewLeaderEventFields)
		if !ok || leader.ClusterSessionID != clusterSessionID {
			return
		}
		handler.NewLeader(
			leader.ClusterSessionID,
			leader.LeadershipTermID,
			leader.LeaderMemberID,
			leader.IngressEndpoints,
		)

	case gensbe.AdminResponseSbeTemplateID:
		decoded, ok := codecs.TryDecodeFromClusterLayout(fragment, layout, codecs.ClusterDecodeMaxVarLen)
		if !ok {
			return
		}
		admin, ok := decoded.Fields.(codecs.AdminResponseFields)
		if !ok || admin.ClusterSessionID != clusterSessionID {
			return
		}
		handler.AdminResponse(
			admin.ClusterSessionID,
			admin.CorrelationID,
			admin.RequestType,
			admin.ResponseCode,
			admin.Message,
			admin.Payload,
		)
	}
}

func egressFragmentSpanValid(payload []byte, offset, length int) bool {
	return offset >= 0 &&
		length >= 0 &&
		length >= codecs.MessageHeaderEncodedLen() &&
		offset <= len(payload)-length
}

func decodeEgressFragment(fragment []byte) (codecs.DecodedMessage, bool) {
	return codecs.TryDecodeByTemplate(fragment, 0, codecs.ClusterDecodeMaxVarLen)
}

func egressFragmentPayload(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) ([]byte, bool) {
	payload := buffer.Bytes()
	if !egressFragmentSpanValid(payload, offset, length) {
		return nil, false
	}
	return payload[offset : offset+length], true
}
