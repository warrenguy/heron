package client

import (
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
)

// imageFromHeader returns the egress Image from the fragment header, or nil when
// the header is nil or ImageFields().Context is not an *aeron.Image. Nil is
// non-fatal: recovery skips egress rebind without error.
func imageFromHeader(header *aeron.Header) *aeron.Image {
	if header == nil {
		return nil
	}
	image, _ := header.ImageFields().Context.(*aeron.Image)
	return image
}

// egressDispatchHooks runs session-recovery side effects before handler callbacks.
// Returning true suppresses the corresponding handler invocation.
type egressDispatchHooks struct {
	beforeSessionEvent func(ev codecs.SessionEventFields, header *aeron.Header) bool
	beforeNewLeader    func(leader codecs.NewLeaderEventFields, header *aeron.Header) bool
}

// handleEgressFragment decodes cluster egress SBE and dispatches to handler.
// SessionEvent, NewLeader, and AdminResponse pass []byte views cloned by
// TryDecodeFromClusterLayout (retain-safe); Message uses the live fragment buffer.
func handleEgressFragment(
	handler EgressHandler,
	clusterSessionID int64,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	dispatchEgressFragmentCore(
		handler,
		clusterSessionID,
		buffer,
		offset,
		length,
		header,
		egressDispatchHooks{},
	)
}

// dispatchSessionEgressFragment decodes cluster egress SBE and dispatches to handler.
// Session recovery hooks may suppress handler callbacks on redirect/new-leader errors;
// recovery state is observable via SessionState and PollEgress.
func dispatchSessionEgressFragment(
	ac *AeronCluster,
	handler EgressHandler,
	clusterSessionID int64,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	dispatchEgressFragmentCore(
		handler,
		clusterSessionID,
		buffer,
		offset,
		length,
		header,
		ac.egressHooks,
	)
}

func sessionEgressDispatchHooks(ac *AeronCluster) egressDispatchHooks {
	return egressDispatchHooks{
		beforeSessionEvent: func(ev codecs.SessionEventFields, header *aeron.Header) bool {
			switch ev.Code {
			case EventCodeRedirect:
				return ac.handleRedirect(ev.LeaderMemberID, ev.Detail, imageFromHeader(header)) != nil
			case EventCodeClosed:
				ac.enterPendingClose()
			case EventCodeOK, EventCodeError, EventCodeAuthenticationRejected, EventCodeNullValue:
			}
			return false
		},
		beforeNewLeader: func(leader codecs.NewLeaderEventFields, header *aeron.Header) bool {
			return ac.handleNewLeader(
				leader.ClusterSessionID,
				leader.LeadershipTermID,
				leader.LeaderMemberID,
				leader.IngressEndpoints,
				imageFromHeader(header),
			) != nil
		},
	}
}

func decodeEgressFragmentLayout(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) (fragment []byte, layout codecs.ClusterMessageLayout, ok bool) {
	fragment, ok = egressFragmentPayload(buffer, offset, length)
	if !ok {
		return nil, codecs.ClusterMessageLayout{}, false
	}
	layout, ok = codecs.ClusterMessageLayoutAt(fragment, 0, codecs.ClusterDecodeMaxVarLen)
	if !ok {
		return nil, codecs.ClusterMessageLayout{}, false
	}
	return fragment, layout, true
}

func dispatchEgressFragmentCore(
	handler EgressHandler,
	clusterSessionID int64,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
	hooks egressDispatchHooks,
) {
	fragment, layout, ok := decodeEgressFragmentLayout(buffer, offset, length)
	if !ok {
		return
	}

	switch layout.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
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

	case clusterwire.ChallengeSbeTemplateID:
		// Connect egress poller owns Challenge.
		return

	case clusterwire.SessionEventSbeTemplateID:
		decoded, ok := codecs.TryDecodeFromClusterLayout(fragment, layout, codecs.ClusterDecodeMaxVarLen)
		if !ok {
			return
		}
		ev, ok := decoded.Fields.(codecs.SessionEventFields)
		if !ok || ev.ClusterSessionID != clusterSessionID {
			return
		}
		if hooks.beforeSessionEvent != nil && hooks.beforeSessionEvent(ev, header) {
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

	case clusterwire.NewLeaderEventSbeTemplateID:
		decoded, ok := codecs.TryDecodeFromClusterLayout(fragment, layout, codecs.ClusterDecodeMaxVarLen)
		if !ok {
			return
		}
		leader, ok := decoded.Fields.(codecs.NewLeaderEventFields)
		if !ok || leader.ClusterSessionID != clusterSessionID {
			return
		}
		if hooks.beforeNewLeader != nil && hooks.beforeNewLeader(leader, header) {
			return
		}
		handler.NewLeader(
			leader.ClusterSessionID,
			leader.LeadershipTermID,
			leader.LeaderMemberID,
			leader.IngressEndpoints,
		)

	case clusterwire.AdminResponseSbeTemplateID:
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
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) ([]byte, bool) {
	payload := buffer.Bytes()
	if !egressFragmentSpanValid(payload, offset, length) {
		return nil, false
	}
	return payload[offset : offset+length], true
}
