package client

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

var errEgressHandlerMessageNotConfigured = errors.New(
	"cluster client: EgressHandler Message callback not configured",
)

// EgressHandler receives decoded cluster egress events. Message buffer views and the
// Header pointer are valid only for the callback duration; do not retain them.
type EgressHandler interface {
	// Message delivers an application payload. Offset points past the SessionMessageHeader;
	// buffer, offset, length, and header follow aeron.FragmentHandler lifetime rules.
	Message(
		clusterSessionID int64,
		timestamp int64,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	)

	// SessionEvent delivers template 2 (SessionEvent). detail is copied wire bytes;
	// safe to retain after return.
	SessionEvent(
		correlationID int64,
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		code EventCode,
		detail []byte,
	)

	// NewLeader delivers template 6 (NewLeaderEvent). ingressEndpoints is copied wire bytes
	// (e.g. "0=localhost:20110,..."); safe to retain after return.
	NewLeader(
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		ingressEndpoints []byte,
	)

	// AdminResponse delivers template 27 (AdminResponse). message and payload are
	// copied wire bytes; safe to retain after return.
	AdminResponse(
		clusterSessionID int64,
		correlationID int64,
		requestType AdminRequestType,
		responseCode AdminResponseCode,
		message []byte,
		payload []byte,
	)
}

// EgressHandlers is an ergonomic EgressHandler adapter with nil-guarded func fields.
// Unset func fields are no-ops. A nil MessageFunc passes Validate(); a nil
// EgressHandler interface value or typed-nil *EgressHandlers fails validation.
//
// Store *EgressHandlers in ClusterContext.EgressHandler (or keep one pointer when
// setting func fields after construction). Assigning a struct value copies handler
// state; later field writes on the original variable do not update the interface.
type EgressHandlers struct {
	MessageFunc func(
		clusterSessionID int64,
		timestamp int64,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	)
	SessionEventFunc func(
		correlationID int64,
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		code EventCode,
		detail []byte,
	)
	NewLeaderFunc func(
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		ingressEndpoints []byte,
	)
	AdminResponseFunc func(
		clusterSessionID int64,
		correlationID int64,
		requestType AdminRequestType,
		responseCode AdminResponseCode,
		message []byte,
		payload []byte,
	)
}

var _ EgressHandler = (*EgressHandlers)(nil)

func (h *EgressHandlers) Message(
	clusterSessionID int64,
	timestamp int64,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	if h.MessageFunc != nil {
		h.MessageFunc(clusterSessionID, timestamp, buffer, offset, length, header)
	}
}

func (h *EgressHandlers) SessionEvent(
	correlationID int64,
	clusterSessionID int64,
	leadershipTermID int64,
	leaderMemberID int32,
	code EventCode,
	detail []byte,
) {
	if h.SessionEventFunc != nil {
		h.SessionEventFunc(
			correlationID,
			clusterSessionID,
			leadershipTermID,
			leaderMemberID,
			code,
			detail,
		)
	}
}

func (h *EgressHandlers) NewLeader(
	clusterSessionID int64,
	leadershipTermID int64,
	leaderMemberID int32,
	ingressEndpoints []byte,
) {
	if h.NewLeaderFunc != nil {
		h.NewLeaderFunc(clusterSessionID, leadershipTermID, leaderMemberID, ingressEndpoints)
	}
}

func (h *EgressHandlers) AdminResponse(
	clusterSessionID int64,
	correlationID int64,
	requestType AdminRequestType,
	responseCode AdminResponseCode,
	message []byte,
	payload []byte,
) {
	if h.AdminResponseFunc != nil {
		h.AdminResponseFunc(clusterSessionID, correlationID, requestType, responseCode, message, payload)
	}
}

type noopEgressHandler struct{}

func (noopEgressHandler) SessionEvent(
	_ int64,
	_ int64,
	_ int64,
	_ int32,
	_ EventCode,
	_ []byte,
) {
}

func (noopEgressHandler) NewLeader(
	_ int64,
	_ int64,
	_ int32,
	_ []byte,
) {
}

func (noopEgressHandler) AdminResponse(
	_ int64,
	_ int64,
	_ AdminRequestType,
	_ AdminResponseCode,
	_ []byte,
	_ []byte,
) {
}

// ThrowingEgressHandler panics on Message simulating runtime dispatch failure when
// message callback not configured; NOT Java conclude() validation. Other methods are
// no-ops. Use in tests that require fail-fast on first message delivery.
type ThrowingEgressHandler struct {
	noopEgressHandler
	Err error
}

var _ EgressHandler = ThrowingEgressHandler{}

func (h ThrowingEgressHandler) Message(
	_ int64,
	_ int64,
	_ atomic.AtomicBuffer,
	_ int,
	_ int,
	_ *aeron.Header,
) {
	err := h.Err
	if err == nil {
		err = errEgressHandlerMessageNotConfigured
	}
	panic(err)
}
