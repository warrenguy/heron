package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
)

// ControlledEgressHandler mirrors upstream ControlledEgressListener.
//
// Only [Message] returns a controlled action; other callbacks always commit.
//
// Buffer views are valid only for the callback duration; do not retain slices
// or the [aeron.Header] pointer.
type ControlledEgressHandler interface {
	// Message delivers an application payload after SessionMessageHeader strip.
	//
	// Return [aeron.ControlledAction] ([aeron.ControlledAbort],
	// [aeron.ControlledBreak], [aeron.ControlledCommit],
	// [aeron.ControlledContinue]).
	Message(
		clusterSessionID int64,
		timestamp int64,
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction

	// SessionEvent delivers template 2 (SessionEvent). detail is raw ASCII bytes.
	//
	// code is [EventCode].
	SessionEvent(
		correlationID int64,
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		code EventCode,
		detail []byte,
	)

	// NewLeader delivers template 6 (NewLeaderEvent).
	//
	// ingressEndpoints is raw ASCII bytes (e.g. "0=localhost:20110,...").
	NewLeader(
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		ingressEndpoints []byte,
	)

	// AdminResponse delivers template 27 (AdminResponse).
	//
	// requestType is [AdminRequestType]; responseCode is [AdminResponseCode].
	AdminResponse(
		clusterSessionID int64,
		correlationID int64,
		requestType AdminRequestType,
		responseCode AdminResponseCode,
		message []byte,
		payload []byte,
	)
}
