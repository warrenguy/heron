package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

type ControlledEgressAction = ringbuffer.ControlledAction

const (
	ControlledEgressAbort    = ringbuffer.ControlledAbort
	ControlledEgressBreak    = ringbuffer.ControlledBreak
	ControlledEgressCommit   = ringbuffer.ControlledCommit
	ControlledEgressContinue = ringbuffer.ControlledContinue
)

// ControlledEgressHandler mirrors Java ControlledEgressListener @ 1.51.0.
// Only Message returns a controlled action; other callbacks always commit.
// Buffer views are valid only for the callback duration; do not retain slices
// or the Header pointer.
type ControlledEgressHandler interface {
	// Message delivers an application payload after SessionMessageHeader strip.
	Message(
		clusterSessionID int64,
		timestamp int64,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) ControlledEgressAction

	// SessionEvent delivers template 2 (SessionEvent). detail is raw ASCII bytes.
	SessionEvent(
		correlationID int64,
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		code EventCode,
		detail []byte,
	)

	// NewLeader delivers template 6 (NewLeaderEvent).
	// ingressEndpoints is raw ASCII bytes (e.g. "0=localhost:20110,...").
	NewLeader(
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		ingressEndpoints []byte,
	)

	// AdminResponse delivers template 27 (AdminResponse).
	AdminResponse(
		clusterSessionID int64,
		correlationID int64,
		requestType AdminRequestType,
		responseCode AdminResponseCode,
		message []byte,
		payload []byte,
	)
}
