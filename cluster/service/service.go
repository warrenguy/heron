// Package service hosts the clustered service container: user callbacks, the
// committed-prefix log adapter, and the service duty-cycle agent.
package service

import (
	"iter"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// SubscriptionAdder registers subscriptions on a media driver.
//
// *aeron.Client satisfies this interface as-built.
type SubscriptionAdder interface {
	AddSubscription(streamID int32, channel string) (*aeron.Subscription, error)
}

var _ SubscriptionAdder = (*aeron.Client)(nil)

// ClusteredService is the deterministic user state machine fed from the
// committed cluster log.
//
// Behaviour: no wall clock. Callbacks carry cluster time. Payload slices are
// valid only for the callback. Implementations must copy what they retain.
//
// Concurrency: invoker-only. Every method runs on the Agent.DoWork goroutine.
type ClusteredService interface {
	// Start runs once, before any log event. A non-nil error marks the sticky
	// service failure and stops the duty cycle.
	Start(cluster Cluster) error

	// SessionOpen runs after the agent adds session to the registry.
	//
	// Behaviour: on the leader, session.Offer inside SessionOpen returns
	// aeron.NotConnected. The response publication add has not completed.
	SessionOpen(session ClientSession, timestamp int64)

	// SessionClose runs after the agent removes session from the registry.
	// The agent also closes the response publication, or cancels its add,
	// before SessionClose runs.
	//
	// Behaviour: on the leader, session.Offer returns aeron.NotConnected.
	SessionClose(session ClientSession, timestamp int64, reason clusterwire.CloseReason)

	// SessionMessage delivers one session message payload.
	//
	// Behaviour: session is nil when the message session id is not open.
	// Service messages arrive with a nil session. A Java module appends
	// them under a service session id, which never names a client session.
	SessionMessage(session ClientSession, timestamp int64, payload []byte)

	TimerEvent(correlationID, timestamp int64)
}

// NewLeadershipTermEventHandler is the optional extension a service implements
// to observe leadership term events (upstream default-method parity).
// NewAgent detects it once with a type assertion.
type NewLeadershipTermEventHandler interface {
	NewLeadershipTermEvent(
		leadershipTermID int64,
		logPosition int64,
		timestamp int64,
		termBaseLogPosition int64,
		leaderMemberID int32,
		logSessionID int32,
		timeUnit clusterwire.ClusterTimeUnit,
		appVersion int32,
	)
}

// TakeSnapshotHandler is the optional extension a service implements to
// write snapshot state when the cluster requests a snapshot action.
// NewAgent detects the handler once with a type assertion.
//
// Behaviour: the agent records publication in the member's Archive. The
// agent writes the begin marker, one row per open session and the end
// marker. TakeSnapshot then offers the service state. The agent closes
// publication after TakeSnapshot returns.
//
// publication is never nil. TakeSnapshot must retry BackPressured and
// AdminAction offer codes. One Offer must not exceed
// publication.MaxMessageLength. TakeSnapshot must not close publication.
//
// A non-nil error fails the take. The agent queues one ServiceAck with
// RelevantID -1. The service failure marks once that ack sends. The mark
// stops the duty cycle. With no bound ModuleProxy, the agent marks at once
// and never sends that ack. An earlier mark or a terminal ack publish error
// also stops the duty cycle before that ack.
//
// Concurrency: invoker-only. TakeSnapshot blocks the duty cycle. Nothing
// pumps the transport client meanwhile. A run past the transport client's
// InterServiceTimeout terminates that client.
type TakeSnapshotHandler interface {
	TakeSnapshot(publication *aeron.ExclusivePublication) error
}

// Cluster is the service-facing proxy into member state.
//
// Concurrency: invoker-only. Call from log-event callbacks and DoWork only.
type Cluster interface {
	LogPosition() int64
	ClusterTime() int64
	MemberID() int32
	Role() codecs.Role

	// ScheduleTimer requests one cluster timer for correlationID at deadline
	// (cluster time, nanoseconds). An existing timer for the same correlationID
	// is rescheduled. Reschedule and cancel race with expiry. A TimerEvent
	// already appended still arrives. Expiry is leader-only.
	//
	// Call from log-event callbacks.
	//
	// Returns:
	//   - true when the request frame was sent
	//   - false when the proxy is absent or the publication is unbound.
	//     Retry from a later callback.
	//   - false when the offer returned a retriable code. Retry from a
	//     later callback.
	//   - false when a terminal publication code marks the service failure
	//     and stops the duty cycle. Must not retry.
	ScheduleTimer(correlationID, deadline int64) bool

	// CancelTimer requests cancellation of the timer for correlationID.
	// Cancel races with expiry. A TimerEvent already appended still arrives.
	//
	// Call from log-event callbacks.
	//
	// Returns:
	//   - true when the request frame was sent
	//   - false when the proxy is absent or the publication is unbound.
	//     Retry from a later callback.
	//   - false when the offer returned a retriable code. Retry from a
	//     later callback.
	//   - false when a terminal publication code marks the service failure
	//     and stops the duty cycle. Must not retry.
	CancelTimer(correlationID int64) bool

	// ClientSession returns the open session with id, or nil and false when
	// id is not open.
	ClientSession(id int64) (ClientSession, bool)

	// ClientSessions returns the open sessions in ascending id order.
	//
	// Behaviour: each walk reads the live registry. The registry does not
	// change during a callback.
	//
	// Allocation: the call allocates nothing. A range loop allocates its
	// yield closure per walk. A stored yield func passed to the sequence
	// allocates nothing.
	ClientSessions() iter.Seq[ClientSession]
}
