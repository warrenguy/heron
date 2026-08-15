// Package service hosts the clustered service container: user callbacks, the
// committed-prefix log adapter, and the service duty-cycle agent.
package service

import (
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

	SessionOpen(sessionID, timestamp int64)
	SessionClose(sessionID, timestamp int64, reason clusterwire.CloseReason)
	SessionMessage(sessionID, timestamp int64, payload []byte)
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
// Behaviour: publication is the snapshot write target and may be nil.
// Implementations must nil-check before offering. A non-nil error marks
// the service failure once and stops the duty cycle. No ServiceAck is
// sent for that action.
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
}
