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

// Cluster is the service-facing proxy into member state.
//
// Concurrency: invoker-only; read from callbacks and DoWork only.
type Cluster interface {
	LogPosition() int64
	ClusterTime() int64
	MemberID() int32
	Role() codecs.Role
}
