package client

import (
	"github.com/warrenguy/heron/aeron"
)

// IngressPublication is the connect-path ingress handle.
//
// Implemented by *aeron.ExclusivePublication (default) and *aeron.Publication.
type IngressPublication interface {
	Offer(buffer []byte) int64
	IsConnected() bool
	Close() error
}

var (
	_ IngressPublication = (*aeron.ExclusivePublication)(nil)
	_ IngressPublication = (*aeron.Publication)(nil)
)

// ConnectResult holds session handles and ids after a successful connect handshake.
//
// On success, Poll transfers ownership of session resources into ConnectResult.
// Discard AsyncConnect without Close; Close is a no-op after success. On failure,
// call AsyncConnect.Close to release partial resources.
type ConnectResult struct {
	ClusterSessionID         int64
	LeadershipTermID         int64
	LeaderMemberID           int32
	LeaderHeartbeatTimeoutNs int64

	IngressPublication IngressPublication
	EgressSubscription *aeron.Subscription
	EgressImage        *aeron.Image

	MemberIngressMap MemberIngressMap

	Aeron           *aeron.Client
	OwnsAeronClient bool
	Context         ClusterContext
}
