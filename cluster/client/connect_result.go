package client

import (
	"github.com/warrenguy/heron/aeron"
)

// ConnectResult holds session handles and ids after a successful connect handshake.
//
// On success, Poll transfers ownership of session resources into ConnectResult.
// After NewAeronCluster(result), the session owns IngressPublication,
// EgressSubscription, EgressImage, and (when OwnsAeronClient) Aeron -- do not Close
// those handles on the ConnectResult. NewAeronCluster nils transferred fields on
// the passed-in struct to prevent double-close misuse. MemberIngressMap leader
// Publication pointers alias the session ingress handle; do not Close map entries
// separately after transfer.
//
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
