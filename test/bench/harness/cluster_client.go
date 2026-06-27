//go:build bench

package harness

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
)

const (
	// BenchClusterSessionID matches connect fixture cluster session id.
	BenchClusterSessionID int64 = 4242
	// BenchLeadershipTermID matches connect fixture leadership term id.
	BenchLeadershipTermID int64 = 100
	// BenchLeaderMemberID is the wire leader member id on connect fixtures.
	BenchLeaderMemberID int32 = 1
	// BenchChannelOnlyMemberID is the synthetic map key for IPC channel-only ingress.
	BenchChannelOnlyMemberID int32 = 0
)

// NewAeronClusterForBench builds a CONNECTED AeronCluster session backed by an
// in-memory exclusive ingress publication and minimal connect-result stubs.
func NewAeronClusterForBench() *client.AeronCluster {
	ingressPub := aeron.NewInMemExclusivePublicationForBench(
		aeron.DefaultBenchExclusivePublicationConfig(),
		aeron.BenchPublicationLimitPosition,
	)
	result := &client.ConnectResult{
		ClusterSessionID:   BenchClusterSessionID,
		LeadershipTermID:   BenchLeadershipTermID,
		LeaderMemberID:     BenchLeaderMemberID,
		IngressPublication: ingressPub,
		EgressSubscription: &aeron.Subscription{},
		EgressImage:        nil,
		MemberIngressMap: client.MemberIngressMap{
			BenchChannelOnlyMemberID: &client.MemberIngress{
				MemberID:    BenchChannelOnlyMemberID,
				Publication: ingressPub,
			},
		},
		Aeron:           &aeron.Client{},
		OwnsAeronClient: false,
		Context:         client.DefaultBenchClusterContextForBench(),
	}
	ac, err := client.NewAeronCluster(result)
	if err != nil {
		panic("harness: NewAeronClusterForBench: " + err.Error())
	}
	return ac
}

// ResetAeronClusterIngressTerm clears the in-memory exclusive ingress term and
// restarts bench timing around the reset.
func ResetAeronClusterIngressTerm(b *testing.B, ac *client.AeronCluster) {
	resetTermWithTimer(b, func() {
		pub, ok := ac.IngressPublication().(*aeron.ExclusivePublication)
		if !ok || pub == nil {
			panic("harness: AeronCluster ingress publication is not exclusive")
		}
		aeron.ResetInMemExclusivePublicationTermForBench(pub)
	})
}

// Cluster overflow helpers use withAdminActionRetryExcludedFromTimer (inmem.go) for
// AdminAction spin. Divergence from exclusive.go: cluster paths spin on AdminAction;
// exclusive paths reset on any negative return without AdminAction spin.
func aeronClusterOpWithTermOverflowReset(
	b *testing.B,
	ac *client.AeronCluster,
	op func() int64,
) int64 {
	pos := withAdminActionRetryExcludedFromTimer(b, op)
	if pos <= 0 {
		ResetAeronClusterIngressTerm(b, ac)
	}
	return pos
}

func withAdminActionIngressClaimRetryExcludedFromTimer(
	b *testing.B,
	attempt func() (client.IngressClaim, int64, error),
) (client.IngressClaim, int64, error) {
	claim, position, err := attempt()
	if position == aeron.AdminAction {
		b.StopTimer()
		for position == aeron.AdminAction {
			claim, position, err = attempt()
			if err != nil {
				b.StartTimer()
				return claim, position, err
			}
		}
		b.StartTimer()
	}
	return claim, position, err
}

func tryClaimAeronClusterWithAdminActionRetryExcludedFromTimer(
	b *testing.B,
	ac *client.AeronCluster,
	payloadLen int,
) (client.IngressClaim, int64, error) {
	return withAdminActionIngressClaimRetryExcludedFromTimer(
		b,
		func() (client.IngressClaim, int64, error) {
			return ac.TryClaim(payloadLen)
		},
	)
}

// OfferAeronClusterWithTermOverflowReset calls ac.Offer once per invocation.
// AdminAction retry spin is excluded from bench timing; other non-positive returns
// stop the timer, reset the ingress term, and restart the timer.
func OfferAeronClusterWithTermOverflowReset(
	b *testing.B,
	ac *client.AeronCluster,
	payload []byte,
) int64 {
	return aeronClusterOpWithTermOverflowReset(b, ac, func() int64 { return ac.Offer(payload) })
}

// TryClaimCommitAeronClusterWithTermOverflowReset calls ac.TryClaim once per
// invocation. AdminAction retry spin is excluded from bench timing; other failures
// stop the timer, reset the ingress term, and restart the timer. On success it
// commits the claim with payload.
func TryClaimCommitAeronClusterWithTermOverflowReset(
	b *testing.B,
	ac *client.AeronCluster,
	payload []byte,
) (int64, error) {
	claim, position, err := tryClaimAeronClusterWithAdminActionRetryExcludedFromTimer(
		b,
		ac,
		len(payload),
	)
	if err != nil || position <= 0 {
		ResetAeronClusterIngressTerm(b, ac)
		return position, err
	}
	position, err = ac.CommitClaim(claim, payload)
	if err != nil || position <= 0 {
		ResetAeronClusterIngressTerm(b, ac)
	}
	return position, err
}

// SendKeepAliveAeronClusterWithTermOverflowReset calls ac.SendKeepAlive once per
// invocation. AdminAction retry spin is excluded from bench timing; other
// non-positive returns stop the timer, reset the ingress term, and restart the
// timer.
func SendKeepAliveAeronClusterWithTermOverflowReset(
	b *testing.B,
	ac *client.AeronCluster,
) int64 {
	return aeronClusterOpWithTermOverflowReset(b, ac, ac.SendKeepAlive)
}
