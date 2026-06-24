//go:build testhooks

// Connect/AeronCluster fixtures for package client tests; parallel defaults live in
// aeron_cluster_helpers_test.go (package client_test).

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const (
	connectFixtureLeadershipTermID int64 = 100
	connectFixtureLeaderMemberID   int32 = 1
	connectFixtureIngressTimestamp int64 = 1700000000123
)

func validConnectFixtureClusterContext() ClusterContext {
	return ClusterContext{
		IngressChannel:      "aeron:udp?endpoint=localhost:20110",
		EgressChannel:       "aeron:ipc",
		EgressHandler:       &EgressHandlers{},
		CredentialsSupplier: NullCredentialsSupplier{},
		IdleStrategy:        NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return connectFixtureIngressTimestamp },
		},
	}.Normalize()
}

func newConnectResultFixture(
	t *testing.T,
	mutate func(*ConnectResult),
) *ConnectResult {
	t.Helper()

	ingress := aeron.NewTestExclusivePublicationForHooks(t)
	result := &ConnectResult{
		ClusterSessionID:   egressTestClusterSessionID,
		LeadershipTermID:   connectFixtureLeadershipTermID,
		LeaderMemberID:     connectFixtureLeaderMemberID,
		IngressPublication: ingress,
		MemberIngressMap: MemberIngressMap{
			channelOnlyMemberID: &MemberIngress{
				MemberID:    channelOnlyMemberID,
				Publication: ingress,
			},
		},
		Aeron:           &aeron.Client{},
		OwnsAeronClient: false,
		Context:         validConnectFixtureClusterContext(),
	}
	if mutate != nil {
		mutate(result)
	}
	return result
}

func newConnectResultWithEgressImage(
	t *testing.T,
	sub *aeron.Subscription,
	image *aeron.Image,
) *ConnectResult {
	t.Helper()

	return newConnectResultFixture(t, func(result *ConnectResult) {
		result.EgressSubscription = sub
		result.EgressImage = image
	})
}

func newAeronClusterWithEgressImage(
	t *testing.T,
	sub *aeron.Subscription,
	image *aeron.Image,
) *AeronCluster {
	t.Helper()

	result := newConnectResultWithEgressImage(t, sub, image)
	result.Context.EgressHandler = newRecordingEgressHandlers()
	ac, err := NewAeronCluster(result)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}
