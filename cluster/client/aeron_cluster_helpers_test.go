//go:build testhooks

// Connect/AeronCluster fixtures for package client_test; parallel defaults live in
// aeron_cluster_connect_fixtures_test.go (package client).

package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/client"
)

const (
	testClusterSessionID int64 = 4242
	testLeadershipTermID int64 = 100

	// testLeaderMemberID is the wire leader id on connect fixtures. The default
	// MemberIngressMap uses channelOnlyMemberID (0) for IPC channel-only smoke
	// while LeaderMemberID stays 1; multi-member fixtures use matching ids.
	testLeaderMemberID     int32 = 1
	testIngressTimestampNs int64 = 1700000000123
	channelOnlyMemberID    int32 = 0
)

func validTestClusterContext() client.ClusterContext {
	return normalizedClusterContext(func(ctx *client.ClusterContext) {
		ctx.AeronContext = aeron.Context{
			NanoClock: func() int64 { return testIngressTimestampNs },
		}
	})
}

func newTestConnectResult(tb testing.TB, mutate func(*client.ConnectResult)) *client.ConnectResult {
	tb.Helper()

	pub := aeron.NewTestExclusivePublicationForHooks(tb)
	sub := aeron.NewTestSubscriptionForHooks(tb)

	result := &client.ConnectResult{
		ClusterSessionID:   testClusterSessionID,
		LeadershipTermID:   testLeadershipTermID,
		LeaderMemberID:     testLeaderMemberID,
		IngressPublication: pub,
		EgressSubscription: sub,
		MemberIngressMap: client.MemberIngressMap{
			channelOnlyMemberID: &client.MemberIngress{
				MemberID:    channelOnlyMemberID,
				Publication: pub,
			},
		},
		Aeron:           &aeron.Client{},
		OwnsAeronClient: false,
		Context:         validTestClusterContext(),
	}
	if mutate != nil {
		mutate(result)
	}
	return result
}

func newTestAeronCluster(tb testing.TB, mutate func(*client.ConnectResult)) *client.AeronCluster {
	tb.Helper()

	result := newTestConnectResult(tb, mutate)
	ac, err := client.NewAeronCluster(result)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ac.Close() })
	return ac
}

func exclusiveIngressTermBytes(
	tb testing.TB,
	pub *aeron.ExclusivePublication,
	termIndex int,
	frameOffset int,
) []byte {
	tb.Helper()

	termBuf := pub.TermBuffer(termIndex)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameOffset))
	require.Positive(tb, frameLength)
	return termBuf.Bytes()[frameOffset : frameOffset+frameLength]
}
