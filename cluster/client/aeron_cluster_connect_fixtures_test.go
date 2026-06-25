//go:build testhooks

// Connect/AeronCluster fixtures for package client tests; parallel defaults live in
// aeron_cluster_helpers_test.go (package client_test).

package client

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const (
	connectFixtureLeadershipTermID int64 = 100
	// connectFixtureLeaderMemberID is the wire leader id; default MemberIngressMap
	// uses channelOnlyMemberID (0) for IPC channel-only smoke.
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

func newConnectResultWithEgressSubscription(t *testing.T) *ConnectResult {
	t.Helper()

	return newConnectResultFixture(t, func(result *ConnectResult) {
		result.EgressSubscription = aeron.NewTestSubscriptionForHooks(t)
	})
}

func newAeronClusterFromConnectResult(t *testing.T, result *ConnectResult) *AeronCluster {
	t.Helper()

	ac, err := NewAeronCluster(result)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

func newAeronClusterWithEgressImage(
	t *testing.T,
	sub *aeron.Subscription,
	image *aeron.Image,
	mutate func(*ConnectResult),
) *AeronCluster {
	t.Helper()

	return newAeronClusterFromConnectFixtureResult(
		t,
		newConnectResultWithEgressImage(t, sub, image),
		mutate,
	)
}

func newAeronClusterFromConnectFixture(
	t *testing.T,
	mutate func(*ConnectResult),
) *AeronCluster {
	t.Helper()

	return newAeronClusterFromConnectFixtureResult(
		t,
		newConnectResultWithEgressSubscription(t),
		mutate,
	)
}

func newAeronClusterFromConnectFixtureResult(
	t *testing.T,
	result *ConnectResult,
	mutate func(*ConnectResult),
) *AeronCluster {
	t.Helper()

	installConnectFixtureEgressHandler(result, mutate)
	return newAeronClusterFromConnectResult(t, result)
}

// installConnectFixtureEgressHandler clears the fixture noop handler before mutate so
// callers can install a custom EgressHandler; installs recordingEgressHandlers when
// mutate leaves handler nil.
func installConnectFixtureEgressHandler(result *ConnectResult, mutate func(*ConnectResult)) {
	result.Context.EgressHandler = nil
	if mutate != nil {
		mutate(result)
	}
	if result.Context.EgressHandler == nil {
		result.Context.EgressHandler = newRecordingEgressHandlers()
	}
}

func mutateConnectContext(result *ConnectResult, mutate func(*ClusterContext)) {
	ctx := result.Context
	mutate(&ctx)
	result.Context = ctx
}

func withAtomicNanoClock(result *ConnectResult, nowNs *atomic.Int64) {
	mutateConnectContext(result, func(ctx *ClusterContext) {
		ctx.AeronContext.NanoClock = func() int64 { return nowNs.Load() }
	})
}

func withConnectNanoClock(
	result *ConnectResult,
	nowNs *atomic.Int64,
	mutateCtx func(*ClusterContext),
) {
	mutateConnectContext(result, mutateCtx)
	withAtomicNanoClock(result, nowNs)
}

func newAeronClusterFromConnectFixtureRecording(
	t *testing.T,
	mutate func(*ConnectResult),
) (*AeronCluster, *recordingEgressHandlers) {
	t.Helper()

	result := newConnectResultWithEgressSubscription(t)
	installConnectFixtureEgressHandler(result, mutate)
	rec, ok := result.Context.EgressHandler.(*recordingEgressHandlers)
	require.True(t, ok)
	ac := newAeronClusterFromConnectResult(t, result)
	return ac, rec
}
