//go:build testhooks

package client_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/internal/testutil"
)

var (
	aeronClusterZeroAllocSink int64
	offerZeroAllocPayload     = []byte("zero-alloc-offer-payload")
	tryClaimZeroAllocPayload  = []byte("try-claim-payload")
)

func spinIngressAdminActionUntilSuccess(step func() (position, sink int64)) {
	for {
		position, sink := step()
		if position != aeron.AdminAction {
			record := sink
			if sink == 0 {
				record = position
			}
			if record <= 0 {
				panic("ingress step failed")
			}
			aeronClusterZeroAllocSink = record
			return
		}
	}
}

func spinIngressPositionUntilSuccess(step func() int64) {
	spinIngressAdminActionUntilSuccess(func() (int64, int64) {
		return step(), 0
	})
}

func tryClaimCommitIngressStep(ac *client.AeronCluster, payload []byte) (position, sink int64) {
	claim, position, err := ac.TryClaim(len(payload))
	if err != nil {
		panic(err)
	}
	if position == aeron.AdminAction || position <= 0 {
		return position, 0
	}
	sink, commitErr := ac.CommitClaim(claim, payload)
	if commitErr != nil {
		panic(commitErr)
	}
	return sink, 0
}

func testAeronClusterIngressPositionZeroAllocs(
	t *testing.T,
	label string,
	step func(*client.AeronCluster) int64,
) {
	t.Helper()
	ac := newTestAeronCluster(t, nil)
	testutil.AssertZeroAllocs(t, label, 1000, func() {
		spinIngressPositionUntilSuccess(func() int64 { return step(ac) })
	})
}

func TestAeronCluster_Offer_ZeroAllocs(t *testing.T) {
	testAeronClusterIngressPositionZeroAllocs(
		t,
		"AeronCluster_Offer",
		func(ac *client.AeronCluster) int64 {
			return ac.Offer(offerZeroAllocPayload)
		},
	)
}

func TestAeronCluster_SendKeepAlive_ZeroAllocs(t *testing.T) {
	testAeronClusterIngressPositionZeroAllocs(
		t,
		"AeronCluster_SendKeepAlive",
		func(ac *client.AeronCluster) int64 {
			return ac.SendKeepAlive()
		},
	)
}

func TestAeronCluster_SendSnapshotRequest_ZeroAllocs(t *testing.T) {
	testAeronClusterIngressPositionZeroAllocs(
		t,
		"AeronCluster_SendSnapshotRequest",
		func(ac *client.AeronCluster) int64 {
			return ac.SendSnapshotRequest(testAdminCorrelationID)
		},
	)
}

func TestAeronCluster_TryClaimCommit_ZeroAllocs(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	testutil.AssertZeroAllocs(t, "AeronCluster_TryClaimCommit", 1000, func() {
		spinIngressAdminActionUntilSuccess(func() (int64, int64) {
			return tryClaimCommitIngressStep(ac, tryClaimZeroAllocPayload)
		})
	})
}
