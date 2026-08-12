//go:build testhooks

package client_test

import (
	"testing"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/internal/testutil"
)

const ingressAdminActionSpinTimeout = 2 * time.Second

var (
	aeronClusterZeroAllocSink int64
	offerZeroAllocPayload     = []byte("zero-alloc-offer-payload")
	tryClaimZeroAllocPayload  = []byte("try-claim-payload")
)

func spinIngressAdminActionUntilSuccess(
	tb testing.TB,
	step func() (position, sink int64),
) {
	tb.Helper()

	// Check the deadline after the attempt so the first step always runs.
	deadline := time.Now().Add(ingressAdminActionSpinTimeout)
	for {
		position, sink := step()
		if position != aeron.AdminAction {
			record := sink
			if sink == 0 {
				record = position
			}
			if record <= 0 {
				tb.Fatal("ingress step failed")
			}
			aeronClusterZeroAllocSink = record
			return
		}
		if time.Now().After(deadline) {
			tb.Fatal("ingress step did not leave AdminAction before deadline")
		}
	}
}

func spinIngressPositionUntilSuccess(tb testing.TB, step func() int64) {
	tb.Helper()
	spinIngressAdminActionUntilSuccess(tb, func() (int64, int64) {
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
		spinIngressPositionUntilSuccess(t, func() int64 { return step(ac) })
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
		spinIngressAdminActionUntilSuccess(t, func() (int64, int64) {
			return tryClaimCommitIngressStep(ac, tryClaimZeroAllocPayload)
		})
	})
}
