//go:build testhooks

package client_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testutil"
)

var (
	aeronClusterZeroAllocSink int64
	offerZeroAllocPayload     = []byte("zero-alloc-offer-payload")
	tryClaimZeroAllocPayload  = []byte("try-claim-payload")
)

func TestAeronCluster_Offer_ZeroAllocs(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	testutil.AssertZeroAllocs(t, "AeronCluster_Offer", 1000, func() {
		for {
			position := ac.Offer(offerZeroAllocPayload)
			if position != aeron.AdminAction {
				if position <= 0 {
					panic("offer failed")
				}
				aeronClusterZeroAllocSink = position
				return
			}
		}
	})
}

func TestAeronCluster_TryClaimCommit_ZeroAllocs(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	testutil.AssertZeroAllocs(t, "AeronCluster_TryClaimCommit", 1000, func() {
		for {
			claim, position, err := ac.TryClaim(len(tryClaimZeroAllocPayload))
			if err != nil {
				panic(err)
			}
			if position != aeron.AdminAction {
				if position <= 0 {
					panic("tryClaim failed")
				}
				aeronClusterZeroAllocSink, err = ac.CommitClaim(claim, tryClaimZeroAllocPayload)
				if err != nil {
					panic(err)
				}
				return
			}
		}
	})
}
