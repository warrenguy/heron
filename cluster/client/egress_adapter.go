package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

// EgressAdapter polls an egress subscription and dispatches cluster SBE events
// to EgressHandler. Not safe for concurrent use.
type EgressAdapter struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *FragmentAssembler
}

// NewEgressAdapter constructs an EgressAdapter. fragmentLimit <= 0 uses
// DefaultEgressFragmentLimit. handler must be non-nil before Poll; use
// &EgressHandlers{} for noop optional callbacks.
func NewEgressAdapter(
	handler EgressHandler,
	clusterSessionID int64,
	subscription *aeron.Subscription,
	fragmentLimit int,
	directAssemblers bool,
) *EgressAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultEgressFragmentLimit
	}
	assembler := NewFragmentAssemblerWithOptions(
		func(
			buffer atomic.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			handleEgressFragment(handler, clusterSessionID, buffer, offset, length, header)
		},
		0,
		directAssemblers,
	)
	return &EgressAdapter{
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		assembler:     assembler,
	}
}

// NewEgressAdapterFromContext constructs an EgressAdapter using DefaultEgressFragmentLimit
// and ctx.DirectAssemblers.
func NewEgressAdapterFromContext(
	ctx ClusterContext,
	handler EgressHandler,
	clusterSessionID int64,
	subscription *aeron.Subscription,
) *EgressAdapter {
	return NewEgressAdapter(handler, clusterSessionID, subscription, 0, ctx.DirectAssemblers)
}

// DirectAssemblers reports the isDirectAssemblers policy stored at construction.
func (adapter *EgressAdapter) DirectAssemblers() bool {
	return adapter.assembler.DirectAssemblers()
}

// FragmentLimit reports the per-poll fragment budget passed to Subscription.Poll.
func (adapter *EgressAdapter) FragmentLimit() int {
	return adapter.fragmentLimit
}

// Poll polls the subscription and returns the fragment count (Java poll()).
// Returns 0 when subscription is nil.
func (adapter *EgressAdapter) Poll() int {
	return pollEgressSubscription(
		adapter.subscription,
		adapter.assembler.Handler(),
		adapter.fragmentLimit,
	)
}
