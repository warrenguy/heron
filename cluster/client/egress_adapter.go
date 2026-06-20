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
// &EgressHandlers{} for noop optional callbacks. subscription must be non-nil before Poll.
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
	delegate := func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		handleEgressFragment(handler, clusterSessionID, buffer, offset, length, header)
	}
	assembler := NewFragmentAssemblerWithOptions(delegate, 0, directAssemblers)
	return &EgressAdapter{
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		assembler:     assembler,
	}
}

// NewEgressAdapterFromContext constructs an EgressAdapter using DefaultEgressFragmentLimit
// and ctx.DirectAssemblers. subscription must be non-nil before Poll.
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
func (adapter *EgressAdapter) Poll() int {
	return adapter.subscription.Poll(adapter.assembler.Handler(), adapter.fragmentLimit)
}
