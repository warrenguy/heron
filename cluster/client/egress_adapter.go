package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
)

// EgressAdapter polls an egress subscription and dispatches cluster SBE events
// to EgressHandler. Not safe for concurrent use.
type EgressAdapter struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *aeron.FragmentAssembler
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
	return newEgressAdapterWithHandler(
		handler,
		clusterSessionID,
		subscription,
		fragmentLimit,
		directAssemblers,
		nil,
	)
}

// NewEgressAdapterFromConfig constructs an EgressAdapter using DefaultEgressFragmentLimit
// and cfg.DirectAssemblers.
func NewEgressAdapterFromConfig(
	cfg Config,
	handler EgressHandler,
	clusterSessionID int64,
	subscription *aeron.Subscription,
) *EgressAdapter {
	return NewEgressAdapter(handler, clusterSessionID, subscription, 0, cfg.DirectAssemblers)
}

// newEgressAdapterWithHandler constructs an EgressAdapter. When onFragment is non-nil
// it replaces the default handleEgressFragment dispatch (session recovery wiring).
func newEgressAdapterWithHandler(
	handler EgressHandler,
	clusterSessionID int64,
	subscription *aeron.Subscription,
	fragmentLimit int,
	directAssemblers bool,
	onFragment aeron.FragmentHandler,
) *EgressAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultEgressFragmentLimit
	}
	if onFragment == nil {
		onFragment = func(
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			handleEgressFragment(handler, clusterSessionID, buffer, offset, length, header)
		}
	}
	assembler := aeron.NewFragmentAssemblerWithOptions(onFragment, 0, directAssemblers)
	return &EgressAdapter{
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		assembler:     assembler,
	}
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

// Clear drops all in-progress egress reassembly state.
func (adapter *EgressAdapter) Clear() {
	if adapter == nil || adapter.assembler == nil {
		return
	}
	recordEgressAdapterOp("Clear")
	adapter.assembler.Clear()
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (adapter *EgressAdapter) FreeSessionBuffer(sessionID int32) bool {
	if adapter == nil || adapter.assembler == nil {
		return false
	}
	recordEgressAdapterOp("FreeSessionBuffer")
	return adapter.assembler.FreeSessionBuffer(sessionID)
}
