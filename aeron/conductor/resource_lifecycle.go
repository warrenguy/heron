package conductor

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// ResourceLifecycle constructs and tears down publications and subscriptions for
// the conductor without importing package aeron.
//
// Internal wiring seam only; the production implementation lives in package
// aeron. The interface may grow without a backward-compat split; out-of-tree
// implementations are unsupported.
//
// New* implementations must not wrap a nil concrete pointer in a returned
// handle (a typed-nil). Production builds cannot distinguish it from a live
// handle; testhooks builds reject it at runtime.
type ResourceLifecycle interface {
	NewPublication(
		event PublicationBuffersReady,
		logBuffers *logbuffer.LogBuffers,
		countersReader *cnc.CountersReader,
		channel string,
	) (PublicationHandle, error)

	NewExclusivePublication(
		event PublicationBuffersReady,
		logBuffers *logbuffer.LogBuffers,
		countersReader *cnc.CountersReader,
		channel string,
	) (PublicationHandle, error)

	NewPreSubscription(
		registrationID int64,
		streamID int32,
		channel string,
	) SubscriptionHandle

	ApplySubscriptionReady(
		sub SubscriptionHandle,
		event SubscriptionReady,
	) error

	// RetireSubscription detaches and closes all images on sub. Returns registry
	// acquire keys recorded at image attach; conductor releases refcounts by key,
	// not via re-resolved driver paths.
	RetireSubscription(sub SubscriptionHandle) ([]string, error)

	// RetirePublication closes pub. Returns the registry acquire key recorded at
	// publication mmap; conductor releases refcounts by key, not via re-resolved
	// driver paths.
	RetirePublication(pub PublicationHandle) (registryKey string, err error)

	// BindPublication wires pub to clientConductor so ClosePublication can run.
	BindPublication(pub PublicationHandle, clientConductor *Conductor) error

	// BindSubscription wires sub to clientConductor so CloseSubscription can run.
	BindSubscription(sub SubscriptionHandle, clientConductor *Conductor) error

	// MarkPublicationClosed marks pub closed on retire failure or idempotent re-close.
	MarkPublicationClosed(pub PublicationHandle)

	// MarkSubscriptionClosed marks sub closed on retire failure or idempotent re-close.
	MarkSubscriptionClosed(sub SubscriptionHandle)
}
