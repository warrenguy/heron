package conductor

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// ResourceLifecycle implements publication and subscription construction in package
// aeron (ConductorResourceLifecycle). Conductor calls these from ready handlers and
// subscription pre-insert without importing aeron.
type ResourceLifecycle interface {
	NewPublication(
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
}
