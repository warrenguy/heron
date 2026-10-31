package conductor

import "math"

// untilUnmapLingerNs keeps a released log buffer mapped until
// UnmapLingeringLogBuffers. A DoWork sweep never reaches its deadline.
const untilUnmapLingerNs = math.MaxInt64

// ClosePublication retires the publication (mark closed, map remove, linger), then
// offers REMOVE_PUBLICATION to the driver.
//
// Idempotent: repeated calls return nil.
//
// When ResourceLifecycle is nil, only map entries are removed; handle retire and
// mark-closed are skipped.
func (conductor *Conductor) ClosePublication(publication PublicationHandle) error {
	conductor.closePublication(publication, conductor.config.ResourceLingerNs)
	return nil
}

func (conductor *Conductor) closePublication(publication PublicationHandle, lingerNs int64) {
	if conductor.retirePublication(publication, lingerNs) {
		conductor.offerRemovePublication(publication.RegistrationID())
	}
}

// retirePublication retires the publication under the resource write lock.
// Does not offer REMOVE_PUBLICATION. A locked-lookup miss marks the handle
// closed. Reports a retire error after the unlock. Returns whether it retired
// a map entry.
func (conductor *Conductor) retirePublication(
	publication PublicationHandle,
	lingerNs int64,
) bool {
	if isAbsentMapValue(publication) {
		return false
	}

	registrationID := publication.RegistrationID()
	result := underResourceWriteLockResult(conductor, func() retireResult {
		pub, ok := conductor.publicationByRegID(registrationID)
		if !ok {
			conductor.markPublicationClosed(publication)
			return retireResult{}
		}
		return retireResult{
			retired: true,
			err:     conductor.retirePublicationLocked(pub, registrationID, lingerNs),
		}
	})
	conductor.reportCloseError(result.err)
	return result.retired
}

// retireResult carries a locked retire outcome past the unlock.
type retireResult struct {
	retired bool
	err     error
}

func (conductor *Conductor) offerDriverCommand(offer func(correlationID int64) error) {
	if conductor.driverProxy == nil {
		return
	}
	correlationID := conductor.driverProxy.NextCorrelationID()
	conductor.reportCloseError(offer(correlationID))
}

func (conductor *Conductor) offerRemovePublication(registrationID int64) {
	conductor.offerDriverCommand(func(correlationID int64) error {
		return conductor.driverProxy.RemovePublication(correlationID, registrationID, false)
	})
}

// retirePublicationLocked requires resourceMu write lock. A retire error
// still marks the handle closed and removes its map entry.
func (conductor *Conductor) retirePublicationLocked(
	publication PublicationHandle,
	registrationID int64,
	lingerNs int64,
) error {
	if conductor.resourceLifecycle == nil {
		conductor.removePublication(registrationID)
		return nil
	}

	registryKey, err := conductor.resourceLifecycle.RetirePublication(publication)
	if err != nil {
		conductor.resourceLifecycle.MarkPublicationClosed(publication)
	}
	conductor.removePublication(registrationID)
	if registryKey != "" {
		conductor.releaseLogBuffersByKey(registryKey, lingerNs)
	}
	return err
}

// CloseSubscription retires the subscription (mark closed, map remove, image
// linger), then offers REMOVE_SUBSCRIPTION to the driver.
//
// Idempotent: repeated calls return nil.
//
// When ResourceLifecycle is nil, only map entries are removed; handle retire and
// mark-closed are skipped.
func (conductor *Conductor) CloseSubscription(subscription SubscriptionHandle) error {
	conductor.closeSubscription(subscription, conductor.config.ResourceLingerNs)
	return nil
}

func (conductor *Conductor) closeSubscription(subscription SubscriptionHandle, lingerNs int64) {
	if conductor.retireSubscription(subscription, lingerNs) {
		conductor.offerRemoveSubscription(subscription.RegistrationID())
	}
}

// retireSubscription retires the subscription under the resource write lock.
// Does not offer REMOVE_SUBSCRIPTION. A locked-lookup miss marks the handle
// closed. Reports a retire error after the unlock. Returns whether it retired
// a map entry.
func (conductor *Conductor) retireSubscription(
	subscription SubscriptionHandle,
	lingerNs int64,
) bool {
	if isAbsentMapValue(subscription) {
		return false
	}

	registrationID := subscription.RegistrationID()
	result := underResourceWriteLockResult(conductor, func() retireResult {
		sub, ok := conductor.subscriptionByRegID(registrationID)
		if !ok {
			conductor.markSubscriptionClosed(subscription)
			return retireResult{}
		}
		return retireResult{
			retired: true,
			err:     conductor.retireSubscriptionLocked(sub, registrationID, lingerNs),
		}
	})
	conductor.reportCloseError(result.err)
	return result.retired
}

func (conductor *Conductor) offerRemoveSubscription(registrationID int64) {
	conductor.offerDriverCommand(func(correlationID int64) error {
		return conductor.driverProxy.RemoveSubscription(correlationID, registrationID)
	})
}

// retireSubscriptionLocked requires resourceMu write lock. A retire error
// still marks the handle closed and removes its map entry.
func (conductor *Conductor) retireSubscriptionLocked(
	subscription SubscriptionHandle,
	registrationID int64,
	lingerNs int64,
) error {
	if conductor.resourceLifecycle == nil {
		conductor.removeSubscription(registrationID)
		return nil
	}

	registryKeys, err := conductor.resourceLifecycle.RetireSubscription(subscription)
	if err != nil {
		conductor.resourceLifecycle.MarkSubscriptionClosed(subscription)
	}
	conductor.removeSubscription(registrationID)
	conductor.releaseLogBuffersByKeys(registryKeys, lingerNs)
	return err
}

// CloseAllResources closes all publications, then all subscriptions.
// Close errors reach ErrorHandler inside HandlerScope. Production callers
// hold clientLock.
//
// Each released log buffer lingers until UnmapLingeringLogBuffers, whatever
// ResourceLingerNs holds. A DoWork sweep never unmaps it.
func (conductor *Conductor) CloseAllResources() {
	entered := conductor.enterHandler()
	defer conductor.exitHandler(entered)

	for _, publication := range conductor.snapshotPublications() {
		conductor.closePublication(publication, untilUnmapLingerNs)
	}
	for _, subscription := range conductor.snapshotSubscriptions() {
		conductor.closeSubscription(subscription, untilUnmapLingerNs)
	}
}

// forceCloseResources retires every publication and subscription. Does not
// offer REMOVE commands to the driver.
//
// A client-timeout terminate finds the driver already removed the resources.
// Other terminate causes leave remains for driver liveness collection.
//
// Concurrency: invoker-only. Must not hold resourceMu. The retire path
// acquires the resource write lock per handle.
//
// Lifetime: idempotent. A later CloseAllResources finds empty maps and no-ops.
func (conductor *Conductor) forceCloseResources() {
	for _, publication := range conductor.snapshotPublications() {
		conductor.retirePublication(publication, conductor.config.ResourceLingerNs)
	}
	for _, subscription := range conductor.snapshotSubscriptions() {
		conductor.retireSubscription(subscription, conductor.config.ResourceLingerNs)
	}
}

func (conductor *Conductor) markPublicationClosed(publication PublicationHandle) {
	if conductor.resourceLifecycle != nil {
		conductor.resourceLifecycle.MarkPublicationClosed(publication)
	}
}

func (conductor *Conductor) markSubscriptionClosed(subscription SubscriptionHandle) {
	if conductor.resourceLifecycle != nil {
		conductor.resourceLifecycle.MarkSubscriptionClosed(subscription)
	}
}

func (conductor *Conductor) snapshotPublications() []PublicationHandle {
	return withResourceRead(conductor, func() []PublicationHandle {
		return snapshotLiveResources(conductor.publications)
	})
}

func (conductor *Conductor) snapshotSubscriptions() []SubscriptionHandle {
	return withResourceRead(conductor, func() []SubscriptionHandle {
		return snapshotLiveResources(conductor.subscriptions)
	})
}
