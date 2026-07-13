package conductor

// ClosePublication retires the publication (mark closed, map remove, linger), then
// offers REMOVE_PUBLICATION to the driver.
//
// Idempotent: repeated calls return nil.
//
// When ResourceLifecycle is nil, only map entries are removed; handle retire and
// mark-closed are skipped.
func (conductor *Conductor) ClosePublication(publication PublicationHandle) error {
	if isAbsentMapValue(publication) {
		return nil
	}

	registrationID := publication.RegistrationID()
	if _, ok := conductor.PublicationByRegID(registrationID); !ok {
		if conductor.resourceLifecycle != nil {
			conductor.resourceLifecycle.MarkPublicationClosed(publication)
		}
		return nil
	}

	var offerRemove bool
	conductor.underResourceWriteLock(func() {
		pub, ok := conductor.publicationByRegID(registrationID)
		if !ok {
			return
		}
		offerRemove = conductor.retirePublicationLocked(pub, registrationID)
	})
	if offerRemove {
		conductor.offerRemovePublication(registrationID)
	}
	return nil
}

func (conductor *Conductor) offerDriverCommand(offer func(correlationID int64) error) {
	if conductor.driverProxy == nil {
		return
	}
	correlationID := conductor.driverProxy.NextCorrelationID()
	if err := offer(correlationID); err != nil {
		invokeErrorHandler(conductor.config.ErrorHandler, err)
	}
}

func (conductor *Conductor) offerRemovePublication(registrationID int64) {
	conductor.offerDriverCommand(func(correlationID int64) error {
		return conductor.driverProxy.RemovePublication(correlationID, registrationID, false)
	})
}

func (conductor *Conductor) retirePublicationLocked(
	publication PublicationHandle,
	registrationID int64,
) bool {
	if conductor.resourceLifecycle == nil {
		conductor.removePublication(registrationID)
		return true
	}

	registryKey, err := conductor.resourceLifecycle.RetirePublication(publication)
	if err != nil {
		invokeErrorHandler(conductor.config.ErrorHandler, err)
		conductor.resourceLifecycle.MarkPublicationClosed(publication)
	}
	conductor.removePublication(registrationID)
	if registryKey != "" {
		conductor.releaseLogBuffersByKey(registryKey, conductor.config.CloseLingerNs)
	}
	return true
}

// CloseSubscription retires the subscription (mark closed, map remove, image
// linger), then offers REMOVE_SUBSCRIPTION to the driver.
//
// Idempotent: repeated calls return nil.
//
// When ResourceLifecycle is nil, only map entries are removed; handle retire and
// mark-closed are skipped.
func (conductor *Conductor) CloseSubscription(subscription SubscriptionHandle) error {
	if isAbsentMapValue(subscription) {
		return nil
	}

	registrationID := subscription.RegistrationID()
	if _, ok := conductor.SubscriptionByRegID(registrationID); !ok {
		if conductor.resourceLifecycle != nil {
			conductor.resourceLifecycle.MarkSubscriptionClosed(subscription)
		}
		return nil
	}

	var offerRemove bool
	conductor.underResourceWriteLock(func() {
		sub, ok := conductor.subscriptionByRegID(registrationID)
		if !ok {
			return
		}
		offerRemove = conductor.retireSubscriptionLocked(sub, registrationID)
	})
	if offerRemove {
		conductor.offerRemoveSubscription(registrationID)
	}
	return nil
}

func (conductor *Conductor) offerRemoveSubscription(registrationID int64) {
	conductor.offerDriverCommand(func(correlationID int64) error {
		return conductor.driverProxy.RemoveSubscription(correlationID, registrationID)
	})
}

func (conductor *Conductor) retireSubscriptionLocked(
	subscription SubscriptionHandle,
	registrationID int64,
) bool {
	if conductor.resourceLifecycle == nil {
		conductor.removeSubscription(registrationID)
		return true
	}

	registryKeys, err := conductor.resourceLifecycle.RetireSubscription(subscription)
	if err != nil {
		invokeErrorHandler(conductor.config.ErrorHandler, err)
		conductor.resourceLifecycle.MarkSubscriptionClosed(subscription)
	}
	conductor.removeSubscription(registrationID)
	conductor.releaseLogBuffersByKeys(registryKeys, conductor.config.CloseLingerNs)
	return true
}

// CloseAllResources closes all publications, then all subscriptions.
func (conductor *Conductor) CloseAllResources() {
	for _, publication := range conductor.snapshotPublications() {
		_ = conductor.ClosePublication(publication)
	}
	for _, subscription := range conductor.snapshotSubscriptions() {
		_ = conductor.CloseSubscription(subscription)
	}
}

// CloseLingerNs returns configured log-buffer linger after resource close.
func (conductor *Conductor) CloseLingerNs() int64 {
	return conductor.config.CloseLingerNs
}

// HasScheduledLinger reports whether any log buffer awaits linger sweep.
func (conductor *Conductor) HasScheduledLinger() bool {
	if conductor == nil || conductor.logBuffersRegistry == nil {
		return false
	}
	return conductor.logBuffersRegistry.HasScheduledLinger()
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
