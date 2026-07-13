package conductor

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// PublicationHandle breaks the conductor and aeron import cycle for the
// publications resource map.
type PublicationHandle interface {
	RegistrationID() int64
}

func isAbsentMapValue[T any](value T) bool {
	return any(value) == nil || isTypedNilHandle(value)
}

func resourceByRegID[T any](resources map[int64]T, registrationID int64) (T, bool) {
	resource, ok := resources[registrationID]
	if !ok || isAbsentMapValue(resource) {
		var zero T
		return zero, false
	}
	return resource, true
}

func liveResourceCount[T any](resources map[int64]T) int {
	count := 0
	for _, resource := range resources {
		if !isAbsentMapValue(resource) {
			count++
		}
	}
	return count
}

func snapshotLiveResources[T any](resources map[int64]T) []T {
	snapshot := make([]T, 0, len(resources))
	for _, resource := range resources {
		if isAbsentMapValue(resource) {
			continue
		}
		snapshot = append(snapshot, resource)
	}
	return snapshot
}

func insertResource[T any](resources map[int64]T, registrationID int64, resource T) {
	if isAbsentMapValue(resource) {
		return
	}
	resources[registrationID] = resource
}

func withResourceRead[T any](conductor *Conductor, work func() T) T {
	conductor.resourceMu.RLock()
	defer conductor.resourceMu.RUnlock()
	return work()
}

func withResourceLookup[T any](conductor *Conductor, work func() (T, bool)) (T, bool) {
	conductor.resourceMu.RLock()
	defer conductor.resourceMu.RUnlock()
	return work()
}

// PublicationByRegID returns a publication handle under RLock.
//
// Do not call from the agent goroutine during resourceMu write-lock scopes (deadlocks on RLock);
// agent handlers use publicationByRegID instead.
func (conductor *Conductor) PublicationByRegID(
	registrationID int64,
) (PublicationHandle, bool) {
	return withResourceLookup(conductor, func() (PublicationHandle, bool) {
		return resourceByRegID(conductor.publications, registrationID)
	})
}

// publicationByRegID looks up under resourceMu write lock.
//
// Agent handlers must call this, not PublicationByRegID.
func (conductor *Conductor) publicationByRegID(
	registrationID int64,
) (PublicationHandle, bool) {
	return resourceByRegID(conductor.publications, registrationID)
}

// SubscriptionByRegID returns a subscription handle under RLock.
//
// Do not call from the agent goroutine during resourceMu write-lock scopes (deadlocks on RLock);
// agent handlers use subscriptionByRegID instead.
func (conductor *Conductor) SubscriptionByRegID(
	registrationID int64,
) (SubscriptionHandle, bool) {
	return withResourceLookup(conductor, func() (SubscriptionHandle, bool) {
		return resourceByRegID(conductor.subscriptions, registrationID)
	})
}

// subscriptionByRegID looks up under resourceMu write lock.
//
// Agent handlers must call this, not SubscriptionByRegID.
func (conductor *Conductor) subscriptionByRegID(
	registrationID int64,
) (SubscriptionHandle, bool) {
	return resourceByRegID(conductor.subscriptions, registrationID)
}

// insertPublication overwrites an existing registrationID entry.
//
// Caller must hold resourceMu write lock.
//
// Nil handles are ignored; map lookups treat absent entries as a miss.
func (conductor *Conductor) insertPublication(
	registrationID int64,
	publication PublicationHandle,
) {
	insertResource(conductor.publications, registrationID, publication)
}

// removePublication requires resourceMu write lock.
func (conductor *Conductor) removePublication(registrationID int64) {
	delete(conductor.publications, registrationID)
}

// insertSubscription overwrites an existing registrationID entry.
//
// Caller must hold resourceMu write lock.
//
// Nil handles are ignored; map lookups treat absent entries as a miss.
func (conductor *Conductor) insertSubscription(
	registrationID int64,
	subscription SubscriptionHandle,
) {
	insertResource(conductor.subscriptions, registrationID, subscription)
}

// removeSubscription requires resourceMu write lock.
func (conductor *Conductor) removeSubscription(registrationID int64) {
	delete(conductor.subscriptions, registrationID)
}

// acquireLogBuffers requires resourceMu write lock.
func (conductor *Conductor) acquireLogBuffers(
	logFileName string,
) (*logbuffer.LogBuffers, error) {
	return conductor.logBuffersRegistry.Acquire(logFileName)
}

// acquireLogBuffersByKey requires resourceMu write lock.
func (conductor *Conductor) acquireLogBuffersByKey(
	key string,
) (*logbuffer.LogBuffers, error) {
	return conductor.logBuffersRegistry.acquireByKey(key)
}

func lingerReleaseNowNs(conductor *Conductor, lingerNs int64) int64 {
	if lingerNs <= 0 {
		return 0
	}
	return conductor.config.NanoClock()
}

// releaseLogBuffers requires resourceMu write lock.
func (conductor *Conductor) releaseLogBuffers(logFileName string, lingerNs int64) {
	nowNs := lingerReleaseNowNs(conductor, lingerNs)
	conductor.logBuffersRegistry.releaseAt(logFileName, nowNs, lingerNs)
}

// releaseLogBuffersByKey requires resourceMu write lock.
func (conductor *Conductor) releaseLogBuffersByKey(key string, lingerNs int64) {
	nowNs := lingerReleaseNowNs(conductor, lingerNs)
	conductor.logBuffersRegistry.releaseResolvedKeyAt(key, nowNs, lingerNs)
}

// releaseLogBuffersByKeys requires resourceMu write lock.
func (conductor *Conductor) releaseLogBuffersByKeys(keys []string, lingerNs int64) {
	if len(keys) == 0 {
		return
	}
	nowNs := lingerReleaseNowNs(conductor, lingerNs)
	for _, key := range keys {
		conductor.logBuffersRegistry.releaseResolvedKeyAt(key, nowNs, lingerNs)
	}
}

func (conductor *Conductor) sweepLingeringLogBuffers(nowNs int64) int {
	return conductor.logBuffersRegistry.SweepLingering(nowNs)
}

// DrainLingeringLogBuffers is the close-path hook for a final linger sweep
// before CLIENT_CLOSE.
//
// Safe from any goroutine once the agent quiesces.
//
// During a suspended image handler the mutex is unlocked, so a foreign goroutine may
// acquire the write lock concurrently with the blocked handler.
func (conductor *Conductor) DrainLingeringLogBuffers() int {
	if conductor.logBuffersRegistry == nil ||
		!conductor.logBuffersRegistry.HasScheduledLinger() {
		return 0
	}
	return underResourceWriteLockResult(conductor, func() int {
		return conductor.sweepLingeringLogBuffers(conductor.config.NanoClock())
	})
}

func (conductor *Conductor) publicationCount() int {
	return liveResourceCount(conductor.publications)
}

func (conductor *Conductor) subscriptionCount() int {
	return liveResourceCount(conductor.subscriptions)
}
