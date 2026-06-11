package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	errImageAttachFailed             = errors.New("conductor: failed to attach image")
	errImageCloseFailedBeforeRelease = errors.New(
		"conductor: image close failed before log buffer release",
	)
)

// SubscriptionHandle and ImageHandle break the conductor and aeron import cycle.
type SubscriptionHandle interface {
	RegistrationID() int64
	StreamID() int32
}

type ImageHandle interface {
	CorrelationID() int64
	LogFileName() string
	IsClosed() bool
}

// ImageLifecycle implements attach/detach in package aeron (ConductorImageLifecycle).
type ImageLifecycle interface {
	HasImage(sub SubscriptionHandle, correlationID int64) bool
	// NewImage: event.LogFileName is the registry acquire key; preserve it on the handle.
	NewImage(
		sub SubscriptionHandle,
		event AvailableImage,
		logBuffers *logbuffer.LogBuffers,
		countersReader *cnc.CountersReader,
		errorHandler func(error),
	) (ImageHandle, error)
	AddImage(sub SubscriptionHandle, image ImageHandle) bool
	RemoveImage(sub SubscriptionHandle, correlationID int64) ImageHandle
	CloseImage(image ImageHandle)
	AvailableImageCallback(sub SubscriptionHandle) func(ImageHandle)
	UnavailableImageCallback(sub SubscriptionHandle) func(ImageHandle)
}

func (conductor *Conductor) subscriptionForImageEvent(
	registrationID int64,
) (SubscriptionHandle, bool) {
	if conductor.imageLifecycle == nil {
		return nil, false
	}
	return conductor.subscriptionByRegID(registrationID)
}

func (conductor *Conductor) onAvailableImage(event AvailableImage) {
	sub, ok := conductor.subscriptionForImageEvent(event.SubscriptionRegistrationID)
	if !ok {
		return
	}

	if conductor.imageLifecycle.HasImage(sub, event.CorrelationID) {
		return
	}

	if conductor.config.MediaDriverDir == "" {
		invokeErrorHandler(conductor.config.ErrorHandler, ErrMediaDriverDirRequired)
		return
	}

	resolvedLogFileName, err := conductor.logBuffersRegistry.resolveLogFileKey(
		event.LogFileName,
	)
	if err != nil {
		invokeErrorHandler(conductor.config.ErrorHandler, err)
		return
	}

	logBuffers, err := conductor.acquireLogBuffersByKey(resolvedLogFileName)
	if err != nil {
		invokeErrorHandler(conductor.config.ErrorHandler, err)
		return
	}

	event.LogFileName = resolvedLogFileName
	image, err := conductor.imageLifecycle.NewImage(
		sub,
		event,
		logBuffers,
		conductor.countersReader,
		conductor.config.SubscriberErrorHandler,
	)
	if err != nil {
		conductor.releaseAcquiredLogFile(resolvedLogFileName, err)
		return
	}

	if !conductor.imageLifecycle.AddImage(sub, image) {
		conductor.imageLifecycle.CloseImage(image)
		conductor.releaseAcquiredLogFile(resolvedLogFileName, errImageAttachFailed)
		return
	}

	if callback := conductor.imageLifecycle.AvailableImageCallback(sub); callback != nil {
		conductor.invokeImageCallback(func() {
			callback(image)
		})
	}
}

func (conductor *Conductor) onUnavailableImage(
	correlationID, subscriptionRegistrationID int64,
) {
	sub, ok := conductor.subscriptionForImageEvent(subscriptionRegistrationID)
	if !ok {
		return
	}

	image := conductor.imageLifecycle.RemoveImage(sub, correlationID)
	if image == nil {
		return
	}

	conductor.imageLifecycle.CloseImage(image)
	// lingerNs=0: synchronous unmap when refcount reaches zero.
	if !image.IsClosed() {
		invokeErrorHandler(conductor.config.ErrorHandler, errImageCloseFailedBeforeRelease)
		return
	}
	conductor.releaseLogBuffersByKey(image.LogFileName(), 0)

	if callback := conductor.imageLifecycle.UnavailableImageCallback(sub); callback != nil {
		conductor.invokeImageCallback(func() {
			callback(image)
		})
	}
}

func (conductor *Conductor) releaseAcquiredLogFile(resolvedLogFileName string, err error) {
	conductor.releaseLogBuffersByKey(resolvedLogFileName, 0)
	invokeErrorHandler(conductor.config.ErrorHandler, err)
}

func (conductor *Conductor) invokeImageCallback(callback func()) {
	// Image callbacks suspend the DoWork write lock so PublicationByRegID and
	// SubscriptionByRegID always take RLock and reentrant DoWork can acquire a
	// fresh write lock for map mutations. Handlers must not assume exclusive map
	// access: other goroutines may read or write resource maps during the suspend
	// window. While isInCallback is set, nested user callbacks are skipped; the
	// driver event is still consumed during reentrant DoWork and is not replayed.
	// Java ClientConductor still delivers nested available/unavailable callbacks.
	if conductor.isInCallback {
		return
	}
	conductor.isInCallback = true
	savedDepth := conductor.suspendResourceWriteLock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err, ok := recovered.(error)
			if !ok {
				err = fmt.Errorf("conductor: image callback panic: %v", recovered)
			}
			invokeErrorHandler(conductor.config.ErrorHandler, err)
		}
		conductor.resumeResourceWriteLock(savedDepth)
		conductor.isInCallback = false
	}()
	callback()
}
