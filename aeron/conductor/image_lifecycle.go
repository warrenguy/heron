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
	AvailableImageHandler(sub SubscriptionHandle) func(ImageHandle)
	UnavailableImageHandler(sub SubscriptionHandle) func(ImageHandle)
}

func (conductor *Conductor) subscriptionForImageEvent(
	registrationID int64,
) (SubscriptionHandle, bool) {
	if conductor.imageLifecycle == nil {
		return nil, false
	}
	return conductor.subscriptionByRegID(registrationID)
}

func (conductor *Conductor) handleAvailableImage(event AvailableImage) {
	conductor.underResourceWriteLock(func() {
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

		resolvedLogFileKey, err := conductor.logBuffersRegistry.resolveLogFileKey(event.LogFileName)
		if err != nil {
			invokeErrorHandler(conductor.config.ErrorHandler, err)
			return
		}

		logBuffers, err := conductor.acquireLogBuffersByKey(resolvedLogFileKey)
		if err != nil {
			invokeErrorHandler(conductor.config.ErrorHandler, err)
			return
		}

		event.LogFileName = resolvedLogFileKey
		image, err := conductor.imageLifecycle.NewImage(
			sub,
			event,
			logBuffers,
			conductor.countersReader,
			conductor.config.SubscriberErrorHandler,
		)
		if err != nil {
			conductor.releaseAcquiredLogBuffers(resolvedLogFileKey, err)
			return
		}

		if !conductor.imageLifecycle.AddImage(sub, image) {
			conductor.imageLifecycle.CloseImage(image)
			conductor.releaseAcquiredLogBuffers(resolvedLogFileKey, errImageAttachFailed)
			return
		}

		if handler := conductor.imageLifecycle.AvailableImageHandler(sub); handler != nil {
			conductor.invokeImageHandler(func() {
				handler(image)
			})
		}
	})
}

func (conductor *Conductor) handleUnavailableImage(
	correlationID, subscriptionRegistrationID int64,
) {
	conductor.underResourceWriteLock(func() {
		sub, ok := conductor.subscriptionForImageEvent(subscriptionRegistrationID)
		if !ok {
			return
		}

		image := conductor.imageLifecycle.RemoveImage(sub, correlationID)
		if image == nil {
			return
		}

		conductor.imageLifecycle.CloseImage(image)
		if !image.IsClosed() {
			invokeErrorHandler(conductor.config.ErrorHandler, errImageCloseFailedBeforeRelease)
			return
		}
		registryKey := image.LogFileName()
		conductor.releaseLogBuffersByKey(registryKey, conductor.config.CloseLingerNs)

		if handler := conductor.imageLifecycle.UnavailableImageHandler(sub); handler != nil {
			conductor.invokeImageHandler(func() {
				handler(image)
			})
		}
	})
}

func (conductor *Conductor) releaseAcquiredLogBuffers(registryKey string, err error) {
	conductor.releaseLogBuffersByKey(registryKey, 0)
	invokeErrorHandler(conductor.config.ErrorHandler, err)
}

func (conductor *Conductor) invokeImageHandler(handler func()) {
	// Image handlers suspend the mutation-scope write lock so PublicationByRegID
	// and SubscriptionByRegID always take RLock and reentrant DoWork can acquire
	// a fresh write lock for map mutations. Handlers must not assume exclusive map
	// access: other goroutines may read or write resource maps during the suspend
	// window. While inCallback is set, nested user handlers are skipped; the
	// driver event is still consumed during reentrant DoWork and is not replayed.
	// Java ClientConductor still delivers nested available/unavailable handlers.
	if conductor.inCallback {
		return
	}
	conductor.inCallback = true
	savedDepth := conductor.suspendResourceWriteLock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err, ok := recovered.(error)
			if !ok {
				err = fmt.Errorf("conductor: image handler panic: %v", recovered)
			}
			invokeErrorHandler(conductor.config.ErrorHandler, err)
		}
		conductor.resumeResourceWriteLock(savedDepth)
		conductor.inCallback = false
	}()
	handler()
}
