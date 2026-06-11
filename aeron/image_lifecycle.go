package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type conductorImageLifecycle struct{}

// ConductorImageLifecycle wires conductor image events to aeron types.
var ConductorImageLifecycle conductor.ImageLifecycle = conductorImageLifecycle{}

func (conductorImageLifecycle) HasImage(
	sub conductor.SubscriptionHandle,
	correlationID int64,
) bool {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return false
	}
	return subscription.imageIndexByCorrelationID(correlationID) >= 0
}

func (conductorImageLifecycle) NewImage(
	sub conductor.SubscriptionHandle,
	event conductor.AvailableImage,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	errorHandler func(error),
) (conductor.ImageHandle, error) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil, err
	}
	return newImage(subscription, event, logBuffers, countersReader, errorHandler)
}

func (conductorImageLifecycle) AddImage(
	sub conductor.SubscriptionHandle,
	image conductor.ImageHandle,
) bool {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return false
	}
	img, ok := imageFromHandle(image)
	if !ok {
		return false
	}
	return subscription.addImage(img)
}

func (conductorImageLifecycle) RemoveImage(
	sub conductor.SubscriptionHandle,
	correlationID int64,
) conductor.ImageHandle {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil
	}
	image := subscription.removeImage(correlationID)
	if image == nil {
		return nil
	}
	return image
}

func (conductorImageLifecycle) CloseImage(image conductor.ImageHandle) {
	img, ok := imageFromHandle(image)
	if !ok {
		return
	}
	img.internalClose()
}

func (conductorImageLifecycle) AvailableImageHandler(
	sub conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return subscriptionImageHandler(sub, func(subscription *Subscription) func(*Image) {
		return subscription.availableImageHandler
	})
}

func (conductorImageLifecycle) UnavailableImageHandler(
	sub conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return subscriptionImageHandler(sub, func(subscription *Subscription) func(*Image) {
		return subscription.unavailableImageHandler
	})
}

func subscriptionImageHandler(
	sub conductor.SubscriptionHandle,
	handler func(*Subscription) func(*Image),
) func(conductor.ImageHandle) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil
	}
	return wrapImageHandler(handler(subscription))
}

func subscriptionFromHandle(sub conductor.SubscriptionHandle) (*Subscription, error) {
	if sub == nil {
		return nil, errors.New("aeron: subscription handle is nil")
	}
	subscription, ok := sub.(*Subscription)
	if !ok {
		return nil, fmt.Errorf("aeron: subscription handle %T is not *Subscription", sub)
	}
	return subscription, nil
}

func publicationFromHandle(pub conductor.PublicationHandle) (*Publication, error) {
	if pub == nil {
		return nil, errors.New("aeron: publication handle is nil")
	}
	publication, ok := pub.(*Publication)
	if !ok {
		return nil, fmt.Errorf("aeron: publication handle %T is not *Publication", pub)
	}
	return publication, nil
}

func imageFromHandle(image conductor.ImageHandle) (*Image, bool) {
	if image == nil {
		return nil, false
	}
	img, ok := image.(*Image)
	if !ok || img == nil {
		return nil, false
	}
	return img, true
}

func wrapImageHandler(handler func(*Image)) func(conductor.ImageHandle) {
	if handler == nil {
		return nil
	}
	return func(image conductor.ImageHandle) {
		img, ok := imageFromHandle(image)
		if !ok {
			return
		}
		handler(img)
	}
}
