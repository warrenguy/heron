package aeron

import (
	"github.com/warrenguy/heron/aeron/conductor"
)

// Subscription manages registration and a set of images for one stream.
//
// Subscription is not thread-safe. Do not call Poll concurrently with
// Client.DoWork on the same client, nor from multiple goroutines. Only one
// goroutine calls Client.DoWork and Subscription.Poll for a given client.
//
// The images slice is mutated only by the conductor via addImage and
// removeImage; application code must not modify it.
type Subscription struct {
	registrationID int64
	streamID       int32
	channel        string

	channelStatusCounterID int32

	images          []*Image
	roundRobinIndex uint32

	isClosed bool

	availableImageHandler   func(*Image)
	unavailableImageHandler func(*Image)

	conductor *conductor.Conductor
}

func newSubscription(
	registrationID int64,
	streamID int32,
	channel string,
	channelStatusCounterID int32,
) *Subscription {
	return &Subscription{
		registrationID:         registrationID,
		streamID:               streamID,
		channel:                channel,
		channelStatusCounterID: channelStatusCounterID,
	}
}

func (subscription *Subscription) RegistrationID() int64 {
	return subscription.registrationID
}

func (subscription *Subscription) StreamID() int32 {
	return subscription.streamID
}

func (subscription *Subscription) Channel() string {
	return subscription.channel
}

func (subscription *Subscription) ImageCount() int {
	return len(subscription.images)
}

// IsConnected reports whether any non-closed image is attached. Returns false
// when the subscription is closed, even if images remain in the slice.
func (subscription *Subscription) IsConnected() bool {
	if subscription.isClosed {
		return false
	}
	for _, image := range subscription.images {
		if image != nil && !image.IsClosed() {
			return true
		}
	}
	return false
}

func (subscription *Subscription) IsClosed() bool {
	return subscription.isClosed
}

func (subscription *Subscription) Poll(handler FragmentHandler, fragmentLimit int) int {
	if subscription.isClosed {
		return 0
	}
	_ = subscription.roundRobinIndex
	_ = subscription.availableImageHandler
	_ = subscription.unavailableImageHandler
	_ = subscription.conductor
	_ = handler
	_ = fragmentLimit
	return 0
}

func (subscription *Subscription) imageIndexByCorrelationID(correlationID int64) int {
	for index, image := range subscription.images {
		if image != nil && image.CorrelationID() == correlationID {
			return index
		}
	}
	return -1
}

func (subscription *Subscription) addImage(image *Image) {
	if image == nil {
		return
	}
	if subscription.imageIndexByCorrelationID(image.CorrelationID()) >= 0 {
		return
	}
	subscription.images = append(subscription.images, image)
}

func (subscription *Subscription) removeImage(correlationID int64) *Image {
	index := subscription.imageIndexByCorrelationID(correlationID)
	if index < 0 {
		return nil
	}
	image := subscription.images[index]
	subscription.images = append(
		subscription.images[:index],
		subscription.images[index+1:]...,
	)
	return image
}

// Close marks the subscription closed. Does not offer RemoveSubscription or
// retire images; conductor client close drain handles driver teardown and
// unavailable-image handling.
func (subscription *Subscription) Close() error {
	subscription.isClosed = true
	return nil
}
