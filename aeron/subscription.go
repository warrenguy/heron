package aeron

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
)

// Subscription manages registration and a set of images for one stream.
//
// Subscription is not thread-safe. Do not call Poll concurrently with
// Client.DoWork on the same client, nor from multiple goroutines. Only one
// goroutine calls Client.DoWork and Subscription.Poll for a given client.
// Image isClosed uses atomic.Bool for visibility within Poll; subscription
// closed is a plain bool; cross-goroutine close visibility for Image only
// is guaranteed under the single-thread contract above.
//
// The images slice is mutated only by the conductor via addImage and
// removeImage; application code must not modify it.
type Subscription struct {
	registrationID int64
	streamID       int32
	channel        string

	channelStatusCounterID int32
	counters               *cnc.CountersReader

	images []*Image
	// roundRobinIndex advances each Poll for fairness. Java Subscription stores
	// this as int; stored as uint32 here and normalized against len(images) every Poll.
	roundRobinIndex uint32

	closed bool

	availableImageHandler   func(*Image)
	unavailableImageHandler func(*Image)
	conductor               *conductor.Conductor
}

func newSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) *Subscription {
	return &Subscription{
		registrationID:         registrationID,
		streamID:               streamID,
		channel:                channel,
		channelStatusCounterID: cnc.NullCounterID,
	}
}

func (subscription *Subscription) applySubscriptionReady(channelStatusCounterID int32) {
	subscription.channelStatusCounterID = channelStatusCounterID
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

// ImageAt returns the attached image at index when present and not closed.
// Slice indices are not stable across Client.DoWork; poll or copy fields before the next DoWork.
func (subscription *Subscription) ImageAt(index int) (*Image, bool) {
	if subscription.closed {
		return nil, false
	}
	if uint(index) >= uint(len(subscription.images)) {
		return nil, false
	}
	image := subscription.images[index]
	if image == nil || image.IsClosed() {
		return nil, false
	}
	return image, true
}

// IsConnected reports whether any non-closed image is attached.
//
// Returns false when the subscription is closed, even if images remain in the slice.
func (subscription *Subscription) IsConnected() bool {
	if subscription.closed {
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
	return subscription.closed
}

// Poll delivers fragments from attached images using round-robin fairness across
// a snapshot of the images slice taken at call entry. Images added during the
// call are excluded; images removed during the call may still be polled from the
// snapshot. Nil entries in the snapshot are skipped without calling Image.Poll.
//
// The fragmentLimit budget is shared across images in rotation order.
//
// Returns 0 when the subscription is closed, without advancing roundRobinIndex.
// A normal bound Close runs RetireSubscription, which nils images and
// internalClose's each image immediately; any mmap linger is deferred at the
// registry layer, not by retaining the images slice. An in-flight Poll keeps
// only its entry snapshot and is not aborted by a reentrant Close() from a
// fragment handler.
//
// Subscription is not thread-safe; see type documentation.
func (subscription *Subscription) Poll(handler FragmentHandler, fragmentLimit int) int {
	if subscription.closed {
		// Closed: no fragments and no round-robin advance.
		return 0
	}

	images := subscription.images
	length := len(images)
	if length == 0 {
		subscription.roundRobinIndex++
		return 0
	}
	startingIndex := subscription.roundRobinIndex % uint32(length)
	subscription.roundRobinIndex = (startingIndex + 1) % uint32(length)

	fragmentsRead := 0
	for offset := range length {
		if fragmentsRead >= fragmentLimit {
			break
		}
		image := images[(int(startingIndex)+offset)%length]
		if image == nil {
			continue
		}
		fragmentsRead += image.Poll(handler, fragmentLimit-fragmentsRead)
	}

	return fragmentsRead
}

func (subscription *Subscription) imageIndexByCorrelationID(correlationID int64) int {
	for index, image := range subscription.images {
		if image != nil && image.CorrelationID() == correlationID {
			return index
		}
	}
	return -1
}

func (subscription *Subscription) addImage(image *Image) bool {
	if image == nil {
		return false
	}
	if subscription.imageIndexByCorrelationID(image.CorrelationID()) >= 0 {
		return false
	}
	subscription.images = append(subscription.images, image)
	return true
}

// removeImage splices the image out of the slice.
//
// Conductor calls removeImage then Image.internalClose so in-flight Poll snapshots stay valid.
func (subscription *Subscription) removeImage(correlationID int64) *Image {
	index := subscription.imageIndexByCorrelationID(correlationID)
	if index < 0 {
		return nil
	}
	image := subscription.images[index]
	n := len(subscription.images) - 1
	if n == 0 {
		subscription.images = nil
		return image
	}
	newImages := make([]*Image, n)
	copy(newImages[:index], subscription.images[:index])
	copy(newImages[index:], subscription.images[index+1:])
	subscription.images = newImages
	return image
}

// Close tears down this registration via the bound conductor.
//
// Idempotent: repeated calls return nil. Concurrent Close from multiple
// goroutines is unsupported. After close, Poll returns zero.
func (subscription *Subscription) Close() error {
	if subscription.closed {
		return nil
	}
	if subscription.conductor == nil {
		return subscription.markClosed()
	}
	return subscription.conductor.CloseSubscription(subscription)
}

func (subscription *Subscription) markClosed() error {
	subscription.closed = true
	return nil
}
