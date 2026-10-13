package aeron

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
)

// Subscription manages registration and the image set for one stream.
//
// Not thread-safe. Must not call Poll or ControlledPoll concurrently with
// Client.DoWork on the same client, and must not call those methods from
// multiple goroutines. One goroutine runs Client.DoWork and the poll methods
// for a given client. Image isClosed uses atomic.Bool for visibility inside
// poll loops. Subscription closed is a plain bool. Cross-goroutine close
// visibility for Image alone holds under that single-writer contract.
//
// The conductor alone mutates the images slice via addImage and removeImage.
// Application code must not modify the slice.
//
// Connect copies image lifecycle callbacks from Config.
// [Subscription.SetUnavailableImageHandler] may replace the unavailable
// callback after the subscription is created.
type Subscription struct {
	registrationID int64
	streamID       int32
	channel        string

	channelStatusCounterID int32
	counters               *cnc.CountersReader

	images []*Image
	// roundRobinIndex is the image slot the next pollImages call starts from.
	// pollImages reduces it modulo len(images) before use.
	roundRobinIndex int

	closed bool

	availableImageHandler   AvailableImageHandler
	unavailableImageHandler UnavailableImageHandler
	conductor               *conductor.Conductor
	client                  *Client

	// receiveDestinationHook, when non-nil, handles AddDestination and
	// RemoveDestination in place of the client/driver path. Tests only.
	receiveDestinationHook receiveDestinationHook
}

// receiveDestinationHook overrides driver destination commands.
//
// Nil in production. Tests set this interface via the test-hook helper only.
type receiveDestinationHook interface {
	AddDestination(channel string) error
	RemoveDestination(channel string) error
}

func newSubscription(
	registrationID int64,
	streamID int32,
	channel string,
	available AvailableImageHandler,
	unavailable UnavailableImageHandler,
) *Subscription {
	return &Subscription{
		registrationID:          registrationID,
		streamID:                streamID,
		channel:                 channel,
		channelStatusCounterID:  cnc.NullCounterID,
		availableImageHandler:   available,
		unavailableImageHandler: unavailable,
	}
}

func (subscription *Subscription) applySubscriptionReady(channelStatusCounterID int32) {
	subscription.channelStatusCounterID = channelStatusCounterID
}

// SetUnavailableImageHandler replaces the unavailable-image callback for this
// subscription.
//
// Behaviour: the next driver OnUnavailableImage for this subscription invokes
// handler. Nil clears the callback. A later call may replace a prior handler.
// A nil receiver is a silent no-op.
//
// Concurrency: invoker-only. Same Client.DoWork restrictions as
// [UnavailableImageHandler].
func (subscription *Subscription) SetUnavailableImageHandler(
	handler UnavailableImageHandler,
) {
	if subscription == nil {
		return
	}
	subscription.unavailableImageHandler = handler
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
	return subscription.pollImages(fragmentLimit, func(image *Image, limit int) int {
		return image.Poll(handler, limit)
	})
}

// ControlledPoll delivers fragments from attached images using round-robin
// fairness, with per-fragment position control via [ControlledFragmentHandler].
//
// Mirrors [Poll] image snapshot and budget rules. Each image runs
// [Image.ControlledPoll] with the remaining fragment budget.
// [ControlledAbort] and [ControlledBreak] stop only the current image's
// batch; remaining budget still visits later images in the rotation.
//
// Returns 0 when the subscription is closed, without advancing roundRobinIndex.
//
// Subscription is not thread-safe; see type documentation.
func (subscription *Subscription) ControlledPoll(
	handler ControlledFragmentHandler,
	fragmentLimit int,
) int {
	return subscription.pollImages(fragmentLimit, func(image *Image, limit int) int {
		return image.ControlledPoll(handler, limit)
	})
}

// pollImages walks a snapshot of images in round-robin order.
//
// Closed: returns 0 without advancing roundRobinIndex.
// Empty: advances roundRobinIndex and returns 0.
// Nil snapshot entries are skipped.
func (subscription *Subscription) pollImages(
	fragmentLimit int,
	pollImage func(image *Image, limit int) int,
) int {
	if subscription.closed {
		return 0
	}

	images := subscription.images
	length := len(images)
	if length == 0 {
		subscription.roundRobinIndex++
		return 0
	}
	startingIndex := subscription.roundRobinIndex % length
	subscription.roundRobinIndex = (startingIndex + 1) % length

	fragmentsRead := 0
	for offset := range length {
		if fragmentsRead >= fragmentLimit {
			break
		}
		image := images[(startingIndex+offset)%length]
		if image == nil {
			continue
		}
		fragmentsRead += pollImage(image, fragmentLimit-fragmentsRead)
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
// goroutines is unsupported. After close, Poll and ControlledPoll return zero.
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
