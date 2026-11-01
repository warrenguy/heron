package archive

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
)

var errFindReplayImageNilSubscription = errors.New(
	"archive: find replay image: subscription is nil",
)

// FindReplayImage returns the first open Image whose session id matches
// replaySessionID. Java Aeron Archive returns a composite replaySessionId of
// (replayId << 32) | publication.sessionId(); subscription images expose only the
// publication session id in the lower 32 bits. Matching therefore compares
// image.SessionID() against int32(uint32(replaySessionID)), not the full int64.
//
// Single-step: does not poll and does not call DoWork. Returns (nil, false, nil) when
// no matching image is present yet. A nil subscription is a programmer error.
func FindReplayImage(
	sub *aeron.Subscription,
	replaySessionID int64,
) (*aeron.Image, bool, error) {
	if sub == nil {
		return nil, false, errFindReplayImageNilSubscription
	}
	//nolint:gosec // G115: the image carries the low 32 bits of the replay session id
	imageSessionID := int32(uint32(replaySessionID))
	image, ok := sub.ImageBySessionID(imageSessionID)
	return image, ok, nil
}

// PollReplayFragments polls up to want-received fragments on image, delivering each
// committed fragment to handler. Returns the updated received total and done=true when
// the total reaches want or (when stopPosition > 0) image.Position() >= stopPosition.
// want must be positive for count-based drain. When received >= want the call completes
// without consulting stopPosition; any non-positive want with received >= want completes
// immediately without polling, regardless of stopPosition. Stop-position completion
// applies only when received < want and stopPosition > 0.
//
// Buffer views and the header pointer follow aeron.FragmentHandler lifetime rules.
//
// Single-step: does not call DoWork. Caller must drive client.DoWork() between
// iterations when assembling fragments from the conductor.
func PollReplayFragments(
	image *aeron.Image,
	want int,
	stopPosition int64,
	received int,
	handler aeron.FragmentHandler,
) (int, bool, error) {
	if image == nil {
		return received, false, errors.New("archive: poll replay fragments: image is nil")
	}
	if handler == nil {
		return received, false, errors.New("archive: poll replay fragments: handler is nil")
	}
	if replayFragmentsComplete(image, want, stopPosition, received) {
		return received, true, nil
	}
	if image.IsClosed() {
		return received, false, errors.New("archive: poll replay fragments: image is closed")
	}

	received += image.Poll(handler, max(want-received, 1))

	if replayFragmentsComplete(image, want, stopPosition, received) {
		return received, true, nil
	}
	return received, false, nil
}

func replayFragmentsComplete(
	image *aeron.Image,
	want int,
	stopPosition int64,
	received int,
) bool {
	if received >= want {
		return true
	}
	return stopPosition > 0 && image.Position() >= stopPosition
}
