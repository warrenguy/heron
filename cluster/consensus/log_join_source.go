package consensus

import "github.com/warrenguy/heron/aeron"

// LogJoinSource supplies the follower log join with the live cluster log image
// for a leader-advertised log session id.
//
// Behaviour: Ready reports the join parameters when an open image for that
// session exists on the member's cluster log subscription, and reports not-ready
// otherwise. Image returns that same image, or nil. Two methods rather than one:
// Ready supplies join params for the election's position check and
// IsLeaderStartup rewrite; Image is a separate re-read at join time because the
// params do not carry the image.
//
// Concurrency: invoker-only. Both methods run on the same goroutine as
// Agent.DoWork.
//
// Lifetime: implementations observe transport resources they do not own. They
// never close a subscription or an image.
type LogJoinSource interface {
	// Ready reports join parameters for logSessionID, or false when no open
	// image for that session is available yet.
	//
	// IsLeaderStartup is left false: the election overwrites it before joining.
	Ready(logSessionID int32) (FollowerLogJoinParams, bool)

	// Image returns the open image for logSessionID, or nil.
	Image(logSessionID int32) *aeron.Image
}

// SubscriptionImages exposes a subscription's identity and the images it
// currently holds.
//
// *aeron.Subscription satisfies this interface as-built. The compile-time
// assertion below fails the build if either side moves.
//
// Channel and StreamID are read straight off the subscription rather than passed
// in beside it, so the reported join parameters cannot disagree with the stream
// they describe.
//
// ImageAt contract: implementations may report an index that holds no usable
// image. *aeron.Subscription already filters a nil slot and a closed image to
// ok false; this interface does not require that, so the consumer filters again.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// SubscriptionLogJoinSource calls them only from the invoker goroutine.
type SubscriptionImages interface {
	Channel() string
	StreamID() int32
	ImageCount() int
	ImageAt(index int) (*aeron.Image, bool)
}

var _ SubscriptionImages = (*aeron.Subscription)(nil)

// SubscriptionLogJoinSource selects the cluster log image by log session id.
//
// Behaviour: every member's cluster log subscription holds one image per member
// publishing on the shared log channel, including this member's own, in an order
// that differs between members. Selection is therefore by session id and never
// by index. A closed image is not joinable and is skipped.
//
// Join position: the selected image's join position, fixed when the image is
// created. The election compares it against the log position of the term it is
// installing; they agree when the member's log subscription was open before the
// leader appended, and disagree when the member joined the stream later.
//
// Concurrency: invoker-only. Not safe for concurrent use.
//
// Lifetime: the caller owns the subscription and closes it. This type closes
// nothing and caches nothing.
type SubscriptionLogJoinSource struct {
	images SubscriptionImages
}

var _ LogJoinSource = (*SubscriptionLogJoinSource)(nil)

// NewSubscriptionLogJoinSource returns a join source over the member's cluster
// log subscription.
//
// The reported channel and stream id come from images, so there is nothing to
// keep in step. A nil images argument is legal and makes every Ready report
// not-ready.
func NewSubscriptionLogJoinSource(images SubscriptionImages) *SubscriptionLogJoinSource {
	return &SubscriptionLogJoinSource{images: images}
}

// Image returns the open image carrying logSessionID, or nil.
//
// Walks the subscription on every call rather than caching, so an image that
// closed or was spliced out since the last call is never handed back.
func (s *SubscriptionLogJoinSource) Image(logSessionID int32) *aeron.Image {
	if s == nil || s.images == nil {
		return nil
	}
	for i := range s.images.ImageCount() {
		image, ok := s.images.ImageAt(i)
		if ok && image != nil && image.SessionID() == logSessionID && !image.IsClosed() {
			return image
		}
	}
	return nil
}

// Ready reports join parameters for logSessionID.
//
// Returns false when the receiver is nil, images is nil, no image carries that
// session id, or the image carrying it is closed.
func (s *SubscriptionLogJoinSource) Ready(
	logSessionID int32,
) (FollowerLogJoinParams, bool) {
	image := s.Image(logSessionID)
	if image == nil {
		return FollowerLogJoinParams{}, false
	}

	// The join position is fixed when the image is created; the election
	// compares it against the log position of the term it is installing.
	return FollowerLogJoinParams{
		LogChannel:      s.images.Channel(),
		LogStreamID:     s.images.StreamID(),
		LogSessionID:    logSessionID,
		LogJoinPosition: image.JoinPosition(),
	}, true
}

// SetLogJoinSource wires or clears the [LogJoinSource] used by
// [Agent.FollowerLogJoinReady] and [Agent.JoinLogAsFollower].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Nil is legal: both join methods report not-ready until rewired.
func (a *Agent) SetLogJoinSource(source LogJoinSource) { a.joinSource = source }

// LogJoinSource returns the wired [LogJoinSource], or nil if unset.
func (a *Agent) LogJoinSource() LogJoinSource { return a.joinSource }
