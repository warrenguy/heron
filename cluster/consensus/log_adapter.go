package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// DefaultLogFragmentLimit matches Aeron LOG_FRAGMENT_LIMIT_DEFAULT (50).
const DefaultLogFragmentLimit = 50

// LogAdapterHandler receives decoded cluster log events the adapter understands.
// Nil handler is legal: Poll still advances Image position under the bound.
//
// Concurrency: invoker-only; same goroutine as Poll.
type LogAdapterHandler interface {
	SessionMessage(clusterSessionID, timestamp int64)
	NewLeadershipTermEvent(
		leadershipTermID int64,
		logPosition int64,
		timestamp int64,
		termBaseLogPosition int64,
		timeUnit clusterwire.ClusterTimeUnit,
		appVersion int32,
	)
}

// LogAdapter polls a cluster log Image under a stream-position commit bound.
//
// Concurrency: invoker-only. One goroutine owns Poll / SetImage (Image contract).
//
// Lifetime: caller owns the Image. LogAdapter does not Close the Image on replace
// (matches LogPublisher publication ownership).
type LogAdapter struct {
	image         *aeron.Image
	fragmentLimit int
	logPosition   int64 // cached when image nil
	handler       LogAdapterHandler
}

// NewLogAdapter returns an adapter with no Image bound.
// fragmentLimit <= 0 becomes DefaultLogFragmentLimit (50).
func NewLogAdapter(fragmentLimit int) *LogAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultLogFragmentLimit
	}
	return &LogAdapter{fragmentLimit: fragmentLimit}
}

// SetImage binds or replaces the log Image. When replacing a non-nil image,
// caches previous image.Position into logPosition.
//
// Does not close the previous Image; the caller owns lifecycle.
func (a *LogAdapter) SetImage(image *aeron.Image) {
	if a.image != nil {
		a.logPosition = a.image.Position()
	}
	a.image = image
}

// DisconnectAt unbinds the cluster log Image and lowers the reported position
// to maxLogPosition.
//
// Behaviour: latches the bound Image position, clears the Image reference, then
// reports the lower of the latched position and maxLogPosition. A later Poll
// returns 0 because no Image is bound. Idempotent: a second call with no Image
// bound only re-applies the clamp.
//
// Concurrency: invoker-only; same goroutine as Poll.
//
// Ownership: does not own the Image and does not own its Subscription. Neither
// is closed. The caller that bound the Image keeps the close duty, matching
// [LogAdapter.SetImage].
func (a *LogAdapter) DisconnectAt(maxLogPosition int64) {
	if a == nil {
		return
	}
	a.SetImage(nil)
	a.logPosition = min(a.logPosition, maxLogPosition)
}

// Image returns the bound Image, or nil.
func (a *LogAdapter) Image() *aeron.Image {
	return a.image
}

// SetHandler wires or clears the optional decode handler.
func (a *LogAdapter) SetHandler(handler LogAdapterHandler) {
	a.handler = handler
}

// Position returns image.Position when bound, else cached logPosition.
//
// Nil receiver returns 0.
func (a *LogAdapter) Position() int64 {
	if a == nil {
		return 0
	}
	if a.image == nil {
		return a.logPosition
	}
	return a.image.Position()
}

// IsImageClosed reports no image or image.IsClosed.
//
// Nil adapter or nil image returns true.
func (a *LogAdapter) IsImageClosed() bool {
	if a == nil || a.image == nil {
		return true
	}
	return a.image.IsClosed()
}

// Poll delivers up to fragmentLimit DATA fragments with start position strictly
// before boundPosition via Image.BoundedControlledPoll.
//
// Nil adapter or nil image returns 0. One duty cycle is one BoundedControlledPoll
// call.
//
// Returns fragment count only; no error path.
func (a *LogAdapter) Poll(boundPosition int64) int {
	if a == nil || a.image == nil {
		return 0
	}
	return a.image.BoundedControlledPoll(a.handleFragment, boundPosition, a.fragmentLimit)
}

// handleFragment is the ControlledFragmentHandler for BoundedControlledPoll.
//
// Unfragmented only. Fragmented frames (missing UNFRAGMENTED) return CONTINUE
// without reassembly. Position still advances under ControlledContinue.
//
// Decode is bound to the fragment span (offset, length), not the whole term
// buffer, so body layout checks cannot read past the frame.
//
// Schema must be cluster schema (111). Unknown template IDs: CONTINUE no-op.
// Decode failure: CONTINUE (fail-open on position advance; do not stall the
// follower forever on corrupt optional fields - transport already committed
// the frame).
func (a *LogAdapter) handleFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	if header.Flags()&logbuffer.Unfragmented != logbuffer.Unfragmented {
		return aeron.ControlledContinue
	}

	// Nil handler: advance position only; skip decode.
	handler := a.handler
	if handler == nil {
		return aeron.ControlledContinue
	}

	// Bound decode to the fragment payload only (match client egress span).
	raw := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(raw)-length {
		return aeron.ControlledContinue
	}
	frag := raw[offset : offset+length]

	// Single header parse; template dispatch reuses the layout (egress pattern).
	layout, ok := codecs.ClusterMessageLayoutAt(frag, 0, 0)
	if !ok {
		return aeron.ControlledContinue
	}

	switch layout.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		if fields, decoded := codecs.SessionMessageHeaderFieldsFromLayout(frag, layout); decoded {
			handler.SessionMessage(fields.ClusterSessionID, fields.Timestamp)
		}
	case clusterwire.NewLeadershipTermEventSbeTemplateID:
		if fields, decoded := codecs.NewLeadershipTermEventFieldsFromLayout(frag, layout); decoded {
			handler.NewLeadershipTermEvent(
				fields.LeadershipTermID,
				fields.LogPosition,
				fields.Timestamp,
				fields.TermBaseLogPosition,
				fields.TimeUnit,
				fields.AppVersion,
			)
		}
	}
	return aeron.ControlledContinue
}
