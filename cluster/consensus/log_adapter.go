package consensus

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// DefaultLogFragmentLimit matches Aeron LOG_FRAGMENT_LIMIT_DEFAULT (50).
const DefaultLogFragmentLimit = 50

// LogAdapterHandler receives decoded cluster log events the adapter understands.
// A nil handler is allowed. Poll still advances Image position under the bound.
//
// SessionOpenEvent and SessionCloseEvent do not reach this handler.
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
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

// sessionEventSink receives committed session lifecycle frames.
//
// Behaviour: the agent sets itself as the sink in [Agent.SetLogAdapter]. A nil
// sink skips dispatch. The adapter must not forward SessionOpenEvent or
// SessionCloseEvent to [LogAdapterHandler].
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
type sessionEventSink interface {
	handleSessionOpenEvent(fields codecs.SessionOpenEventFields, logPosition int64)
	handleSessionCloseEvent(fields codecs.SessionCloseEventFields)
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
	sessionSink   sessionEventSink
	// assembler reassembles BEGIN, middle, and END trains before handleFragment.
	assembler *aeron.ControlledFragmentAssembler
	// assemblerHandler is the Handler() method value cached at construction.
	// Handler() allocates. Poll must not call Handler each duty cycle.
	assemblerHandler aeron.ControlledFragmentHandler
}

// NewLogAdapter returns an adapter with no Image bound.
// fragmentLimit <= 0 becomes DefaultLogFragmentLimit (50).
func NewLogAdapter(fragmentLimit int) *LogAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultLogFragmentLimit
	}
	a := &LogAdapter{fragmentLimit: fragmentLimit}
	a.assembler = aeron.NewControlledFragmentAssembler(a.handleFragment)
	a.assemblerHandler = a.assembler.Handler()
	return a
}

// SetImage binds or replaces the log Image. When replacing a non-nil image,
// caches the previous image.Position into logPosition. Clears in-progress
// reassembly so a later image cannot complete a prior train.
//
// Does not close the previous Image. The caller owns the Image lifecycle.
func (a *LogAdapter) SetImage(image *aeron.Image) {
	if a.image != nil {
		a.logPosition = a.image.Position()
	}
	a.image = image
	if a.assembler != nil {
		a.assembler.Clear()
	}
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

// SetSessionEventSink wires or clears the session lifecycle sink.
//
// Behaviour: a nil sink skips SessionOpenEvent and SessionCloseEvent dispatch.
// The setter does not change [LogAdapterHandler].
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
func (a *LogAdapter) SetSessionEventSink(sink sessionEventSink) {
	a.sessionSink = sink
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

// Poll delivers up to fragmentLimit DATA fragments whose start position is
// strictly before boundPosition. Delivery uses Image.BoundedControlledPoll.
// Fragmented trains reassemble through a ControlledFragmentAssembler bound at
// construction.
//
// A nil adapter or a nil image returns 0. One duty cycle is one
// BoundedControlledPoll call.
//
// Returns: fragment count only. No error path. 0 with an advanced Position
// means the poll skipped a term-end PAD frame or a term-end slack gap.
func (a *LogAdapter) Poll(boundPosition int64) int {
	if a == nil || a.image == nil {
		return 0
	}
	return a.image.BoundedControlledPoll(a.assemblerHandler, boundPosition, a.fragmentLimit)
}

// handleFragment is the assembler delegate for BoundedControlledPoll.
//
// Behaviour: receives whole messages only. Decode is bound to the fragment
// span. Unknown templates and decode failure return CONTINUE so the poll
// position still advances. A corrupt optional field must not stall the
// follower. The transport already committed the frame.
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
func (a *LogAdapter) handleFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	sink := a.sessionSink
	handler := a.handler
	if header == nil || (sink == nil && handler == nil) {
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
	case clusterwire.SessionOpenEventSbeTemplateID:
		if sink == nil {
			return aeron.ControlledContinue
		}
		if fields, decoded := codecs.TryDecodeSessionOpenEventAt(frag, 0); decoded {
			sink.handleSessionOpenEvent(fields, header.Position())
		}
	case clusterwire.SessionCloseEventSbeTemplateID:
		if sink == nil {
			return aeron.ControlledContinue
		}
		if fields, decoded := codecs.SessionCloseEventFieldsFromLayout(frag, layout); decoded {
			sink.handleSessionCloseEvent(fields)
		}
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		if handler == nil {
			return aeron.ControlledContinue
		}
		if fields, decoded := codecs.SessionMessageHeaderFieldsFromLayout(frag, layout); decoded {
			handler.SessionMessage(fields.ClusterSessionID, fields.Timestamp)
		}
	case clusterwire.NewLeadershipTermEventSbeTemplateID:
		if handler == nil {
			return aeron.ControlledContinue
		}
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
