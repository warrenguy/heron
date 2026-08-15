package service

import (
	"errors"
	"math"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// DefaultLogFragmentLimit matches upstream LOG_FRAGMENT_LIMIT_DEFAULT (50).
const DefaultLogFragmentLimit = 50

// Dispatch failure causes returned to handleDispatchFailure.
var (
	errLogDispatchHeader = errors.New("service: log message header invalid")
	errLogDispatchSchema = errors.New("service: log message schema id mismatch")
	errLogDispatchDecode = errors.New("service: log message undecodable")
)

// logEventSink receives decoded log events and decides the poll action.
// Implemented by Agent. Unexported: the adapter and the agent share the
// package; tests substitute a recording sink.
type logEventSink interface {
	handleSessionMessage(
		logPosition, sessionID, timestamp int64,
		payload []byte,
	) aeron.ControlledAction
	handleSessionOpen(
		logPosition int64,
		fields codecs.SessionOpenEventFields,
	) aeron.ControlledAction
	handleSessionClose(
		logPosition int64,
		fields codecs.SessionCloseEventFields,
	) aeron.ControlledAction
	handleTimerEvent(
		logPosition int64,
		fields codecs.TimerEventFields,
	) aeron.ControlledAction
	handleNewLeadershipTermEvent(
		logPosition int64,
		fields codecs.NewLeadershipTermEventFields,
	) aeron.ControlledAction
	handleClusterAction(
		logPosition int64,
		fields codecs.ClusterActionRequestFields,
	) aeron.ControlledAction
	handleDispatchFailure(err error) aeron.ControlledAction
}

// BoundedLogAdapter consumes the cluster log Image under a stream-position
// upper bound and dispatches whole messages to the sink.
//
// Concurrency: invoker-only. One goroutine owns Poll and SetImage.
//
// Ownership: does not own the Image. The caller that bound the Image keeps the
// close duty. BoundedLogAdapter does not close the Image on replace.
type BoundedLogAdapter struct {
	fragmentLimit  int
	image          *aeron.Image
	maxLogPosition int64
	logPosition    int64 // cached when image nil
	assembler      *aeron.ControlledFragmentAssembler
	handler        aeron.ControlledFragmentHandler
	sink           logEventSink
}

// NewBoundedLogAdapter builds an adapter with no Image bound.
//
// maxLogPosition starts at math.MaxInt64: consumption is uncapped until a join
// sets the cap. fragmentLimit <= 0 takes DefaultLogFragmentLimit. A nil sink
// panics: the adapter exists to dispatch.
func NewBoundedLogAdapter(sink logEventSink, fragmentLimit int) *BoundedLogAdapter {
	if sink == nil {
		panic("service: nil logEventSink")
	}
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultLogFragmentLimit
	}
	b := &BoundedLogAdapter{
		fragmentLimit:  fragmentLimit,
		maxLogPosition: math.MaxInt64,
		sink:           sink,
	}
	b.assembler = aeron.NewControlledFragmentAssembler(b.dispatch)
	// Bind once: Handler allocates a method value per call.
	b.handler = b.assembler.Handler()
	return b
}

// SetImage binds or replaces the log Image.
//
// When replacing a non-nil image, caches the previous image.Position into
// logPosition. Does not close the previous Image; the caller owns lifecycle.
func (b *BoundedLogAdapter) SetImage(image *aeron.Image) {
	if b == nil {
		return
	}
	if b.image != nil {
		b.logPosition = b.image.Position()
	}
	b.image = image
}

// SetMaxLogPosition caps consumption at position (join handoff point).
func (b *BoundedLogAdapter) SetMaxLogPosition(position int64) {
	if b == nil {
		return
	}
	b.maxLogPosition = position
}

// Position returns image position when bound, else the cached position.
//
// Nil receiver returns 0.
func (b *BoundedLogAdapter) Position() int64 {
	if b == nil {
		return 0
	}
	if b.image == nil {
		return b.logPosition
	}
	return b.image.Position()
}

// Image returns the bound Image, or nil.
func (b *BoundedLogAdapter) Image() *aeron.Image {
	if b == nil {
		return nil
	}
	return b.image
}

// MaxLogPosition returns the consumption cap.
//
// Nil receiver returns 0.
func (b *BoundedLogAdapter) MaxLogPosition() int64 {
	if b == nil {
		return 0
	}
	return b.maxLogPosition
}

// Poll delivers whole messages whose frames start before
// min(boundPosition, maxLogPosition).
//
// Returns the fragment count. Nil adapter or nil image returns 0. One duty
// cycle is one BoundedControlledPoll call.
func (b *BoundedLogAdapter) Poll(boundPosition int64) int {
	if b == nil || b.image == nil {
		return 0
	}
	limit := min(boundPosition, b.maxLogPosition)
	return b.image.BoundedControlledPoll(b.handler, limit, b.fragmentLimit)
}

// dispatch is the ControlledFragmentAssembler delegate. Receives whole
// messages only (unfragmented frames, or assembled fragment trains).
//
// Behaviour:
//   - wrong schema or undecodable known template: sink failure action
//     (normally ControlledAbort)
//   - unknown template, or known cluster template not owned here:
//     ControlledContinue
//
// The hot path parses the layout once. Failure classification re-reads the
// header only when layout parse fails.
func (b *BoundedLogAdapter) dispatch(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	raw := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(raw)-length {
		return b.sink.handleDispatchFailure(errLogDispatchHeader)
	}
	frag := raw[offset : offset+length]
	logPosition := header.Position()

	layout, layoutOK := codecs.ClusterMessageLayoutAt(frag, 0, 0)
	if !layoutOK {
		return b.classifyLayoutFailure(frag)
	}

	switch layout.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		fields, decoded := codecs.SessionMessageHeaderFieldsFromLayout(frag, layout)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		payload := codecs.SessionMessagePayloadFromLayout(frag, layout)
		if payload == nil {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleSessionMessage(
			logPosition,
			fields.ClusterSessionID,
			fields.Timestamp,
			payload,
		)

	case clusterwire.TimerEventSbeTemplateID:
		fields, decoded := codecs.TimerEventFieldsFromLayout(frag, layout)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleTimerEvent(logPosition, fields)

	case clusterwire.SessionOpenEventSbeTemplateID:
		// SessionOpen has var-length fields. TryDecode validates lengths first.
		// FieldsFromLayout alone does not fail closed on corrupt var lengths.
		fields, decoded := codecs.TryDecodeSessionOpenEventAt(frag, 0)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleSessionOpen(logPosition, fields)

	case clusterwire.SessionCloseEventSbeTemplateID:
		fields, decoded := codecs.SessionCloseEventFieldsFromLayout(frag, layout)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleSessionClose(logPosition, fields)

	case clusterwire.NewLeadershipTermEventSbeTemplateID:
		fields, decoded := codecs.NewLeadershipTermEventFieldsFromLayout(frag, layout)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleNewLeadershipTermEvent(logPosition, fields)

	case clusterwire.ClusterActionRequestSbeTemplateID:
		fields, decoded := codecs.ClusterActionRequestFieldsFromLayout(frag, layout)
		if !decoded {
			return b.sink.handleDispatchFailure(errLogDispatchDecode)
		}
		return b.sink.handleClusterAction(logPosition, fields)

	default:
		// Layout succeeded for a template this adapter does not dispatch.
		return aeron.ControlledContinue
	}
}

// classifyLayoutFailure maps a failed ClusterMessageLayoutAt onto the
// fail-closed error taxonomy. Templates absent from the root bounds table
// return ControlledContinue (same as the dispatch switch default).
func (b *BoundedLogAdapter) classifyLayoutFailure(frag []byte) aeron.ControlledAction {
	_, templateID, schemaID, version, ok := codecs.DecodeMessageHeader(frag, 0)
	if !ok {
		return b.sink.handleDispatchFailure(errLogDispatchHeader)
	}
	if schemaID != clusterwire.MessageHeaderSbeSchemaID {
		return b.sink.handleDispatchFailure(errLogDispatchSchema)
	}
	// Header and schema OK. Layout failed on block bounds or a short body.
	if _, _, known := codecs.ClusterRootBlockLengthBounds(templateID, version); !known {
		return aeron.ControlledContinue
	}
	return b.sink.handleDispatchFailure(errLogDispatchDecode)
}
