package consensus

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var errSnapshotLoad = errors.New("consensus: load snapshot")

// SnapshotLoader restores one module snapshot into its agent.
//
// Behaviour: restores each row as it arrives. Accepts BEGIN, one
// ConsensusModule row, cluster session rows, timer rows, pending-message
// tracker rows, SECTION marks, NULL_VALUE marks, then END. Any other
// sequence fails the load. An error formats each enum as an integer.
//
// Concurrency: invoker-only. Call before the first Agent.DoWork.
type SnapshotLoader struct {
	agent       *Agent
	logPosition int64
	inSnapshot  bool
	sawModule   bool
	done        bool
	err         error

	seenTimers   map[int64]struct{}
	seenTrackers map[int32]struct{}

	handler aeron.ControlledFragmentHandler
}

// NewSnapshotLoader returns a loader that restores into a. Every snapshot
// marker must carry logPosition.
//
// Concurrency: invoker-only. Call before the first Agent.DoWork.
func (a *Agent) NewSnapshotLoader(logPosition int64) *SnapshotLoader {
	l := &SnapshotLoader{
		agent:        a,
		logPosition:  logPosition,
		seenTimers:   make(map[int64]struct{}),
		seenTrackers: make(map[int32]struct{}),
	}
	l.handler = aeron.NewControlledFragmentAssembler(l.handleMessage).Handler()
	return l
}

// Handler returns the handler for the replay image poll. A fragment
// assembler runs before dispatch.
func (l *SnapshotLoader) Handler() aeron.ControlledFragmentHandler {
	return l.handler
}

// IsDone reports whether the loader consumed the END marker.
//
// Behaviour: IsDone stays false when the load fails, or when the replay
// stops before END.
func (l *SnapshotLoader) IsDone() bool {
	return l.done
}

// Err returns the first load failure, or nil.
//
// Behaviour: the error starts with "consensus: load snapshot". IsDone stays
// false when Err is non-nil.
func (l *SnapshotLoader) Err() error {
	return l.err
}

func (l *SnapshotLoader) handleMessage(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	_ *aeron.Header,
) aeron.ControlledAction {
	if l.err != nil {
		return aeron.ControlledAbort
	}
	if l.done {
		return aeron.ControlledBreak
	}
	raw := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(raw) || length > len(raw)-offset {
		return l.failf("fragment offset %d length %d outside the buffer", offset, length)
	}
	return l.dispatch(raw[offset : offset+length])
}

func (l *SnapshotLoader) dispatch(frag []byte) aeron.ControlledAction {
	layout, ok := codecs.ClusterMessageLayoutAt(frag, 0, 0)
	if !ok {
		return l.failLayout(frag)
	}
	if layout.TemplateID == clusterwire.SnapshotMarkerSbeTemplateID {
		return l.handleMarker(frag, layout)
	}
	if !l.inSnapshot {
		return l.failf("template %d before the begin marker", layout.TemplateID)
	}
	switch layout.TemplateID {
	case clusterwire.ConsensusModuleSbeTemplateID:
		return l.handleConsensusModule(frag, layout)

	case clusterwire.ClusterSessionSbeTemplateID:
		return l.handleClusterSession(frag)

	case clusterwire.TimerSbeTemplateID:
		return l.handleTimer(frag, layout)

	case clusterwire.PendingMessageTrackerSbeTemplateID:
		return l.handlePendingMessageTracker(frag, layout)

	default:
		return l.failf("template %d inside the snapshot", layout.TemplateID)
	}
}

func (l *SnapshotLoader) handleMarker(
	frag []byte,
	layout codecs.ClusterMessageLayout,
) aeron.ControlledAction {
	fields, _ := codecs.SnapshotMarkerFieldsFromLayout(frag, layout)
	switch {
	case fields.TypeID != codecs.ConsensusModuleSnapshotTypeID:
		return l.failf(
			"snapshot type id %d, want %d",
			fields.TypeID,
			codecs.ConsensusModuleSnapshotTypeID,
		)

	case fields.LogPosition != l.logPosition:
		return l.failf(
			"marker log position %d, want %d",
			fields.LogPosition,
			l.logPosition,
		)
	}

	switch fields.Mark {
	case clusterwire.SnapshotMark_BEGIN:
		if l.inSnapshot {
			return l.failf("begin marker repeated")
		}
		if fields.TimeUnit != clusterwire.ClusterTimeUnit_NANOS {
			if _, err := clusterwire.LookupClusterTimeUnit(int32(fields.TimeUnit)); err != nil {
				return l.failf("time unit %d unknown", int32(fields.TimeUnit))
			}
			return l.failf(
				"time unit %d, want %d",
				int32(fields.TimeUnit),
				int32(clusterwire.ClusterTimeUnit_NANOS),
			)
		}
		gotMajor := semanticMajor(fields.AppVersion)
		wantMajor := semanticMajor(l.agent.appVersion)
		if gotMajor != wantMajor {
			return l.failf("app version major %d, want %d", gotMajor, wantMajor)
		}
		l.inSnapshot = true
		return aeron.ControlledContinue

	case clusterwire.SnapshotMark_END:
		if !l.inSnapshot {
			return l.failf("end marker before the begin marker")
		}
		if !l.sawModule {
			return l.failf("end marker without a consensus module row")
		}
		l.done = true
		return aeron.ControlledBreak

	case clusterwire.SnapshotMark_SECTION, clusterwire.SnapshotMark_NULL_VALUE:
		if !l.inSnapshot {
			return l.failf(
				"snapshot mark %d before the begin marker",
				int32(fields.Mark),
			)
		}
		return aeron.ControlledContinue

	default:
		return l.failf("snapshot mark %d unknown", int32(fields.Mark))
	}
}

func (l *SnapshotLoader) handleConsensusModule(
	frag []byte,
	layout codecs.ClusterMessageLayout,
) aeron.ControlledAction {
	if l.sawModule {
		return l.failf("consensus module row repeated")
	}
	fields, ok := codecs.ConsensusModuleFieldsFromLayout(frag, layout)
	if !ok {
		return l.failf("consensus module row does not decode")
	}
	l.sawModule = true
	l.agent.sessions.nextSessionID = max(l.agent.sessions.nextSessionID, fields.NextSessionID)
	return aeron.ControlledContinue
}

func (l *SnapshotLoader) handleClusterSession(frag []byte) aeron.ControlledAction {
	fields, ok := codecs.TryDecodeClusterSessionAt(frag, 0)
	if !ok {
		return l.failf("cluster session row does not decode")
	}
	if err := l.agent.sessions.restoreSession(fields); err != nil {
		return l.failf("%s", err.Error())
	}
	return aeron.ControlledContinue
}

func (l *SnapshotLoader) handleTimer(
	frag []byte,
	layout codecs.ClusterMessageLayout,
) aeron.ControlledAction {
	fields, ok := codecs.TimerFieldsFromLayout(frag, layout)
	if !ok {
		return l.failf("timer row does not decode")
	}
	if _, seen := l.seenTimers[fields.CorrelationID]; seen {
		return l.failf("timer %d repeated", fields.CorrelationID)
	}
	if !l.agent.timers.schedule(fields.CorrelationID, fields.Deadline) {
		return l.failf("timer wheel refused correlation id %d", fields.CorrelationID)
	}
	l.seenTimers[fields.CorrelationID] = struct{}{}
	return aeron.ControlledContinue
}

func (l *SnapshotLoader) handlePendingMessageTracker(
	frag []byte,
	layout codecs.ClusterMessageLayout,
) aeron.ControlledAction {
	fields, ok := codecs.PendingMessageTrackerFieldsFromLayout(frag, layout)
	if !ok {
		return l.failf("pending message tracker row does not decode")
	}
	if !l.agent.serviceAcks.hasService(fields.ServiceID) {
		return l.failf(
			"tracker service id %d outside service count %d",
			fields.ServiceID,
			len(l.agent.serviceAcks.queues),
		)
	}
	if _, seen := l.seenTrackers[fields.ServiceID]; seen {
		return l.failf("tracker service id %d repeated", fields.ServiceID)
	}
	l.seenTrackers[fields.ServiceID] = struct{}{}
	return aeron.ControlledContinue
}

// semanticMajor is the Agrona semantic major in the low 8 bits. cnc.MajorVersion
// keeps the high bits.
func semanticMajor(version int32) int {
	return cnc.MajorVersion(version) & 0xff
}

func (l *SnapshotLoader) failLayout(frag []byte) aeron.ControlledAction {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(frag, 0)
	switch {
	case !ok:
		return l.failf("message header short, length %d", len(frag))

	case schemaID != clusterwire.MessageHeaderSbeSchemaID:
		return l.failf(
			"schema id %d, want %d",
			schemaID,
			clusterwire.MessageHeaderSbeSchemaID,
		)

	case templateID == clusterwire.SnapshotMarkerSbeTemplateID:
		return l.failf("snapshot marker does not decode")

	default:
		return l.failf("template %d does not decode", templateID)
	}
}

func (l *SnapshotLoader) failf(format string, args ...any) aeron.ControlledAction {
	if l.err == nil {
		l.err = fmt.Errorf("%w: %s", errSnapshotLoad, fmt.Sprintf(format, args...))
	}
	l.done = false
	return aeron.ControlledAbort
}
