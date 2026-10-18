package service

import (
	"errors"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// nullValue is the ServiceAck RelevantID sentinel. Matches upstream
// NULL_VALUE (-1). Producers with no RelevantID set nullValue on the entry.
const nullValue int64 = -1

// Failure causes for the control and ack path. Marked once on the service path.
// Unexported: tests match with errors.Is against the package-level values.
var (
	errAckRequestPassed = errors.New("service: ack request position passed")
	errAckPublish       = errors.New("service: ack publish failed")
)

// controlWork runs one control duty step:
//  1. Poll Control when non-nil (defaultControlFragmentLimit); dispatch each
//     whole message
//  2. Sweep unready request entries for readiness or passed-position failure
//  3. Drain at most one ready first queued ack when ModuleProxy has a bound
//     publication
//
// Returns the work count: control messages delivered plus one on a successful
// send.
//
// Concurrency: invoker-only. Called from Agent.DoWork.
func (a *Agent) controlWork() int {
	work := 0
	if a.control != nil {
		work += a.control.Poll(defaultControlFragmentLimit, a.controlOnFragment)
	}
	a.sweepRequestAckReadiness()
	work += a.drainReadyCoordinationAck()
	return work
}

// handleControlFragment routes one whole control message at MessageHeader
// offset.
//
// Behaviour:
//   - Not ok or wrong schema: skip silently
//   - JoinLog (40): decode, map to logJoinEvent, call handleJoinLog
//   - RequestServiceAck (108): decode, append one request-kind entry
//   - Other templates and undecodable known templates: skip silently
func (a *Agent) handleControlFragment(buf []byte, offset int) {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterwire.MessageHeaderSbeSchemaID {
		return
	}

	switch templateID {
	case clusterwire.JoinLogSbeTemplateID:
		fields, decOK := codecs.TryDecodeJoinLogAt(buf, offset)
		if !decOK {
			return
		}
		a.handleJoinLog(logJoinEvent{
			logPosition:    fields.LogPosition,
			maxLogPosition: fields.MaxLogPosition,
			memberID:       fields.MemberID,
			logSessionID:   fields.LogSessionID,
			logStreamID:    fields.LogStreamID,
			isStartup:      fields.IsStartup == clusterwire.BooleanType_TRUE,
			role:           codecs.Role(fields.Role),
			logChannel:     string(fields.LogChannel),
		})

	case clusterwire.RequestServiceAckSbeTemplateID:
		fields, decOK := codecs.TryDecodeRequestServiceAckAt(buf, offset)
		if !decOK {
			return
		}
		a.handleRequestServiceAck(fields.LogPosition)
	}
}

// handleRequestServiceAck appends one request-kind coordinationAck with ready
// false. Sets relevantID to nullValue. Every delivered request gets its own entry.
func (a *Agent) handleRequestServiceAck(logPosition int64) {
	if a.closed {
		return
	}
	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: logPosition,
		kind:        ackKindRequest,
		ready:       false,
		relevantID:  nullValue,
	})
}

// sweepRequestAckReadiness marks request entries ready when the agent log
// position equals the requested position. Marks failure once when the log has
// passed a requested position. Leaves entries unready when the log is still
// below the request.
//
// Also refreshes unreadyRequestBound and hasUnreadyRequestBound for the log poll
// bound. One walk covers readiness and the lowest unready request position.
//
// A passed entry stays unready so the prefix gate blocks drain of later
// entries. markFailure runs once; later ticks freeze on serviceFailure.
func (a *Agent) sweepRequestAckReadiness() {
	pos := a.logPosition
	lowest := int64(0)
	found := false
	for i := range a.coordinationAcks {
		e := &a.coordinationAcks[i]
		if e.kind != ackKindRequest {
			continue
		}
		if !e.ready {
			switch {
			case pos == e.logPosition:
				e.ready = true
			case pos > e.logPosition:
				a.markFailure(errAckRequestPassed)
			}
		}
		if !e.ready {
			if !found || e.logPosition < lowest {
				lowest = e.logPosition
				found = true
			}
		}
	}
	a.unreadyRequestBound = lowest
	a.hasUnreadyRequestBound = found
}

// canSendAck reports whether a bound ModuleProxy publication exists.
func (a *Agent) canSendAck() bool {
	return a.moduleProxy != nil && a.moduleProxy.Publication() != nil
}

// drainReadyCoordinationAck publishes one ServiceAck for the ready first
// queued entry.
//
// Behaviour:
//   - canSendAck false, empty queue, or unready first entry: no send
//   - offer result > 0: pop the first entry, advance ackID, mark the entry
//     failure when set, return 1. A snapshot entry also stores its
//     RelevantID in lastSnapshotRecordingID
//   - retriable offer code: keep the first entry and ackID, return 0
//   - terminal offer code: mark the entry failure when set, else
//     errAckPublish. Return 0
//
// Send fields:
//   - LogPosition from the entry
//   - Timestamp from clusterTime
//   - AckID from ackID
//   - RelevantID from the entry. Start entries carry the configured
//     relevant id. Snapshot entries carry the recording id, or nullValue
//     when the take failed. Join and request entries carry nullValue
//   - ServiceID from config
func (a *Agent) drainReadyCoordinationAck() int {
	if !a.canSendAck() {
		return 0
	}
	if len(a.coordinationAcks) == 0 {
		return 0
	}
	entry := a.coordinationAcks[0]
	if !entry.ready {
		return 0
	}

	result := a.moduleProxy.PublishServiceAck(codecs.ServiceAckFields{
		LogPosition: entry.logPosition,
		Timestamp:   a.clusterTime,
		AckID:       a.ackID,
		RelevantID:  entry.relevantID,
		ServiceID:   a.serviceID,
	})
	cause := errAckPublish
	if entry.failure != nil {
		cause = entry.failure
	}
	if !a.acceptOffer(result, cause) {
		return 0
	}
	a.coordinationAcks = a.coordinationAcks[1:]
	a.ackID++
	if entry.kind == ackKindSnapshot {
		a.lastSnapshotRecordingID = entry.relevantID
	}
	if entry.failure != nil {
		a.markFailure(entry.failure)
	}
	return 1
}
