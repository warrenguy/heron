package service

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// nullValue is the ServiceAck RelevantID sentinel. Matches upstream
// NULL_VALUE (-1). Producers with no RelevantID set nullValue on the entry.
const nullValue int64 = -1

// ErrAckRequestPassed marks an ack request below the log position. The error is
// fatal.
var ErrAckRequestPassed = errors.New("service: ack request position passed")

// ErrAckPublish marks a terminal service-ack offer code. The error is fatal.
var ErrAckPublish = errors.New("service: ack publish failed")

// ErrControlMessageSchema reports a control message with a foreign schema id.
// The error is not fatal.
var ErrControlMessageSchema = errors.New("service: control message schema id mismatch")

// ErrControlMessageDecode reports a control message that does not decode. The
// error is not fatal.
var ErrControlMessageDecode = errors.New("service: control message undecodable")

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
func (a *Agent) handleControlFragment(buf []byte, offset int) {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(buf, offset)
	if !ok {
		a.reportError(fmt.Errorf("%w: short header", ErrControlMessageDecode))
		return
	}
	if schemaID != clusterwire.MessageHeaderSbeSchemaID {
		a.reportError(fmt.Errorf("%w: schema id %d", ErrControlMessageSchema, schemaID))
		return
	}

	switch templateID {
	case clusterwire.JoinLogSbeTemplateID:
		fields, decOK := codecs.TryDecodeJoinLogAt(buf, offset)
		if !decOK {
			a.reportError(fmt.Errorf("%w: template id %d", ErrControlMessageDecode, templateID))
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
			a.reportError(fmt.Errorf("%w: template id %d", ErrControlMessageDecode, templateID))
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
// entries.
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
				a.markFailure(ErrAckRequestPassed)
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

// canPublish reports whether the Agent is open and a bound ModuleProxy
// publication exists.
func (a *Agent) canPublish() bool {
	return !a.closed && a.moduleProxy != nil && a.moduleProxy.Publication() != nil
}

// drainReadyCoordinationAck publishes one ServiceAck for the ready first
// queued entry.
//
// Behaviour:
//   - canPublish false, empty queue, or unready first entry: no send
//   - offer result > 0: pop the first entry, advance ackID, mark the entry
//     failure when set, return 1. A snapshot entry also stores its
//     RelevantID in lastSnapshotRecordingID
//   - retriable offer code: keep the first entry and ackID, return 0
//   - terminal offer code: mark ErrAckPublish with the code, then the entry
//     failure when set. Return 0
//
// Send fields:
//   - LogPosition from the entry
//   - Timestamp from clusterTime
//   - AckID from ackID
//   - RelevantID from the entry. A start entry carries the configured
//     relevant id. After a Start error, the start entry carries nullValue.
//     A snapshot entry carries the recording id. After a failed take, the
//     snapshot entry carries nullValue. Join and request entries carry
//     nullValue
//   - ServiceID from config
func (a *Agent) drainReadyCoordinationAck() int {
	if !a.canPublish() {
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
	if !a.acceptOffer(result, ErrAckPublish, entry.failure) {
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
