package consensus

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// defaultServiceControlFragmentLimit is the max fragments drained per
// [Agent.pollServiceControl] poll. Shares the consensus ingress batch size.
const defaultServiceControlFragmentLimit = defaultConsensusIngressFragmentLimit

// spyChannelPrefix marks a subscription that spies a publication. Applied to
// non-IPC leader log channels in JoinLog.
const spyChannelPrefix = "aeron-spy:"

// ipcChannelMedia is the IPC channel media prefix used for JoinLog spy policy.
const ipcChannelMedia = "aeron:ipc"

// ErrServiceControlUnbound reports a publish path called with no service
// control publication bound (ServiceCount 0 or module not open).
var ErrServiceControlUnbound = errors.New(
	"consensus: service control publication is not bound",
)

// SetServiceIngress replaces the service control poller (stream 105).
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
//
// Nil is allowed. [Agent.pollServiceControl] returns 0 until rewired.
func (a *Agent) SetServiceIngress(ingress ConsensusIngress) {
	a.serviceIngress = ingress
}

// ServiceIngress returns the wired service control ingress, or nil.
func (a *Agent) ServiceIngress() ConsensusIngress {
	return a.serviceIngress
}

// SetServiceProxy binds or replaces the service control publisher. Nil is
// allowed.
//
// Behaviour:
//   - JoinLog publish is a silent no-op while nil (return 0; no wait; no
//     failure mark)
//   - [Agent.RequestServiceAck] returns [ErrServiceControlUnbound] while nil
//   - pendingJoinLog survives nil and rebind
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetServiceProxy(p *ServiceProxy) {
	a.serviceProxy = p
}

// ServiceProxy returns the wired publisher, or nil.
func (a *Agent) ServiceProxy() *ServiceProxy {
	return a.serviceProxy
}

// SetLocalLogStream sets the channel and stream id the leader JoinLog
// advertises.
//
// OpenLogStreams calls it with the resolved log publication channel.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetLocalLogStream(channel string, streamID int32) {
	a.localLogChannel = channel
	a.localLogStreamID = streamID
}

// PendingServiceAckWaits returns the queued coordination wait count.
//
// Concurrency: invoker-only.
func (a *Agent) PendingServiceAckWaits() int {
	return len(a.serviceAckWaits)
}

// ExpectedAckPosition returns the first queued wait position, or -1 (the
// package nullValue sentinel) when no wait is queued.
//
// Concurrency: invoker-only.
func (a *Agent) ExpectedAckPosition() int64 {
	if len(a.serviceAckWaits) == 0 {
		return nullValue
	}
	return a.serviceAckWaits[0]
}

// HasPendingJoinLog reports an unpublished JoinLog awaiting retry.
//
// Concurrency: invoker-only.
func (a *Agent) HasPendingJoinLog() bool {
	return a.pendingJoinLog != nil
}

// ServicePublishFailure returns the first terminal publish failure on the
// stream 104 publish path, or nil. Marked once. Never cleared in this surface.
//
// The stream 105 receive-path mark is [Agent.ServiceAckFailure].
//
// Concurrency: invoker-only.
func (a *Agent) ServicePublishFailure() error {
	return a.servicePublishFailure
}

// RequestServiceAck publishes RequestServiceAck (template 108) at logPosition
// and queues a coordination wait for the matching acks.
//
// Callers should pass a committed position the services will observe exactly.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - no service proxy is bound ([ErrServiceControlUnbound])
//   - serviceAckFailure is marked (returns that failure)
//   - a snapshot action wait is pending ([errSnapshotActionPending])
//   - the publish returns a non-positive code after the attempt budget
//
// On error no wait is queued. RequestServiceAck does not store a pending
// retry.
func (a *Agent) RequestServiceAck(logPosition int64) error {
	if a.serviceProxy == nil {
		return ErrServiceControlUnbound
	}
	if a.serviceAckFailure != nil {
		return a.serviceAckFailure
	}
	if a.snapshotWaitPosition != 0 {
		return errSnapshotActionPending
	}
	pos := a.serviceProxy.PublishRequestServiceAck(logPosition)
	if pos <= 0 {
		return fmt.Errorf(
			"consensus: request service ack publish failed: code=%d",
			pos,
		)
	}
	a.serviceAckWaits = append(a.serviceAckWaits, logPosition)
	return nil
}

// serviceControlWork consumes the first coordination wait when every service's
// first queued entry has reached the wait position.
//
// Behaviour:
//  1. serviceAckFailure non-nil: skip. The wait stays queued.
//  2. Empty waits: return.
//  3. First queued entry check via hasReachedServiceAcks:
//     - (true, nil): removeServiceAckHead, pop the first wait, count work
//     - (false, nil): not every first queued entry has reached; return
//     - (false, ErrServiceAckOutOfSequence): mark serviceAckFailure once
//
// At most one wait completes per DoWork call.
//
// A consumed wait that matches snapshotWaitPosition clears
// snapshotWaitPosition to 0. A consumed wait at a retired snapshot position
// drops leftover tracker entries at the consumed position. A remaining wait at
// the consumed position keeps the entries.
//
// Pending JoinLog retry is [Agent.retryPendingJoinLog]. DoWork runs the retry
// after the election or steady branch. PrepareForNewLeadership can then drop a
// superseded pending before any offer.
//
// Concurrency: invoker-only.
//
// Returns 1 when a wait is consumed, else 0.
func (a *Agent) serviceControlWork() int {
	if a.serviceAckFailure != nil || len(a.serviceAckWaits) == 0 {
		return 0
	}
	head := a.serviceAckWaits[0]
	ok, err := a.hasReachedServiceAcks(head)
	if err != nil {
		a.serviceAckFailure = err
		return 0
	}
	if !ok {
		return 0
	}
	a.removeServiceAckHead()
	a.serviceAckWaits = a.serviceAckWaits[1:]
	if a.replayJoinWaitsAhead > 0 {
		a.replayJoinWaitsAhead--
	}
	if head == a.snapshotWaitPosition {
		a.snapshotWaitPosition = 0
	}
	a.dropAbandonedSnapshotLeftovers(head)
	return 1
}

// retryPendingJoinLog offers a stored retriable JoinLog publish.
//
// DoWork runs this after the election or steady branch. PrepareForNewLeadership
// clears pending first on a new election, so a superseded unpublished JoinLog
// does not reach services.
//
// Behaviour: a retriable outcome keeps the same pending fields. Does not
// allocate a new copy.
//
// Concurrency: invoker-only.
//
// Returns 1 on a successful publish, else 0.
func (a *Agent) retryPendingJoinLog() int {
	if a.pendingJoinLog == nil {
		return 0
	}
	return a.publishJoinLog(a.pendingJoinLog)
}

// publishOrQueueJoinLog publishes JoinLog or stores a pending retry.
//
// Behaviour: copies fields onto the heap, then offers. The caller may reuse the
// LogChannel slice after the call.
//
// Concurrency: invoker-only.
func (a *Agent) publishOrQueueJoinLog(fields codecs.JoinLogFields) int {
	return a.publishJoinLog(copyJoinLogFields(fields))
}

// publishJoinLog offers fields and applies the outcome to waits and pending.
//
// Behaviour:
//  1. Nil proxy: return 0. No wait. No failure mark.
//  2. Success (position > 0): clear pendingJoinLog and return 1. When
//     serviceAckFailure is nil, append LogPosition to serviceAckWaits. When
//     serviceAckFailure is marked, the frame still publishes and no wait is
//     queued (waits would never drain).
//  3. Retriable offer code: store fields as pendingJoinLog (replace); no wait;
//     return 0.
//  4. Terminal code: mark servicePublishFailure once; clear pending; return 0.
//
// Concurrency: invoker-only.
//
// Ownership: on a retriable outcome the agent stores fields as pendingJoinLog.
// The caller must not reuse the pointer after the call.
//
// Returns 1 on a successful publish, else 0.
func (a *Agent) publishJoinLog(fields *codecs.JoinLogFields) int {
	if a.serviceProxy == nil {
		return 0
	}
	pos := a.serviceProxy.PublishJoinLog(*fields)
	if pos > 0 {
		if a.serviceAckFailure == nil {
			a.serviceAckWaits = append(a.serviceAckWaits, fields.LogPosition)
		}
		a.pendingJoinLog = nil
		a.markReplayJoinPublished()
		return 1
	}
	if aeron.IsOfferRetriable(pos) {
		a.pendingJoinLog = fields
		return 0
	}
	if a.servicePublishFailure == nil {
		a.servicePublishFailure = fmt.Errorf(
			"consensus: service control publish failed: code=%d",
			pos,
		)
	}
	a.pendingJoinLog = nil
	if a.replayJoinWaitsAhead == replayJoinUnpublished {
		a.replayJoinWaitsAhead = 0
	}
	return 0
}

// publishLeaderJoinLog builds leader JoinLog fields and publishes or queues.
//
// Non-IPC log channels get the spy prefix so services subscribe without
// competing as receivers on the leader publication.
func (a *Agent) publishLeaderJoinLog(
	logPosition int64,
	logSessionID int32,
	isStartup bool,
) {
	a.publishOrQueueJoinLog(codecs.JoinLogFields{
		LogPosition:    logPosition,
		MaxLogPosition: math.MaxInt64,
		MemberID:       a.thisMemberID,
		LogSessionID:   logSessionID,
		LogStreamID:    a.localLogStreamID,
		IsStartup:      booleanTypeFromBool(isStartup),
		Role:           RoleLeader.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(spyPrefixLogChannel(a.localLogChannel)),
	})
}

// publishFollowerJoinLog builds follower JoinLog fields and publishes or queues.
//
// Uses params channel unchanged (no spy prefix).
func (a *Agent) publishFollowerJoinLog(params FollowerLogJoinParams) {
	a.publishOrQueueJoinLog(codecs.JoinLogFields{
		LogPosition:    params.LogJoinPosition,
		MaxLogPosition: math.MaxInt64,
		MemberID:       a.thisMemberID,
		LogSessionID:   params.LogSessionID,
		LogStreamID:    params.LogStreamID,
		IsStartup:      booleanTypeFromBool(params.IsLeaderStartup),
		Role:           RoleFollower.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(params.LogChannel),
	})
}

// spyPrefixLogChannel returns channel unchanged for IPC media, else with the
// spy prefix applied.
func spyPrefixLogChannel(channel string) string {
	if isIPCChannel(channel) {
		return channel
	}
	return spyChannelPrefix + channel
}

// isIPCChannel reports whether channel uses IPC media (exact or with query).
func isIPCChannel(channel string) bool {
	media, _, _ := strings.Cut(channel, "?")
	return media == ipcChannelMedia
}

// booleanTypeFromBool maps a Go bool to the wire BooleanType.
func booleanTypeFromBool(v bool) clusterwire.BooleanType {
	if v {
		return clusterwire.BooleanType_TRUE
	}
	return clusterwire.BooleanType_FALSE
}

// copyJoinLogFields returns a heap copy of fields. Clones LogChannel bytes.
func copyJoinLogFields(fields codecs.JoinLogFields) *codecs.JoinLogFields {
	copied := fields
	copied.LogChannel = bytes.Clone(fields.LogChannel)
	return &copied
}

// pollServiceControl drains one batch of service control frames.
//
// Nil serviceIngress returns 0.
func (a *Agent) pollServiceControl() int {
	if a.serviceIngress == nil {
		return 0
	}
	return a.serviceIngress.Poll(
		defaultServiceControlFragmentLimit,
		a.serviceIngressHandler,
	)
}

// handleServiceControlFragment routes one service control frame.
//
// ServiceAck offers onto the tracker. ScheduleTimer and CancelTimer update the
// local timer wheel. Unknown templates and undecodable bytes skip without
// error.
func (a *Agent) handleServiceControlFragment(buf []byte, offset int) {
	templateID, ok := consensusMessageTemplateID(buf, offset)
	if !ok {
		return
	}
	switch templateID {
	case clusterwire.ServiceAckSbeTemplateID:
		if fields, ok := codecs.TryDecodeServiceAckAt(buf, offset); ok {
			a.handleServiceAck(fields)
		}
	case clusterwire.ScheduleTimerSbeTemplateID:
		if fields, ok := codecs.TryDecodeScheduleTimerAt(buf, offset); ok {
			a.handleScheduleTimer(fields)
		}
	case clusterwire.CancelTimerSbeTemplateID:
		if fields, ok := codecs.TryDecodeCancelTimerAt(buf, offset); ok {
			a.handleCancelTimer(fields)
		}
	}
}

// handleServiceAck offers one decoded ServiceAck, then runs the receive-time
// sequence check.
//
// Behaviour:
//   - A frame at a retired snapshot position drops before the offer when
//     isAbandonedSnapshotAck reports true. The drop clears the owed flag of
//     the sending service at that position, then notes the dropped AckID so
//     serviceAckID can skip that step.
//   - A refused offer (service id out of range) drops the frame and runs no
//     check.
//   - A successful offer calls afterOfferSequenceError for the expected
//     serviceAckID step. The check uses the first queued entry for that step,
//     not the incoming frame position. Multiple serviceAckID steps may sit in
//     one FIFO behind that first entry.
//   - An empty lower-id queue stops the walk. Later queues are not examined.
//     No mark in that case.
//   - ErrServiceAckOutOfSequence marks serviceAckFailure once. The first cause
//     is kept. Later mismatches do not replace the cause.
//
// Timestamp is decoded and not stored. Wait paths keep exact (ackID,
// logPosition) match via hasReachedServiceAcks at the wait position.
func (a *Agent) handleServiceAck(fields codecs.ServiceAckFields) {
	if a.isAbandonedSnapshotAck(fields.ServiceID, fields.LogPosition) {
		a.clearOwedSnapshotAck(fields.ServiceID, fields.LogPosition)
		a.noteAbandonedSnapshotAckID(fields.ServiceID, fields.AckID)
		a.advancePastAbandonedSnapshotAcks()
		return
	}
	if !a.serviceAcks.offer(fields.ServiceID, serviceAck{
		ackID:       fields.AckID,
		logPosition: fields.LogPosition,
		relevantID:  fields.RelevantID,
	}) {
		return
	}
	err := a.serviceAcks.afterOfferSequenceError(a.serviceAckID)
	if err != nil && a.serviceAckFailure == nil {
		a.serviceAckFailure = err
	}
}
