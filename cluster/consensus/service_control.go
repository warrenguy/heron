package consensus

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// defaultServiceControlFragmentLimit is the max fragments drained per
// [Agent.pollServiceControl] poll. Shares the consensus ingress batch size.
const defaultServiceControlFragmentLimit = defaultConsensusIngressFragmentLimit

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
// ServiceAck offers onto the tracker. Unknown templates and undecodable bytes
// skip without error.
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
	}
}

// handleServiceAck offers one decoded ServiceAck, then runs the receive-time
// sequence check.
//
// Behaviour:
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
