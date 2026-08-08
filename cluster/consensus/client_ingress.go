package consensus

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// defaultClientIngressFragmentLimit is the max fragments drained per
// [Agent.pollClientIngress] poll. Shares the consensus ingress batch size.
const defaultClientIngressFragmentLimit = defaultConsensusIngressFragmentLimit

// SessionConnectHandler receives one decoded SessionConnectRequest from the
// leader client ingress dispatch.
//
// Behaviour:
//   - The agent calls the handler only on a settled leader (election settled,
//     role leader).
//   - Nil runs the product session accept. A non-nil handler replaces the
//     product accept.
//   - Redirect on a settled non-leader is not behind this seam.
//   - Var field slices in fields are copies. The handler may retain them.
//   - nowNs is the duty-cycle clock of the poll that delivered the frame.
//   - Fragmented ingress frames never reach this handler. The subscription
//     adapter skips them without reassembly.
//   - Handlers must not log EncodedCredentials raw.
//
// Concurrency: invoker-only. Called from [Agent.DoWork].
type SessionConnectHandler interface {
	HandleSessionConnect(fields codecs.SessionConnectRequestFields, nowNs int64)
}

// SetClientIngress replaces the client ingress poller.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
//
// Nil is allowed. [Agent.pollClientIngress] returns 0 until rewired.
func (a *Agent) SetClientIngress(ingress ConsensusIngress) { a.clientIngress = ingress }

// ClientIngress returns the wired client [ConsensusIngress], or nil if unset.
func (a *Agent) ClientIngress() ConsensusIngress { return a.clientIngress }

// SetSessionConnectHandler wires or clears the [SessionConnectHandler].
//
// Behaviour: nil runs the product session accept. A non-nil handler replaces
// the product accept on the leader only. Redirect is not behind this seam.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSessionConnectHandler(handler SessionConnectHandler) {
	a.sessionConnectHandler = handler
}

// pollClientIngress drains one batch of unfragmented client ingress frames.
//
// Fragmented frames are skipped by the subscription adapter without
// reassembly.
func (a *Agent) pollClientIngress(nowNs int64) int {
	if a.clientIngress == nil {
		return 0
	}
	a.clientIngressPollNowNs = nowNs
	return a.clientIngress.Poll(defaultClientIngressFragmentLimit, a.clientIngressHandler)
}

func (a *Agent) handleClientIngressFragment(buf []byte, offset int) {
	// Election live: no settled leader to accept or redirect toward.
	if a.election != nil {
		return
	}
	// Settled non-leader: drop before header peek when a redirect cannot be
	// queued. Connect floods stay cheap. A non-leader's only outcome is a
	// REDIRECT enqueue. A full queue or nil adder would drop the event after
	// decode anyway.
	if a.role != RoleLeader {
		if a.leaderMemberID == nullMemberID || !a.canEnqueueEgressEvent() {
			return
		}
	}
	templateID, ok := consensusMessageTemplateID(buf, offset)
	if !ok || templateID != clusterwire.SessionConnectRequestSbeTemplateID {
		return
	}
	fields, ok := codecs.TryDecodeSessionConnectRequestAt(buf, offset)
	if !ok {
		return
	}
	a.handleSessionConnect(fields, a.clientIngressPollNowNs)
}

// handleSessionConnect dispatches a decoded connect. The caller checks the
// settled state and leader-hint gate before this call.
//
// Behaviour:
//   - A non-leader queues SessionEvent REDIRECT.
//   - On the leader, nil sessionConnectHandler runs acceptSessionConnect.
//   - A non-nil handler replaces the product accept.
func (a *Agent) handleSessionConnect(
	fields codecs.SessionConnectRequestFields,
	nowNs int64,
) {
	if a.role != RoleLeader {
		a.queueSessionRedirect(fields, nowNs)
		return
	}
	if a.sessionConnectHandler != nil {
		a.sessionConnectHandler.HandleSessionConnect(fields, nowNs)
		return
	}
	a.acceptSessionConnect(fields, nowNs)
}
