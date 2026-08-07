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
// Behaviour: the agent calls the handler only on a steady-state leader
// (election settled, role leader). Var field slices in fields are copies. The
// handler may retain them. nowNs is the duty-cycle clock of the poll that
// delivered the frame. Fragmented ingress frames never reach this handler. The
// subscription adapter skips them without reassembly. Handlers must not log
// EncodedCredentials raw.
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
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
//
// Nil is allowed. A steady-state leader drops connects until rewired.
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
	// Only a settled leader with a wired handler decodes and accepts. Followers
	// learn sessions from the log. Non-leaders and nil-handler leaders return
	// before header peek or var-field clone so connect floods stay cheap. There
	// is no redirect path.
	if a.election != nil || a.role != RoleLeader || a.sessionConnectHandler == nil {
		return
	}
	templateID, ok := consensusMessageTemplateID(buf, offset)
	if !ok || templateID != clusterwire.SessionConnectRequestSbeTemplateID {
		return
	}
	fields, ok := codecs.TryDecodeSessionConnectRequestAt(buf, offset)
	if !ok {
		return
	}
	a.sessionConnectHandler.HandleSessionConnect(fields, a.clientIngressPollNowNs)
}
