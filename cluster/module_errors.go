package cluster

import (
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

const moduleErrorCounterLabel = "Cluster Errors - clusterId=0"

func addModuleErrorCounter(client *aeron.Client) (*aeron.Counter, error) {
	return addClusterCounter(
		client,
		consensus.ClusterConsensusModuleErrorCountCounterTypeID,
		moduleErrorCounterLabel,
		"module error",
	)
}

// moduleErrorHandler counts each module error on the type 212 counter. It
// then passes the error to Config.ErrorHandler.
//
// Concurrency: invoker-only.
type moduleErrorHandler struct {
	values  atomicbuffer.AtomicBuffer
	offset  int
	handler func(error)
	closed  bool
}

// Returns an error when counterID is out of range for counters.
func newModuleErrorHandler(
	counters *cnc.CountersReader,
	counterID int32,
	handler func(error),
) (*moduleErrorHandler, error) {
	if _, err := counters.CounterValue(counterID); err != nil {
		return nil, fmt.Errorf("cluster: module error counter: %w", err)
	}
	return &moduleErrorHandler{
		values:  counters.CounterValuesBuffer(),
		offset:  cnc.CounterOffset(counterID),
		handler: handler,
	}, nil
}

func (h *moduleErrorHandler) handleError(err error) {
	if err == nil || h.closed {
		return
	}
	h.values.AddInt64(h.offset, 1)
	if h.handler != nil {
		h.handler(err)
	}
}

// close stops later reports and leaves the counter open. Idempotent.
func (h *moduleErrorHandler) close() {
	h.closed = true
}

// moduleAgentConfig builds the consensus agent config for one opened member.
func moduleAgentConfig(
	cfg Config,
	electionCfg election.ElectionConfig,
	commit *atomic.Int64,
	errorHandler func(error),
) consensus.AgentConfig {
	return consensus.AgentConfig{
		Commit:                    commit,
		ThisMemberID:              cfg.MemberID,
		LeaderMemberID:            election.NullMemberID,
		LeaderHeartbeatIntervalNs: electionCfg.LeaderHeartbeatIntervalNs,
		LeaderHeartbeatTimeoutNs:  electionCfg.LeaderHeartbeatTimeoutNs,
		SessionTimeoutNs:          cfg.SessionTimeoutNs,
		AppVersion:                electionCfg.AppVersion,
		Authenticator:             cfg.Authenticator,
		ServiceCount:              cfg.ServiceCount,
		ErrorHandler:              errorHandler,
	}
}
