package cluster

import (
	"context"
	"fmt"
	"sync/atomic"

	"luas.fast/aeron"
	"luas.fast/aeron/cnc"
	"luas.fast/archive"
	"luas.fast/atomicbuffer"
	"luas.fast/cluster/consensus"
	"luas.fast/cluster/election"
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
// then passes the error to Config.ErrorHandler. An archive STORAGE_SPACE error
// then ends the member.
//
// Concurrency: call from the goroutine that runs consensus.Agent.DoWork. Not
// safe from other goroutines.
type moduleErrorHandler struct {
	values    atomicbuffer.AtomicBuffer
	offset    int
	handler   func(error)
	terminate func(error)
	closed    bool
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
	if h.terminate != nil && isArchiveStorageSpace(err) {
		h.terminate(err)
	}
}

func isArchiveStorageSpace(err error) bool {
	code, ok := archive.ExceptionCode(err)
	return ok && code == archive.ErrorStorageSpace
}

// close stops later reports and leaves the counter open. Idempotent.
func (h *moduleErrorHandler) close() {
	h.closed = true
}

type archiveFailureSource interface {
	consensus.ArchiveConnection
	SetErrorResponseHandler(handler func(error))
}

// wireArchiveFailure makes one opened member end on an archive STORAGE_SPACE
// error or a lost archive connection.
func wireArchiveFailure(
	errorHandler *moduleErrorHandler,
	agent *consensus.Agent,
	source archiveFailureSource,
) {
	errorHandler.terminate = agent.TerminateOnError
	agent.SetArchiveConnection(source)
	source.SetErrorResponseHandler(errorHandler.handleError)
}

func moduleLogReplayDeps(
	ctx context.Context,
	module *ConsensusModule,
	imageAwaitNs int64,
	errorHandler func(error),
) logReplayDeps {
	return logReplayDeps{
		Ctx:            ctx,
		Control:        module.archiveClient,
		Adder:          module.client,
		Agent:          module.agent,
		ReplayChannel:  module.topology.ReplayChannel(),
		ReplayStreamID: module.topology.ReplayStreamID(),
		ImageAwaitNs:   imageAwaitNs,
		ErrorHandler:   errorHandler,
	}
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
		TerminationTimeoutNs:      cfg.TerminationTimeoutNs,
		AppVersion:                electionCfg.AppVersion,
		Authenticator:             cfg.Authenticator,
		Authoriser:                cfg.Authoriser,
		ServiceCount:              cfg.ServiceCount,
		ErrorHandler:              errorHandler,
	}
}
