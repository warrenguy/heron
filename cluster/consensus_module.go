package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/idlestrategy"
)

// ConsensusModuleDeps are the collaborators [OpenConsensusModule] needs.
//
// Named for [RecoveryDeps], [ConsensusPublicationsDeps], and [LogStreamsDeps],
// which carry injected collaborators the same way.
type ConsensusModuleDeps struct {
	// Config is the member configuration. OpenConsensusModule normalizes and
	// validates it. OpenConsensusModule does not retain Config.
	Config Config

	// ArchiveLister resolves recording extents for a recording log that carries
	// term entries. Production passes archive.NewLister over a connected
	// control client. Unit tests pass a map-backed fake.
	//
	// Nil is allowed when the recording log is empty. Nil with a non-empty
	// recording log is refused.
	//
	// OpenConsensusModule connects no archive client. The caller owns the
	// client, its context, and Close.
	ArchiveLister recordinglog.ArchiveLister
}

// ConsensusModule is one assembled consensus member: transport client, durable
// recovery state, consensus publication map, consensus subscription, cluster log
// streams, agent, host adapter, and election.
//
// Open with [OpenConsensusModule]. The module owns every handle it opened and
// closes all of them. A transport client supplied through Config.Aeron stays the
// caller's.
//
// Concurrency: invoker-only. Every method must run on the duty-cycle goroutine
// (aeron.Client.DoWork then consensus.Agent.DoWork). Exception:
// [ConsensusModule.CommitPosition] reads the mode-1 *atomic.Int64 and is safe
// from any goroutine.
//
// Lifetime: outlives the election. Close belongs in caller teardown after the
// duty cycle has stopped.
//
// Do not copy. Most fields are pointers. A value copy shares the same handles;
// both values look like owners responsible for Close.
type ConsensusModule struct {
	topology   *Topology
	client     *aeron.Client
	ownsClient bool

	recovery  *Recovery
	coldStart bool
	agent     *consensus.Agent
	pubs      *ConsensusPublications
	sub       *ConsensusSubscription
	streams   *LogStreams
	host      *election.HostAdapter
	election  *election.Election
	idle      idlestrategy.IdleStrategy
	nanoClock func() int64

	logPublisher *consensus.LogPublisher
	logAdapter   *consensus.LogAdapter
}

// OpenConsensusModule assembles one consensus member and returns it with every
// transport handle and agent dependency bound before the first duty tick.
//
// ctx bounds the transport connect only. Open does not retain ctx. Cancel after
// Open returns has no effect. When Config.Aeron is set, ctx is unused.
//
// Open order:
//  1. topology
//  2. recovery
//  3. transport client
//  4. agent
//  5. consensus publications
//  6. consensus subscription
//  7. cluster log streams
//  8. host adapter
//  9. election
//
// Recovery runs before connect so failures that need no media driver skip
// connect cost. The cluster log publication and subscription are open before
// return so the first duty tick can win leadership and append a leadership-term
// event.
//
// Errors:
//   - Config fails Validate
//   - recovery fails
//   - transport client cannot connect
//   - consensus publications, consensus subscription, or log streams fail to open
//
// On error every handle opened so far is closed, the cluster directory lock is
// released, and the returned value is nil.
func OpenConsensusModule(
	ctx context.Context,
	deps ConsensusModuleDeps,
) (*ConsensusModule, error) {
	cfg := deps.Config.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	topology, err := NewTopology(cfg)
	if err != nil {
		return nil, err
	}

	module := &ConsensusModule{
		topology: topology,
		idle:     cfg.Idle,
	}

	succeeded := false
	defer func() {
		if !succeeded {
			_ = module.Close()
		}
	}()

	recovery, err := OpenRecovery(RecoveryDeps{
		Config:        cfg,
		ArchiveLister: deps.ArchiveLister,
	})
	if err != nil {
		return nil, err
	}
	module.recovery = recovery
	module.coldStart = recovery.IsColdStart()
	electionCfg := recovery.ElectionConfig()

	if cfg.Aeron != nil {
		module.client = cfg.Aeron
	} else {
		client, err := aeron.Connect(ctx, cfg.AeronConfig)
		if err != nil {
			return nil, fmt.Errorf("cluster: connect aeron client: %w", err)
		}
		module.client = client
		module.ownsClient = true
	}

	nanoClock, epochClock := moduleClocks(cfg)
	module.nanoClock = nanoClock

	// Seed commit from the recovery plan. ProposeMaxCommit is monotonic only,
	// so open is the sole absolute write of a recovered position.
	commit := new(atomic.Int64)
	if seed := recovery.Plan().CommittedLogPosition; seed > 0 {
		commit.Store(seed)
	}

	module.logPublisher = consensus.NewLogPublisher()
	module.logAdapter = consensus.NewLogAdapter(consensus.DefaultLogFragmentLimit)

	module.agent = consensus.NewAgent(consensus.AgentConfig{
		Commit:                    commit,
		ThisMemberID:              cfg.MemberID,
		LeaderMemberID:            election.NullMemberID,
		LeaderHeartbeatIntervalNs: electionCfg.LeaderHeartbeatIntervalNs,
		AppVersion:                electionCfg.AppVersion,
	})

	// Wire the recovered log recording id so term coherency can append. Skip
	// the null sentinel: a cold start has no log term to adopt.
	if id := recovery.Plan().TermRecordingID(); id != recordinglog.NullRecordingID {
		module.agent.SetLogRecordingID(id)
	}

	publisher := election.NewAeronPublisher()

	pubs, err := OpenConsensusPublications(ConsensusPublicationsDeps{
		Adder:     module.client,
		Topology:  topology,
		Publisher: publisher,
	})
	if err != nil {
		return nil, err
	}
	module.pubs = pubs

	sub, err := OpenConsensusSubscription(ConsensusSubscriptionDeps{
		Adder:    module.client,
		Topology: topology,
		Agent:    module.agent,
	})
	if err != nil {
		return nil, err
	}
	module.sub = sub

	streams, err := OpenLogStreams(LogStreamsDeps{
		Adder:        module.client,
		Topology:     topology,
		Agent:        module.agent,
		LogPublisher: module.logPublisher,
		LogAdapter:   module.logAdapter,
	})
	if err != nil {
		return nil, err
	}
	module.streams = streams

	module.host = election.NewHostAdapter(election.HostAdapterConfig{
		Agent:                    module.agent,
		Publisher:                publisher,
		Members:                  topology.Members(),
		ThisMember:               topology.ThisMember(),
		LeaderHeartbeatTimeoutNs: electionCfg.LeaderHeartbeatTimeoutNs,
		NanoClock:                nanoClock,
		LeaderMemberID:           election.NullMemberID,
	})
	module.agent.SetPositions(module.host)
	module.agent.SetEgress(module.host)

	module.election = election.NewElection(electionCfg, election.ElectionDeps{
		Handler:    module.agent,
		Host:       module.host,
		Publisher:  publisher,
		NanoClock:  nanoClock,
		EpochClock: epochClock,

		// Never nil: a nil randomiser gives every member a zero nomination
		// back-off, and members that all nominate on the same tick split the
		// ballot and never elect.
		NominationRand: rand.Float64,

		NodeState:                 recovery.NodeState(),
		Members:                   topology.Members(),
		ThisMember:                topology.ThisMember(),
		RecordingLog:              recovery.RecordingLog(),
		RecordingLogLookup:        recovery.RecordingLog(),
		RecordingLogFileSyncLevel: cfg.RecordingLogFileSyncLevel,
	})
	module.agent.SetElection(module.election)

	succeeded = true
	return module, nil
}

func moduleClocks(cfg Config) (nanoClock, epochClock func() int64) {
	nanoClock = cfg.AeronConfig.NanoClock
	if nanoClock == nil {
		nanoClock = func() int64 { return time.Now().UnixNano() }
	}
	epochClock = cfg.AeronConfig.EpochClock
	if epochClock == nil {
		epochClock = func() int64 { return time.Now().UnixMilli() }
	}
	return nanoClock, epochClock
}

// Close tears the member down in exact reverse open order: agent bindings,
// cluster log streams, consensus subscription, consensus publications, the
// transport client when the module connected it, then recovery.
//
// Idempotent, and safe on a nil receiver, so it is usable in a defer taken
// before the error check.
//
// A transport client supplied through Config.Aeron is left open.
//
// Returns the joined close errors, or nil.
func (m *ConsensusModule) Close() error {
	if m == nil {
		return nil
	}

	var err error

	// Clear agent bindings before closing the handles they reference: positions
	// and egress hold the host adapter and its shared publisher, which
	// publications close next. Log publisher, adapter, append source, and join
	// source are cleared by the streams Close.
	if m.agent != nil {
		m.agent.SetElection(nil)
		m.agent.SetPositions(nil)
		m.agent.SetEgress(nil)
	}
	if m.streams != nil {
		err = errors.Join(err, m.streams.Close())
		m.streams = nil
	}
	if m.sub != nil {
		err = errors.Join(err, m.sub.Close())
		m.sub = nil
	}
	if m.pubs != nil {
		err = errors.Join(err, m.pubs.Close())
		m.pubs = nil
	}
	if m.client != nil && m.ownsClient {
		err = errors.Join(err, m.client.Close())
	}
	m.client = nil
	if m.recovery != nil {
		err = errors.Join(err, m.recovery.Close())
		m.recovery = nil
	}
	return err
}

// Client returns the transport client, or nil after Close.
//
// Pair it with [ConsensusModule.Agent] to drive one duty cycle: the transport
// client first, then the agent.
func (m *ConsensusModule) Client() *aeron.Client { return m.client }

// Agent returns the consensus agent. It stays non-nil after Close, with every
// binding cleared, so a caller can assert teardown.
func (m *ConsensusModule) Agent() *consensus.Agent { return m.agent }

// Topology returns the resolved channel and stream map.
func (m *ConsensusModule) Topology() *Topology { return m.topology }

// LogStreams returns the cluster log publication and subscription pair, or nil
// after Close.
func (m *ConsensusModule) LogStreams() *LogStreams { return m.streams }

// LogPublisher returns the cluster log publisher. It stays non-nil after Close,
// with its publication unbound.
func (m *ConsensusModule) LogPublisher() *consensus.LogPublisher {
	return m.logPublisher
}

// LogAdapter returns the cluster log adapter. It stays non-nil after Close.
//
// A follower holds a bound Image after it joins the live log; a leader holds
// none.
func (m *ConsensusModule) LogAdapter() *consensus.LogAdapter {
	return m.logAdapter
}

// RecordingLog returns the open recording log, or nil after Close.
func (m *ConsensusModule) RecordingLog() *recordinglog.Log {
	if m.recovery == nil {
		return nil
	}
	return m.recovery.RecordingLog()
}

// IdleStrategy returns the configured idle strategy. It is never nil: Normalize
// defaults it. The module never calls it; the duty-cycle owner does.
func (m *ConsensusModule) IdleStrategy() idlestrategy.IdleStrategy {
	return m.idle
}

// NanoClock returns the clock the election and the host adapter share. A duty
// cycle must pass this clock's value to Agent.DoWork so agent deadlines and
// election deadlines agree.
func (m *ConsensusModule) NanoClock() func() int64 { return m.nanoClock }

// Role returns the current cluster node role.
func (m *ConsensusModule) Role() consensus.Role { return m.agent.Role() }

// IsLeader reports whether this member currently holds leadership.
func (m *ConsensusModule) IsLeader() bool {
	return m.agent.Role() == consensus.RoleLeader
}

// LeadershipTermID returns the current leadership term.
func (m *ConsensusModule) LeadershipTermID() int64 {
	return m.agent.LeadershipTermID()
}

// LeaderMemberID returns the known leader's member id.
//
// On the leader this is -1, not the leader's own id: JoinLogAsLeader clears the
// field because a leader has no append target. Assert -1, never self.
func (m *ConsensusModule) LeaderMemberID() int32 {
	return m.agent.LeaderMemberID()
}

// ThisMemberID returns this member's cluster id.
func (m *ConsensusModule) ThisMemberID() int32 {
	return m.agent.ThisMemberID()
}

// CommitPosition returns the mode-1 local commit value.
//
// It is seeded at open from the recovery plan's committed log position and only
// ever rises: ProposeMaxCommit is monotonic.
//
// Concurrency: safe from any goroutine. It reads an *atomic.Int64.
func (m *ConsensusModule) CommitPosition() int64 {
	return m.agent.CommitPosition()
}

// NotifiedCommitPosition returns the follower-visible commit high-water.
//
// It advances only while this member is a follower, so a leader's value stays at
// whatever it held when leadership was won. Do not assert that it moves on a
// leader.
func (m *ConsensusModule) NotifiedCommitPosition() int64 {
	return m.agent.NotifiedCommitPosition()
}

// ElectionComplete reports whether the election completed.
//
// Derived from the retained election state machine: completion sets
// ElectionStateClosed in the same step that clears the agent's runner, and
// Close does not disturb the state, so a member closed mid-election keeps
// reporting false, which is the case a failure dump exists to catch.
func (m *ConsensusModule) ElectionComplete() bool {
	return m.election != nil && m.election.State() == election.ElectionStateClosed
}

// ElectionState returns the election state machine's current state.
//
// After Close it reports the state the machine last reached. Close does not
// release the election: it holds no OS resource, and retaining it is what makes
// this accessor answer truthfully once the transport handles are gone.
func (m *ConsensusModule) ElectionState() election.ElectionState {
	if m.election == nil {
		return election.ElectionStateInit
	}
	return m.election.State()
}

// IsColdStart reports whether recovery found no usable cluster history.
//
// Snapshotted at open, so it keeps its meaning after Close.
func (m *ConsensusModule) IsColdStart() bool { return m.coldStart }
