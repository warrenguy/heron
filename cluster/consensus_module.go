package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
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
}

// ConsensusModule is one assembled consensus member: transport client, archive
// control client, durable recovery state, consensus publication map, consensus
// subscription, cluster log streams, agent, host adapter, and election.
//
// Open with [OpenConsensusModule]. The module owns every handle it opened.
//
// The module closes every handle it owns. A transport client supplied through
// Config.Aeron stays the caller's. The module does not own that client.
//
// Concurrency: invoker-only. Every method must run on the duty cycle goroutine
// in this order: aeron.Client.DoWork, then archive.Client.Poll, then
// consensus.Agent.DoWork. Exception: [ConsensusModule.CommitPosition] reads the
// mode-1 *atomic.Int64 and is safe from any goroutine.
//
// Lifetime: the module outlives the latest election handle. A settled member
// may install a further election from the open-time election factory; each
// re-install of *election.Election updates m.election. Close clears that
// factory before it clears the runner. Call Close in caller teardown after the
// duty cycle has stopped.
//
// Do not copy. Most fields are pointers. A value copy shares the same handles.
//
// Both values look like owners responsible for Close.
type ConsensusModule struct {
	topology   *Topology
	client     *aeron.Client
	ownsClient bool

	// archiveClient is the module-owned archive control client.
	//
	// Close closes the archive client before the transport client.
	//
	// The archive control publication and subscription belong to that transport client.
	archiveClient *archive.Client

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
	logRecording *logRecording
}

// OpenConsensusModule assembles one consensus member and returns it with every
// transport handle and agent dependency bound before the first duty tick.
//
// ctx bounds the transport connect, the archive control connect, and the
// recording-extent lookups the recovery plan makes.
//
// The log recording control keeps Open's context values and detaches cancel
// (context.WithoutCancel). Cancel after Open returns does not fail later
// start or stop awaits. ControlResponseTimeout bounds those awaits.
//
// When Config.Aeron is set, ctx still bounds the archive control connect.
//
// Open order:
//  1. topology
//  2. transport client
//  3. archive control client
//  4. recording-log archive lister
//  5. recovery
//  6. agent
//  7. log recording control
//  8. consensus publications
//  9. consensus subscription
//  10. cluster log streams
//  11. host adapter
//  12. election
//  13. election factory
//
// Recovery plans from the recording log.
//
// A recording log that carries term entries needs a connected archive lister
// to resolve extents.
//
// Assembly notes:
//   - transport and archive control connect before recovery
//   - log recording control is wired after the agent and before the log streams
//     so no recording starts at Open
//   - cluster log publication and subscription open before return so the first
//     duty tick can win leadership and append a leadership-term event
//
// Returns an error when:
//   - Config fails Validate
//   - the transport client cannot connect
//   - the archive control client cannot connect
//   - recovery fails
//   - consensus publications, consensus subscription, or log streams fail to open
//   - the transport client has no counters reader
//
// Open blocks on the archive control connect, bounded by
// archive.DefaultConnectTimeout and by ctx. A process that opens several members
// in sequence services no earlier member's transport client during that block.
//
// Open every member's Archive before opening any member. Keep
// Config.AeronConfig.InterServiceTimeout above the total connect cost.
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

	archiveClient, err := archive.Connect(ctx, archiveConnectConfig(cfg, module.client))
	if err != nil {
		return nil, fmt.Errorf("cluster: connect archive client: %w", err)
	}
	module.archiveClient = archiveClient

	lister := archive.NewLister(ctx, module.archiveClient)

	recovery, err := OpenRecovery(RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	if err != nil {
		return nil, err
	}
	module.recovery = recovery
	module.coldStart = recovery.IsColdStart()
	electionCfg := recovery.ElectionConfig()

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
		LeaderHeartbeatTimeoutNs:  electionCfg.LeaderHeartbeatTimeoutNs,
		AppVersion:                electionCfg.AppVersion,
	})

	// Wire the recovered log recording id so term coherency can append. Skip
	// the null sentinel: a cold start has no log term to adopt.
	if id := recovery.Plan().TermRecordingID(); id != recordinglog.NullRecordingID {
		module.agent.SetLogRecordingID(id)
	}

	// Wire archive log recording control before log streams open.
	//
	// Nothing starts a recording at Open.
	//
	// Election join paths call StartLogRecording.
	module.logRecording = newLogRecording(
		ctx,
		module.archiveClient,
		topology.LogChannel(),
		topology.LogStreamID(),
		recovery.RecordingLog(),
	)
	module.agent.SetLogRecordingControl(module.logRecording)

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

	counters, err := module.client.CountersReader()
	if err != nil {
		return nil, fmt.Errorf("cluster: counters reader for append rank: %w", err)
	}

	streams, err := OpenLogStreams(LogStreamsDeps{
		Adder:        module.client,
		Topology:     topology,
		Agent:        module.agent,
		LogPublisher: module.logPublisher,
		LogAdapter:   module.logAdapter,
		AppendSource: consensus.NewRecordingPosAppendPosition(module.agent, counters),
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
		EpochClock:               epochClock,
		RecordingLogLookup:       recovery.RecordingLog(),
		AppVersion:               electionCfg.AppVersion,
	})
	module.agent.SetPositions(module.host)
	module.agent.SetEgress(module.host)

	// Never nil: a nil randomiser gives every member a zero nomination
	// back-off, and members that all nominate on the same tick split the
	// ballot and never elect.
	electionDeps := election.ElectionDeps{
		Handler:                   module.agent,
		Host:                      module.host,
		Publisher:                 publisher,
		NanoClock:                 nanoClock,
		EpochClock:                epochClock,
		NominationRand:            rand.Float64,
		NodeState:                 recovery.NodeState(),
		Members:                   topology.Members(),
		ThisMember:                topology.ThisMember(),
		RecordingLog:              recovery.RecordingLog(),
		RecordingLogLookup:        recovery.RecordingLog(),
		RecordingLogFileSyncLevel: cfg.RecordingLogFileSyncLevel,
	}
	module.election = election.NewElection(electionCfg, electionDeps)
	module.agent.SetElection(module.election)
	module.agent.SetElectionFactory(trackModuleElection(
		module,
		newElectionFactory(
			electionCfg,
			electionDeps,
			recovery.Plan(),
		),
	))

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

// Close tears the member down in reverse open order:
//  1. agent bindings
//  2. best-effort log recording stop
//  3. cluster log streams
//  4. consensus subscription
//  5. consensus publications
//  6. archive control client
//  7. owned transport client
//  8. recovery
//
// The archive control client closes before the transport client. The archive
// control publication and subscription belong to that transport client.
//
// Idempotent. Safe on a nil receiver. Usable in a defer taken before the error
// check.
//
// A transport client supplied through Config.Aeron stays open.
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
	// source are cleared by the streams Close. Clear the election factory
	// before the runner so a post-Close duty tick cannot re-install against a
	// torn-down recovery.
	if m.agent != nil {
		m.agent.SetElectionFactory(nil)
		m.agent.SetElection(nil)
		m.agent.SetPositions(nil)
		m.agent.SetEgress(nil)
	}
	// Best-effort stop before streams and archive client close.
	//
	// Fail-closed election paths own stop errors.
	//
	// Close still releases transport handles.
	if m.logRecording != nil {
		if _, stopErr := m.logRecording.StopLogRecording(); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		m.logRecording = nil
	}
	if m.agent != nil {
		m.agent.SetLogRecordingControl(nil)
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
	if m.archiveClient != nil {
		err = errors.Join(err, m.archiveClient.Close())
		m.archiveClient = nil
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
// Pair the client with [ConsensusModule.ArchiveClient] and
// [ConsensusModule.Agent] to drive one duty cycle: transport DoWork, then
// archive Poll, then agent DoWork.
func (m *ConsensusModule) Client() *aeron.Client { return m.client }

// ArchiveClient returns the archive control client, or nil after Close.
//
// Pass the client to RunnerDeps.Archive so the duty cycle demuxes control
// responses and recording signals. A multi-member duty cycle must call Poll on
// the client after that member's transport DoWork and before that member's
// agent DoWork.
//
// Concurrency: invoker-only.
//
// Ownership: the module owns the client. Callers must not close the client.
func (m *ConsensusModule) ArchiveClient() *archive.Client { return m.archiveClient }

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

// ElectionComplete reports whether the latest installed election completed.
//
// Behaviour:
//   - reads m.election (*election.Election most recently installed at open or
//     settled re-install)
//   - true when that machine is Closed
//   - false when m.election is nil, or the latest machine has not closed
//   - each settled re-install of *election.Election replaces m.election, so
//     this tracks the live machine again after re-entry
//   - Close does not clear m.election; a member closed mid election keeps
//     reporting false until that machine reaches Closed
//   - after Close on a settled member, reports whether the last installed
//     machine is Closed (post-Close dumps)
//
// Same handle as [ConsensusModule.ElectionState]. For install count and
// trigger, use [consensus.Agent.ElectionCount] and
// [consensus.Agent.LastElectionTrigger]. [consensus.Agent.Election] is non-nil
// only while a runner is live on the duty path.
func (m *ConsensusModule) ElectionComplete() bool {
	return m.election != nil && m.election.State() == election.ElectionStateClosed
}

// ElectionState returns the latest installed election state machine's state.
//
// Behaviour:
//   - same handle as [ConsensusModule.ElectionComplete]
//   - each settled re-install of *election.Election replaces m.election; this
//     tracks the live *election.Election after re-entry
//   - after Close, reports the state the latest machine last reached
//   - Close does not release m.election; the election holds no OS resource
//   - retaining m.election is what makes this accessor answer after transport
//     handles are gone
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
