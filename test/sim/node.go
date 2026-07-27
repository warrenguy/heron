package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// DefaultThreeNodeMemberSpec is the embedded 3-node ParseMembers fixture for sim tests.
func DefaultThreeNodeMemberSpec() string { return defaultThreeNodeMemberSpec }

const defaultThreeNodeMemberSpec = "" +
	"0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|" +
	"1,localhost:9102,localhost:9103,localhost:9104,localhost:9105,localhost:9101|" +
	"2,localhost:9202,localhost:9203,localhost:9204,localhost:9205,localhost:9201|"

// DefaultSingleNodeMemberSpec is the embedded 1-node ParseMembers fixture for
// single-node election scenarios.
func DefaultSingleNodeMemberSpec() string { return defaultSingleNodeMemberSpec }

const defaultSingleNodeMemberSpec = "" +
	"0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|"

// Node wraps one Election with simulated clocks and network-routed publishes.
type Node struct {
	id        int32
	members   []election.Member
	election  *election.Election
	publisher *election.SbePublisher
	host      election.ElectionHost
	agent     *simAgentHandler
	clock     *NodeClock
	nodeState election.CandidateTermStore
	observer  election.StateObserver
	network   *Network

	// agentCM is the consensus Agent when UseReplicationHost; nil otherwise.
	agentCM *consensus.Agent

	// recordingLog is optional; set when Config.AttachRecordingLog is true.
	recordingLog *recordinglog.Log

	// crashSnap holds log identity for RestartNode; set only while crashed.
	crashSnap *crashSnap

	// Candidate-term tracking for testhooks safety (maxCandidateSeen only
	// checked after leave Init; initLoaded enables that check).
	initLoaded       bool
	maxCandidateSeen int64

	// receivedMsgKeys tracks inbound deliveries in order; sender order is
	// projected from .from for ReceivedFromOrderForTest / InboundCount.
	receivedMsgKeys []deliveredKey

	// publishGen increments on each attachLiveElection so post-restart Sent
	// indices do not collide with pre-crash deliveredKey identity.
	publishGen int

	// wireScan caches the incremental CommitPosition scan over publisher.Sent
	// for continuous commit safety; stale when gen differs from publishGen.
	wireScan *wireCommitScan
}

// wireCommitScan folds publisher.Sent CommitPosition rows into per-term
// aggregates. Sent is append-only per publisher generation, so rows before
// nextIndex never change; attachLiveElection bumps publishGen and the next
// advance rebuilds from an empty Sent.
type wireCommitScan struct {
	gen       int
	nextIndex int

	// highByTerm is the max LogPosition per leadership term.
	highByTerm map[int64]int64

	// leaderByTerm is the first LeaderMemberID per term; conflictByTerm the
	// first differing pair, kept for dual-authority reporting.
	leaderByTerm   map[int64]int32
	conflictByTerm map[int64][2]int32
}

// crashSnap holds election log identity restored on mid-run restart.
//
// Untagged so Node can hold the field; only testhooks code fills it.
//
// Candidate term stays in durable nodeState (not snapshotted here).
type crashSnap struct {
	leadershipTerm    int64
	logPosition       int64
	logLeadershipTerm int64
}

// Election returns the underlying election instance.
func (n *Node) Election() *election.Election { return n.election }

// Publisher returns the routed publisher for this node.
func (n *Node) Publisher() *election.SbePublisher { return n.publisher }

// Clock returns the per-node simulated clock.
func (n *Node) Clock() *NodeClock { return n.clock }

// InitLoadedForTest reports whether the node has left Init and candidate-term
// tracking is active (maxCandidateSeen seeded from the live term).
func (n *Node) InitLoadedForTest() bool { return n.initLoaded }

// MaxCandidateSeenForTest returns the highest candidate term observed since
// the node last left Init (when the mark was seeded).
func (n *Node) MaxCandidateSeenForTest() int64 { return n.maxCandidateSeen }

// Host returns the election host (*simElectionHost or *simReplicationHost).
func (n *Node) Host() election.ElectionHost { return n.host }

// AgentCM returns the consensus Agent when UseReplicationHost, else nil.
func (n *Node) AgentCM() *consensus.Agent { return n.agentCM }

// HostRole returns the role the election host last had set on it.
//
// Returns RoleFollower when host is nil (crashed node).
func (n *Node) HostRole() consensus.Role {
	if n.host == nil {
		return consensus.RoleFollower
	}
	return n.host.Role()
}

// SetHostRole seeds the election host role before the first tick.
//
// No-op when host is nil (crashed node).
func (n *Node) SetHostRole(role consensus.Role) {
	if n.host == nil {
		return
	}
	n.host.RoleSet(role)
}

// Tick runs one Election.DoWork cycle at nowNs. No-ops when election is nil.
func (n *Node) Tick(nowNs int64) int {
	if n.election == nil {
		return 0
	}
	n.clock.Set(nowNs)
	return n.election.DoWork(nowNs)
}

// ObserverStatesForTest returns state transitions recorded by the election observer.
func (n *Node) ObserverStatesForTest() []election.ElectionState {
	o, ok := n.observer.(stateHistory)
	if !ok {
		return nil
	}
	return o.StatesCopy()
}

// stateHistory is implemented by timeline and safety observers.
type stateHistory interface {
	StatesCopy() []election.ElectionState
}

// ReceivedFromOrderForTest returns inbound sender order for delivered messages.
func (n *Node) ReceivedFromOrderForTest() []int32 {
	out := make([]int32, len(n.receivedMsgKeys))
	for i, k := range n.receivedMsgKeys {
		out[i] = k.from
	}
	return out
}

// hasDelivered reports whether the message at the source's Sent index has been
// delivered to this node.
func (n *Node) hasDelivered(key deliveredKey) bool {
	return slices.Contains(n.receivedMsgKeys, key)
}

// Snapshot returns observable per-node state.
func (n *Node) Snapshot() NodeSnapshot {
	candidateTermID := election.NullValue
	ct, err := n.nodeState.CandidateTerm()
	if err == nil {
		candidateTermID = ct.CandidateTermID()
	}
	snap := NodeSnapshot{
		NodeID:          n.id,
		CandidateTermID: candidateTermID,
		InboundCount:    len(n.receivedMsgKeys),
	}
	if n.publisher != nil {
		snap.PublishedCount = len(n.publisher.Sent)
	}
	if n.agent != nil {
		snap.PrepareCalls = n.agent.prepareCalls
		snap.PrepareLogPosition = n.agent.prepareLogPosition
		snap.PrepareNowNs = n.agent.prepareNowNs
	}
	if n.election != nil {
		snap.ElectionState = n.election.State()
		snap.LeadershipTermID = n.election.LeadershipTermID()
		snap.LogPosition = n.election.LogPosition()
	}
	return snap
}

// ID returns the cluster member id for this node.
func (n *Node) ID() int32 { return n.id }

// RecordingLog returns the optional attached recording log, or nil.
func (n *Node) RecordingLog() *recordinglog.Log { return n.recordingLog }

// Close releases election, optional recording log, then durable node state.
//
// Order: nil election -> recordingLog.Close -> nodeState.Close.
func (n *Node) Close() error {
	n.election = nil
	var first error
	if n.recordingLog != nil {
		if err := n.recordingLog.Close(); err != nil && first == nil {
			first = err
		}
		n.recordingLog = nil
	}
	if n.nodeState != nil {
		if err := n.nodeState.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type nodeBuildConfig struct {
	id                 int32
	members            []election.Member
	electionCfg        election.ElectionConfig
	perNodeOverrides   func(id int32, cfg *election.ElectionConfig)
	nodeStateDir       string
	network            *Network
	sim                *ElectionSim
	attachRecordingLog bool
}

func newNode(cfg nodeBuildConfig) (*Node, error) {
	members := append([]election.Member(nil), cfg.members...)
	if _, err := memberPtrByID(members, cfg.id); err != nil {
		return nil, err
	}

	nodeDir := filepath.Join(cfg.nodeStateDir, fmt.Sprintf("node-%d", cfg.id))
	nodeState, err := openNodeStateDir(nodeDir)
	if err != nil {
		return nil, err
	}

	var recLog *recordinglog.Log
	if cfg.attachRecordingLog {
		recLog, err = openNodeRecordingLog(nodeDir)
		if err != nil {
			_ = nodeState.Close()
			return nil, err
		}
	}

	electionCfg := perNodeElectionConfig(cfg.electionCfg, cfg.id, cfg.perNodeOverrides)

	node := &Node{
		id:           cfg.id,
		members:      members,
		clock:        NewNodeClock(simStartTimeNs, defaultEpochMs),
		nodeState:    nodeState,
		network:      cfg.network,
		recordingLog: recLog,
	}
	if err := attachLiveElection(cfg.sim, node, electionCfg); err != nil {
		_ = node.Close()
		return nil, err
	}
	return node, nil
}

func perNodeElectionConfig(
	base election.ElectionConfig,
	id int32,
	overrides func(id int32, cfg *election.ElectionConfig),
) election.ElectionConfig {
	base.MemberID = id
	if overrides != nil {
		overrides(id, &base)
	}
	return base
}

func memberPtrByID(members []election.Member, id int32) (*election.Member, error) {
	for _, m := range election.MembersPtrs(members) {
		if m.ID == id {
			return m, nil
		}
	}
	return nil, fmt.Errorf("sim: member id %d not in spec", id)
}

// attachLiveElection builds publisher/host/agent/observer and NewElection.
//
// Keeps node.clock and node.nodeState. Clears inbound delivery tracking.
// Bumps publishGen and scrubs peer delivered keys for this source so a fresh
// SbePublisher (Sent indices restart at 0) cannot false-complete inject waits
// against pre-crash deliveries.
//
// Re-reads sim.cfg.UseReplicationHost so crash/restart rebuilds the same host
// path as the initial attach.
func attachLiveElection(sim *ElectionSim, node *Node, cfg election.ElectionConfig) error {
	thisMember, err := memberPtrByID(node.members, node.id)
	if err != nil {
		return err
	}
	network, id := node.network, node.id
	publisher := &election.SbePublisher{
		ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion,
		SentHandler:   func(msg election.SentMessage) { network.Enqueue(id, msg) },
	}
	observer := newElectionStateObserver(sim, node)

	node.publishGen++
	node.publisher = publisher
	node.observer = observer
	node.receivedMsgKeys = nil
	node.agentCM = nil
	for _, peer := range sim.nodes {
		if peer.id == id {
			continue
		}
		scrubDeliveredFrom(peer, id)
	}

	var (
		host    election.ElectionHost
		handler *simAgentHandler
		members []*election.Member
	)
	if sim.cfg.UseReplicationHost {
		host, handler, members = attachReplicationHost(node, cfg, publisher, thisMember)
	} else {
		host, handler, members = attachElectionHost(node)
	}
	node.host = host
	node.agent = handler

	deps := election.ElectionDeps{
		Handler:        handler,
		Host:           host,
		Publisher:      publisher,
		NanoClock:      node.clock.TimeNs,
		EpochClock:     node.clock.TimeMs,
		NominationRand: sim.cfg.NominationRand,
		NodeState:      node.nodeState,
		Members:        members,
		ThisMember:     thisMember,
		StateObserver:  observer,
	}
	if node.recordingLog != nil {
		deps.RecordingLog = node.recordingLog
		deps.RecordingLogLookup = node.recordingLog
		deps.RecordingLogFileSyncLevel = recordinglog.FileSyncNone
		host.SetLogRecordingID(LocalLogRecordingID)
	}
	node.election = election.NewElection(cfg, deps)
	return nil
}

// nodeAppendSource reads the election append high-water live so the Agent
// rank and publish source never desyncs from election state (restart
// leave-Init and SetLeaderAppend need no counter re-sync). Zero while the
// node is crashed.
type nodeAppendSource struct{ node *Node }

func (a nodeAppendSource) AppendPosition() int64 {
	el := a.node.election
	if el == nil {
		return 0
	}
	return el.AppendPosition()
}

// clearLiveHost drops election-host wiring so a crashed node does not expose
// stale Host / Agent handles. Publisher and nodeState stay for dump/inject.
func clearLiveHost(n *Node) {
	n.election = nil
	n.host = nil
	n.agent = nil
	n.agentCM = nil
}

// attachElectionHost builds the default scripted simElectionHost path.
func attachElectionHost(node *Node) (election.ElectionHost, *simAgentHandler, []*election.Member) {
	host := &simElectionHost{now: node.clock.TimeNs, followerLogJoinOK: true}
	handler := &simAgentHandler{joinReturns: true}
	return host, handler, election.MembersPtrs(node.members)
}

// attachReplicationHost builds Agent + HostAdapter + simReplicationHost.
//
// Members slice is shared between HostAdapter and ElectionDeps (same pointers).
func attachReplicationHost(
	node *Node,
	cfg election.ElectionConfig,
	publisher *election.SbePublisher,
	thisMember *election.Member,
) (election.ElectionHost, *simAgentHandler, []*election.Member) {
	agentCM := consensus.NewAgent(consensus.AgentConfig{
		Commit:                    new(atomic.Int64),
		LeaderHeartbeatIntervalNs: cfg.LeaderHeartbeatIntervalNs,
		AppendSource:              nodeAppendSource{node: node},
	})
	members := election.MembersPtrs(node.members)
	hostAdapter := election.NewHostAdapter(election.HostAdapterConfig{
		Agent:                    agentCM,
		Publisher:                publisher,
		Members:                  members,
		ThisMember:               thisMember,
		LeaderHeartbeatTimeoutNs: cfg.LeaderHeartbeatTimeoutNs,
		NanoClock:                node.clock.TimeNs,
		LeaderMemberID:           election.NullMemberID,
	})
	agentCM.SetPositions(hostAdapter)
	agentCM.SetEgress(hostAdapter)

	node.agentCM = agentCM
	return &simReplicationHost{
		HostAdapter:       hostAdapter,
		followerLogJoinOK: true,
	}, &simAgentHandler{joinReturns: true, agentCM: agentCM}, members
}

// scrubDeliveredFrom drops peer-side delivery bookkeeping for messages that
// originated at from so reattach cannot treat pre-crash deliveries as current.
func scrubDeliveredFrom(n *Node, from int32) {
	keys := n.receivedMsgKeys[:0]
	for _, k := range n.receivedMsgKeys {
		if k.from != from {
			keys = append(keys, k)
		}
	}
	n.receivedMsgKeys = keys
}

func openNodeState(baseDir string, memberID int32) (election.CandidateTermStore, error) {
	dir := filepath.Join(baseDir, fmt.Sprintf("node-%d", memberID))
	return openNodeStateDir(dir)
}

func openNodeStateDir(dir string) (election.CandidateTermStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
}

// LocalLogRecordingID is the host recording id when AttachRecordingLog is set.
const LocalLogRecordingID int64 = 1

// openNodeRecordingLog opens recording.log under nodeDir/recording/.
//
// createNew is always true so Open creates a missing file and reloads an
// existing one (O_CREATE without O_TRUNC).
func openNodeRecordingLog(nodeDir string) (*recordinglog.Log, error) {
	recDir := filepath.Join(nodeDir, "recording")
	if err := os.MkdirAll(recDir, 0o700); err != nil {
		return nil, err
	}
	return recordinglog.Open(recDir, true)
}

// DefaultElectionConfig is the base a sim node takes when Config.ElectionConfig
// is the zero value: the Java ElectionTest starting positions (null leadership
// term, zero log and append positions, startup true).
//
// Scenarios seed from it so the harness default and the scenarios share one
// authority.
func DefaultElectionConfig() election.ElectionConfig {
	cfg := election.ElectionTestConfig()
	cfg.LeadershipTermID = election.NullValue
	cfg.LogPosition = 0
	cfg.AppendPosition = 0
	cfg.IsNodeStartup = true
	return cfg
}
