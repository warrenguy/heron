package sim

import (
	"fmt"
	"os"
	"path/filepath"

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
	host      *simElectionHost
	agent     *simAgentHandler
	clock     *NodeClock
	nodeState election.CandidateTermStore
	observer  *StateObserver

	receivedFromOrder []int32
	receivedMsgKeys   []deliveredKey
}

// Election returns the underlying election instance.
func (n *Node) Election() *election.Election { return n.election }

// Publisher returns the routed publisher for this node.
func (n *Node) Publisher() *election.SbePublisher { return n.publisher }

// HostRole returns the role the election host last had set on it.
func (n *Node) HostRole() consensus.Role { return n.host.Role() }

// SetHostRole seeds the election host role before the first tick.
func (n *Node) SetHostRole(role consensus.Role) { n.host.role = role }

// Tick runs one Election.DoWork cycle at nowNs.
func (n *Node) Tick(nowNs int64) int {
	n.clock.Set(nowNs)
	return n.election.DoWork(nowNs)
}

// ObserverStatesForTest returns state transitions recorded by the election observer.
func (n *Node) ObserverStatesForTest() []election.ElectionState {
	if n.observer == nil {
		return nil
	}
	return append([]election.ElectionState(nil), n.observer.States...)
}

// ReceivedFromOrderForTest returns inbound sender order for delivered messages.
func (n *Node) ReceivedFromOrderForTest() []int32 {
	return append([]int32(nil), n.receivedFromOrder...)
}

// hasDelivered reports whether the message at the source's Sent index has been
// delivered to this node.
func (n *Node) hasDelivered(key deliveredKey) bool {
	for _, k := range n.receivedMsgKeys {
		if k == key {
			return true
		}
	}
	return false
}

// Snapshot returns observable per-node state.
func (n *Node) Snapshot() NodeSnapshot {
	candidateTermID := election.NullValue
	ct, err := n.nodeState.CandidateTerm()
	if err == nil {
		candidateTermID = ct.CandidateTermID()
	}
	return NodeSnapshot{
		NodeID:             n.id,
		ElectionState:      n.election.State(),
		LeadershipTermID:   n.election.LeadershipTermID(),
		LogPosition:        n.election.LogPosition(),
		CandidateTermID:    candidateTermID,
		PublishedCount:     len(n.publisher.Sent),
		InboundCount:       len(n.receivedFromOrder),
		PrepareCalls:       n.agent.prepareCalls,
		PrepareLogPosition: n.agent.prepareLogPosition,
		PrepareNowNs:       n.agent.prepareNowNs,
	}
}

// Close releases durable node state.
func (n *Node) Close() error {
	return n.nodeState.Close()
}

type nodeBuildConfig struct {
	id               int32
	members          []election.Member
	electionCfg      election.ElectionConfig
	perNodeOverrides func(id int32, cfg *election.ElectionConfig)
	nodeStateDir     string
	network          *Network
	sim              *ElectionSim
}

func newNode(cfg nodeBuildConfig) (*Node, error) {
	members := append([]election.Member(nil), cfg.members...)
	memberPtrs := election.MembersPtrs(members)
	var thisMember *election.Member
	for _, m := range memberPtrs {
		if m.ID == cfg.id {
			thisMember = m
			break
		}
	}
	if thisMember == nil {
		return nil, fmt.Errorf("sim: member id %d not in spec", cfg.id)
	}

	nodeState, err := openNodeState(cfg.nodeStateDir, cfg.id)
	if err != nil {
		return nil, err
	}

	clock := NewNodeClock(simStartTimeNs, defaultEpochMs)
	network, id := cfg.network, cfg.id
	publisher := &election.SbePublisher{
		ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion,
		SentHandler:   func(msg election.SentMessage) { network.Enqueue(id, msg) },
	}

	electionCfg := cfg.electionCfg
	electionCfg.MemberID = cfg.id
	if cfg.perNodeOverrides != nil {
		cfg.perNodeOverrides(cfg.id, &electionCfg)
	}

	observer := &StateObserver{sim: cfg.sim, nodeID: cfg.id}

	host := &simElectionHost{now: clock.TimeNs}
	agent := &simAgentHandler{}

	deps := election.ElectionDeps{
		Handler:       agent,
		Host:          host,
		Publisher:     publisher,
		NanoClock:     clock.TimeNs,
		EpochClock:    clock.TimeMs,
		NodeState:     nodeState,
		Members:       memberPtrs,
		ThisMember:    thisMember,
		StateObserver: observer,
	}

	return &Node{
		id:        cfg.id,
		members:   members,
		election:  election.NewElection(electionCfg, deps),
		publisher: publisher,
		host:      host,
		agent:     agent,
		clock:     clock,
		nodeState: nodeState,
		observer:  observer,
	}, nil
}

func openNodeState(baseDir string, memberID int32) (election.CandidateTermStore, error) {
	dir := filepath.Join(baseDir, fmt.Sprintf("node-%d", memberID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
}

// DefaultElectionConfig is the base a sim node takes when Config.ElectionConfig
// is the zero value: the Java ElectionTest starting positions (null leadership
// term, zero log and append positions, startup true). Scenarios seed from it so
// the harness default and the scenarios share one authority.
func DefaultElectionConfig() election.ElectionConfig {
	cfg := election.ElectionTestConfig()
	cfg.LeadershipTermID = election.NullValue
	cfg.LogPosition = 0
	cfg.AppendPosition = 0
	cfg.IsNodeStartup = true
	return cfg
}
