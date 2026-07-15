// Package sim provides a deterministic discrete-event election simulator:
// scheduler, in-memory network, and Node wiring around election.Election.
package sim

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/warrenguy/heron/cluster/election"
)

const defaultMaxSteps = 100_000

var (
	ErrQueueEmpty      = errors.New("sim: scheduler queue empty")
	ErrMaxSteps        = errors.New("sim: max steps exceeded")
	ErrEmptyMemberSpec = errors.New("sim: empty member spec")
)

// Config configures a deterministic multi-node election simulation.
type Config struct {
	Seed       uint64
	MemberSpec string

	// ElectionConfig seeds every node; MemberID is overwritten per node.
	// The zero value takes harness defaults, any other value is used verbatim.
	ElectionConfig election.ElectionConfig

	// PerNodeOverrides mutates a node's ElectionConfig after the MemberID is set.
	// Nil applies no per-node change. Clearing IsNodeStartup on one member
	// switches its canvass deadline from StartupCanvassTimeoutNs to the leader
	// heartbeat path (TimeOfLastLeaderUpdateNs + LeaderHeartbeatTimeoutNs).
	PerNodeOverrides func(id int32, cfg *election.ElectionConfig)

	// NodeStateDir holds per-node durable state under node-<id>/.
	// Empty defaults to tb.TempDir().
	NodeStateDir string

	DeliverLatencyNs int64
	MaxSteps         int
	Nemesis          Nemesis
	NetworkPolicy    NetworkPolicy

	// FocusNodeID limits ticking and inbound delivery to one member when non-nil.
	// Timeout scenarios set it so only the subject node runs Election.DoWork while
	// peers act as wire inject sources. Without it, the default three-node exchange
	// reaches NOMINATE before scripted timeout jumps. Nil restores full multi-node
	// sim behavior for canvass regression scenarios.
	FocusNodeID *int32
}

// ElectionSim orchestrates scheduler, network, and Node instances.
type ElectionSim struct {
	cfg       Config
	scheduler *Scheduler
	network   *Network
	nodes     []*Node
	timeline  []TimelineEntry
	rng       *rand.Rand
	steps     int
	closed    bool
}

// NewElectionSim constructs and fully wires a simulation.
func NewElectionSim(tb testing.TB, cfg Config) (*ElectionSim, error) {
	tb.Helper()
	if cfg.MemberSpec == "" {
		cfg.MemberSpec = defaultThreeNodeMemberSpec
	}
	cfg.Seed = SeedFromEnv(tb, cfg.Seed)
	if cfg.ElectionConfig == (election.ElectionConfig{}) {
		cfg.ElectionConfig = DefaultElectionConfig()
	}
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	if cfg.Nemesis == nil {
		cfg.Nemesis = DefaultNemesis()
	}
	if cfg.NodeStateDir == "" {
		cfg.NodeStateDir = tb.TempDir()
	}

	parsed, err := election.ParseMembers(cfg.MemberSpec)
	if err != nil {
		return nil, err
	}
	if len(parsed) == 0 {
		return nil, ErrEmptyMemberSpec
	}

	s := &ElectionSim{
		cfg:       cfg,
		scheduler: NewScheduler(simStartTimeNs),
		rng:       NewRNG(cfg.Seed),
	}
	policy := cfg.NetworkPolicy
	if cfg.FocusNodeID != nil {
		if policy == nil {
			policy = AllowAllPolicy{}
		}
		policy = focusOnlyPolicy{focus: *cfg.FocusNodeID, inner: policy}
	}
	s.network = NewNetwork(s, s.scheduler, policy, cfg.DeliverLatencyNs)

	for i := range parsed {
		node, err := newNode(nodeBuildConfig{
			id:               parsed[i].ID,
			members:          parsed,
			electionCfg:      cfg.ElectionConfig,
			perNodeOverrides: cfg.PerNodeOverrides,
			nodeStateDir:     cfg.NodeStateDir,
			network:          s.network,
			sim:              s,
		})
		if err != nil {
			for _, built := range s.nodes {
				_ = built.Close()
			}
			return nil, err
		}
		s.nodes = append(s.nodes, node)
		s.network.Register(node)
	}

	s.scheduleInitialTicks()

	if err := s.cfg.Nemesis.OnBuild(s); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// Node returns the Node for a member id, nil when unknown.
func (s *ElectionSim) Node(id int32) *Node {
	for _, node := range s.nodes {
		if node.id == id {
			return node
		}
	}
	return nil
}

// Nodes returns all Node instances in member-spec order.
func (s *ElectionSim) Nodes() []*Node { return s.nodes }

// RunSteps pops exactly n scheduler events.
func (s *ElectionSim) RunSteps(n int) error {
	for range n {
		if err := s.step(); err != nil {
			return err
		}
	}
	return nil
}

// RunUntil loops until fn returns true, the scheduler queue is empty, or
// MaxSteps is hit. An empty queue before the next step is a normal stop and
// returns nil. A predicate that always returns false drains pending events
// without asserting a target state.
func (s *ElectionSim) RunUntil(fn func(*ElectionSim) bool) error {
	for {
		if fn(s) {
			return nil
		}
		if _, ok := s.scheduler.PeekNext(); !ok {
			return nil
		}
		if err := s.step(); err != nil {
			return err
		}
	}
}

// AdvanceToPhase advances toward targetNs, schedules a tick for nodeID at the
// resulting master time, and runs one scheduler step so Election.DoWork
// evaluates timeout branches. A targetNs before the current clock does not
// regress it: AdvanceTo clamps, and the tick fires at the current master time.
func (s *ElectionSim) AdvanceToPhase(nodeID int32, targetNs int64) error {
	if s.Node(nodeID) == nil {
		return fmt.Errorf("sim: unknown phase node %d", nodeID)
	}
	if err := s.AdvanceTo(targetNs); err != nil {
		return err
	}
	s.scheduleEvent(s.Now(), EventNodeTick, nodeID, nil)
	return s.RunSteps(1)
}

// AdvanceTo runs events while the next event time is <= deadlineNs, then advances
// master time to deadlineNs when it is not before the current master time.
// A deadline before the current master time does not regress the clock.
func (s *ElectionSim) AdvanceTo(deadlineNs int64) error {
	for {
		next, ok := s.scheduler.PeekNext()
		if !ok || next.AtNs > deadlineNs {
			if deadlineNs > s.scheduler.Now() {
				s.scheduler.SetNow(deadlineNs)
			}
			return nil
		}
		if err := s.step(); err != nil {
			return err
		}
	}
}

// Now returns the current master simulated time in nanoseconds. Scenarios use
// it to derive AdvanceTo deadlines relative to the point a state was reached.
func (s *ElectionSim) Now() int64 { return s.scheduler.Now() }

// Timeline returns a copy of recorded timeline entries.
func (s *ElectionSim) Timeline() []TimelineEntry {
	out := make([]TimelineEntry, len(s.timeline))
	copy(out, s.timeline)
	return out
}

// DumpOnFail writes seed, timeline, and per-node state when the test failed.
func (s *ElectionSim) DumpOnFail(tb testing.TB) {
	tb.Helper()
	if !tb.Failed() {
		return
	}
	s.Dump(tb)
}

// Dump unconditionally writes simulation state to tb.Log.
func (s *ElectionSim) Dump(tb testing.TB) {
	tb.Helper()
	dumpElectionSim(tb, s)
}

// Close releases per-node durable state.
func (s *ElectionSim) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var first error
	for _, node := range s.nodes {
		if err := node.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *ElectionSim) step() error {
	pop, ok := s.scheduler.PopNext()
	if !ok {
		return ErrQueueEmpty
	}
	s.steps++
	s.scheduler.SetNow(pop.AtNs)

	switch pop.Kind {
	case EventNodeTick:
		node := s.Node(pop.NodeID)
		if node == nil {
			return fmt.Errorf("sim: unknown tick node %d", pop.NodeID)
		}
		coalesceSeq := s.scheduler.ReserveSeq()
		work := node.Tick(pop.AtNs)
		s.appendTimeline(pop, "tick", TimelineEntry{State: node.election.State(), Work: work})
		// Focused timeout scenarios advance one DoWork per AdvanceToPhase; coalesced
		// re-ticks would run before scripted inject delivery and skip heartbeat steps.
		if work > 0 && s.cfg.FocusNodeID == nil {
			s.scheduler.ScheduleReserved(pop.AtNs, coalesceSeq, ScheduledEvent{
				Kind:   EventNodeTick,
				NodeID: node.id,
			})
		}
	case EventDeliver:
		payload, ok := pop.Payload.(*DeliverPayload)
		if !ok || payload == nil {
			return errors.New("sim: deliver event missing payload")
		}
		handedOff := s.network.Deliver(*payload)
		s.appendTimeline(pop, "deliver", TimelineEntry{
			From:       payload.From,
			To:         payload.To,
			TemplateID: payload.Msg.TemplateID,
			HandedOff:  handedOff,
		})
		// Focused timeout scenarios schedule ticks explicitly via AdvanceToPhase.
		if handedOff && s.cfg.FocusNodeID == nil {
			s.scheduleEvent(pop.AtNs, EventNodeTick, payload.To, nil)
		}
	case EventNemesis:
		if err := s.cfg.Nemesis.OnEvent(s, pop); err != nil {
			return err
		}
		s.appendTimeline(pop, "nemesis", TimelineEntry{})
	default:
		return fmt.Errorf("sim: unknown event kind %d", pop.Kind)
	}

	if s.steps > s.cfg.MaxSteps {
		return ErrMaxSteps
	}
	return nil
}

func (s *ElectionSim) scheduleEvent(atNs int64, kind EventKind, nodeID int32, payload any) {
	s.scheduler.Schedule(atNs, ScheduledEvent{
		Kind:    kind,
		NodeID:  nodeID,
		Payload: payload,
	})
}

func (s *ElectionSim) appendTimeline(pop ScheduledEvent, kind string, entry TimelineEntry) {
	entry.AtNs = pop.AtNs
	entry.Seq = pop.Seq
	entry.Kind = kind
	entry.NodeID = pop.NodeID
	s.timeline = append(s.timeline, entry)
}

func (s *ElectionSim) appendTimelineNow(kind string, nodeID int32, entry TimelineEntry) {
	entry.AtNs = s.scheduler.Now()
	entry.Seq = s.scheduler.ReserveSeq()
	entry.Kind = kind
	entry.NodeID = nodeID
	s.timeline = append(s.timeline, entry)
}

func (s *ElectionSim) scheduleInitialTicks() {
	for _, node := range s.nodes {
		if s.ticksNode(node.id) {
			s.scheduleEvent(simStartTimeNs, EventNodeTick, node.id, nil)
		}
	}
}

func (s *ElectionSim) ticksNode(nodeID int32) bool {
	if s.cfg.FocusNodeID == nil {
		return true
	}
	return *s.cfg.FocusNodeID == nodeID
}
