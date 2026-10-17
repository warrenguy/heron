// Package sim provides a deterministic discrete-event election simulator:
// scheduler, in-memory network, and Node wiring around election.Election.
package sim

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
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
	//
	// The zero value takes harness defaults, any other value is used verbatim.
	ElectionConfig election.ElectionConfig

	// PerNodeOverrides mutates a node's ElectionConfig after the MemberID is set.
	//
	// Nil applies no per-node change. Clearing IsNodeStartup on one member
	// switches its canvass deadline from StartupCanvassTimeoutNs to the leader
	// heartbeat path (TimeOfLastLeaderUpdateNs + LeaderHeartbeatTimeoutNs).
	PerNodeOverrides func(id int32, cfg *election.ElectionConfig)

	// NodeStateDir holds per-node durable state under node-<id>/.
	//
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

	// SkipSplitBrainCheck disables the election after-step scan for two live
	// members in self-leader states on the same term. Transition-time
	// first-winner recording (leaderWins) and candidate-term monotonicity
	// still run under testhooks. Set when a script intentionally forms illegal
	// intermediate wire state and must not stop the step loop on the election
	// live scan alone.
	//
	// Continuous commit safety (UseReplicationHost && !SkipReplicationSafety)
	// still fails dual self-leaders via CheckSingleCommitAuthority. To force
	// dual leaders under replication host, set SkipReplicationSafety as well.
	SkipSplitBrainCheck bool

	// NominationRand seeds per-node nomination back-off. Nil leaves election
	// deps at zero delay (default). Scenarios that need randomness call
	// ConfigWithSimRand; NewElectionSim never invents a default RNG.
	NominationRand func() float64

	// AttachRecordingLog opens a real recording.log under each node dir and
	// wires it into election deps. Default false keeps soak seeds cheap.
	AttachRecordingLog bool

	// UseReplicationHost wires HostAdapter + consensus.Agent as the election
	// host. Default false keeps election catalog on simElectionHost.
	UseReplicationHost bool

	// SkipReplicationSafety disables continuous commit safety when
	// UseReplicationHost is true. Default false: commit checks run after each
	// step whenever the replication host is on. Set true to keep HostAdapter
	// wiring without those checks.
	SkipReplicationSafety bool

	// SeedAttribution is optional scenario run identity for failure dumps.
	// Empty Scenario skips base/scenario/index dump lines.
	SeedAttribution SeedAttribution
}

// SeedAttribution names base, scenario, and run index for failure dumps.
type SeedAttribution struct {
	BaseSeed uint64
	Scenario string
	RunIndex int
}

// ConfigWithSimRand returns cfg with NominationRand bound to a deterministic
// RNG seeded from cfg.Seed. Call after Seed is final.
//
// NewElectionSim never applies this; nil NominationRand stays zero delay.
func ConfigWithSimRand(cfg Config) Config {
	rng := NewRNG(cfg.Seed)
	cfg.NominationRand = func() float64 {
		return rng.Float64()
	}
	return cfg
}

// commitSignalKey is (member, leadership term) for continuous commit high-water.
type commitSignalKey struct {
	memberID int32
	term     int64
}

// ElectionSim orchestrates scheduler, network, and Node instances.
type ElectionSim struct {
	cfg         Config
	scheduler   *Scheduler
	network     *Network
	nodes       []*Node
	timeline    []TimelineEntry
	rng         *rand.Rand
	steps       int
	closed      bool
	innerPolicy NetworkPolicy
	safetyErr   error
	leaderWins  map[int64]int32

	// votes holds every vote grant under testhooks. A crash keeps it, so the
	// one grant per term check spans restarts.
	votes electiontest.VoteLedger

	// inbound is the message in delivery, or nil outside Network.Deliver.
	inbound *DeliverPayload

	// injecting is true while inject publishes.
	injecting bool

	// lastCommitSignals tracks high-water commit signal per (memberID, term)
	// for continuous commit safety under UseReplicationHost. Lazy-init on
	// first check.
	lastCommitSignals map[commitSignalKey]int64

	// restartLeaveInitHook replaces post-rebuild Init DoWork when non-nil
	// (stuck-Init rollback tests under testhooks).
	restartLeaveInitHook func(*Node, int64)
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
		cfg:        cfg,
		scheduler:  NewScheduler(simStartTimeNs),
		rng:        NewRNG(cfg.Seed),
		leaderWins: make(map[int64]int32),
	}
	s.innerPolicy = normalizeNetworkPolicy(cfg.NetworkPolicy)
	s.network = NewNetwork(s, s.scheduler, s.wrapPolicy(s.innerPolicy), cfg.DeliverLatencyNs)

	for i := range parsed {
		node, err := newNode(nodeBuildConfig{
			id:                 parsed[i].ID,
			members:            parsed,
			electionCfg:        cfg.ElectionConfig,
			perNodeOverrides:   cfg.PerNodeOverrides,
			nodeStateDir:       cfg.NodeStateDir,
			network:            s.network,
			sim:                s,
			attachRecordingLog: cfg.AttachRecordingLog,
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

// IsLive reports whether node id is currently live (not crashed).
//
// Liveness is node.election != nil: CrashNode nils it, RestartNode rebuilds it.
func (s *ElectionSim) IsLive(id int32) bool {
	node := s.Node(id)
	return node != nil && node.election != nil
}

// SetNetworkPolicy sets the INNER policy and re-wraps focus when FocusNodeID set.
func (s *ElectionSim) SetNetworkPolicy(inner NetworkPolicy) {
	s.innerPolicy = normalizeNetworkPolicy(inner)
	s.network.setPolicy(s.wrapPolicy(s.innerPolicy))
}

// InnerNetworkPolicy returns the mutable inner policy (not the focus wrap).
func (s *ElectionSim) InnerNetworkPolicy() NetworkPolicy {
	return s.innerPolicy
}

// ScheduleNemesis schedules an EventNemesis at atNs with the given payload.
func (s *ElectionSim) ScheduleNemesis(atNs int64, payload NemesisPayload) {
	s.scheduleEvent(atNs, EventNemesis, payload.NodeID, payload)
}

// SafetyErr returns the first recorded safety violation, if any.
func (s *ElectionSim) SafetyErr() error {
	return s.safetyErr
}

// LeaderWins returns a copy of term -> first member to enter a self-leader
// state for that leadership term. Nil when empty.
func (s *ElectionSim) LeaderWins() map[int64]int32 {
	if len(s.leaderWins) == 0 {
		return nil
	}
	out := make(map[int64]int32, len(s.leaderWins))
	maps.Copy(out, s.leaderWins)
	return out
}

func normalizeNetworkPolicy(p NetworkPolicy) NetworkPolicy {
	if p == nil {
		return AllowAllPolicy{}
	}
	return p
}

func (s *ElectionSim) wrapPolicy(inner NetworkPolicy) NetworkPolicy {
	if s.cfg.FocusNodeID == nil {
		return inner
	}
	return focusOnlyPolicy{focus: *s.cfg.FocusNodeID, inner: inner}
}

// recordSafetyErr stores the first safety violation only (later calls no-op).
func (s *ElectionSim) recordSafetyErr(err error) {
	if err == nil || s.safetyErr != nil {
		return
	}
	s.safetyErr = err
	s.appendTimelineNow("safety", 0, TimelineEntry{Detail: err.Error()})
}

// continuousSafetyAfterStep is set under testhooks to run after each step:
//   - candidate-term monotonicity
//   - the live two-self-leaders scan, unless SkipSplitBrainCheck
//   - the ballot vote count scan
//   - continuous commit safety (monotonic, floor-on-advance, single
//     authority) when UseReplicationHost && !SkipReplicationSafety
//
// Untagged builds leave it nil.
var continuousSafetyAfterStep func(*ElectionSim) error

// publishSafetyCheck is set under testhooks to check each published message
// before the drop policy runs.
//
// Untagged builds leave it nil.
var publishSafetyCheck func(s *ElectionSim, from int32, msg election.SentMessage)

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
//
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

// Steps returns how many scheduler events have been processed.
func (s *ElectionSim) Steps() int { return s.steps }

// Timeline returns a copy of recorded timeline entries.
func (s *ElectionSim) Timeline() []TimelineEntry {
	return s.TimelineSince(0)
}

// TimelineLen returns the number of recorded timeline entries.
func (s *ElectionSim) TimelineLen() int { return len(s.timeline) }

// TimelineSince returns a copy of timeline entries from index start.
//
// RunUntil predicates track a scanned offset with it instead of re-copying
// the whole timeline every step.
func (s *ElectionSim) TimelineSince(start int) []TimelineEntry {
	if start >= len(s.timeline) {
		return nil
	}
	return slices.Clone(s.timeline[start:])
}

// DumpOnFail writes seed, timeline, and per-node state when the test failed.
func (s *ElectionSim) DumpOnFail(tb testing.TB) {
	tb.Helper()
	if !tb.Failed() {
		return
	}
	s.Dump(tb)
}

// Dump writes simulation state to tb.Log.
//
// When HERON_SIM_SAVE=1, also writes under FailureDumpsDir and returns that
// directory path. Returns "" when save is off or the write fails (write errors
// are logged only so DumpOnFail never fails the failure dump).
func (s *ElectionSim) Dump(tb testing.TB) string {
	tb.Helper()
	return dumpElectionSim(tb, s)
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
		if node.election == nil {
			s.appendTimeline(pop, "tick_skip", TimelineEntry{Skipped: true})
			break
		}

		// Reserve coalesce seq before Tick so re-tick precedes same-instant delivers.
		//
		// Focused timeout scenarios skip coalesce: one DoWork per AdvanceToPhase.
		coalesce := s.cfg.FocusNodeID == nil
		var coalesceSeq uint64
		if coalesce {
			coalesceSeq = s.scheduler.ReserveSeq()
		}
		work := node.Tick(pop.AtNs)
		s.appendTimeline(pop, "tick", TimelineEntry{State: node.election.State(), Work: work})
		if work > 0 && coalesce {
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
		entry := TimelineEntry{
			From:       payload.From,
			To:         payload.To,
			TemplateID: payload.Msg.TemplateID,
		}

		// Registered but dead: no-op without panic. Unknown targets still take
		// the Deliver path (handed_off=false) so isolation tests see "deliver".
		if dst := s.Node(payload.To); dst != nil && dst.election == nil {
			entry.Skipped = true
			s.appendTimeline(pop, "deliver_skip", entry)
			break
		}
		entry.HandedOff = s.network.Deliver(*payload)
		s.appendTimeline(pop, "deliver", entry)

		// Focused timeout scenarios schedule ticks explicitly via AdvanceToPhase.
		if entry.HandedOff && s.cfg.FocusNodeID == nil {
			s.scheduleEvent(pop.AtNs, EventNodeTick, payload.To, nil)
		}
	case EventNemesis:
		if err := s.cfg.Nemesis.OnEvent(s, pop); err != nil {
			return err
		}
		action := ""
		if p, ok := pop.Payload.(NemesisPayload); ok {
			action = p.Action
		}
		s.appendTimeline(pop, "nemesis", TimelineEntry{Action: action})
	default:
		return fmt.Errorf("sim: unknown event kind %d", pop.Kind)
	}

	// Safety before MaxSteps: over-budget step must not bury a violation.
	if s.safetyErr != nil {
		return s.safetyErr
	}
	if continuousSafetyAfterStep != nil {
		if err := continuousSafetyAfterStep(s); err != nil {
			return err
		}
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
