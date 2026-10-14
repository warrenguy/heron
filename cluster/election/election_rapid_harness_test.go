//go:build rapid

package election_test

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	rapidClusterSize   = 3
	rapidBagCap        = 64
	rapidSeedTerm      = testLeadershipTermID
	rapidSeedLogPos    = testLogPosition
	rapidSeedAppendPos = testAppendPosition
)

type rapidAction struct {
	weight      int
	crashWeight int
	run         func(*electionRapidCluster, *rapid.T)
}

// weight: default safety mix, high Tick/Advance/Deliver, medium Seed, low
// Partition/Heal/Crash/Restart. crashWeight: crash/restart-heavy mix for
// durable candidate-term properties.
var rapidActions = []rapidAction{
	{20, 14, (*electionRapidCluster).actTickAll},
	{14, 10, (*electionRapidCluster).actTickOne},
	{18, 14, (*electionRapidCluster).actAdvanceClock},
	{14, 12, (*electionRapidCluster).actDeliverOne},
	{12, 10, (*electionRapidCluster).actDeliverAll},
	{10, 6, (*electionRapidCluster).actSeedPeerLogs},
	{3, 2, (*electionRapidCluster).actPartition},
	{2, 2, (*electionRapidCluster).actHeal},
	{3, 15, (*electionRapidCluster).actCrash},
	{4, 15, (*electionRapidCluster).actRestart},
}

// isSelfLeaderState reports self-leadership via election state. Host role is
// sticky after HandleError leaves state Init without RoleSet(Follower).
func isSelfLeaderState(s election.ElectionState) bool {
	switch s {
	case election.ElectionStateLeaderLogReplication,
		election.ElectionStateLeaderReplay,
		election.ElectionStateLeaderInit,
		election.ElectionStateLeaderReady:
		return true
	default:
		return false
	}
}

type pendingMsg struct {
	from int32
	msg  election.SentMessage
}

func (p pendingMsg) to() int32 { return p.msg.Dst.MemberID }

// bagCommitHost bridges Host.PublishCommitPosition into SbePublisher.Sent so
// harvest can bag CommitPosition the same way as consensus-pub messages.
type bagCommitHost struct {
	*recordingElectionHost
	publisher *election.SbePublisher
	leaderID  int32
	peerIDs   []int32
}

func (h *bagCommitHost) PublishCommitPosition(commitPosition, leadershipTermID int64) {
	h.recordingElectionHost.PublishCommitPosition(commitPosition, leadershipTermID)
	for _, peerID := range h.peerIDs {
		h.publisher.CommitPosition(
			election.PublicationTarget{MemberID: peerID},
			codecs.CommitPositionFields{
				LeadershipTermID: leadershipTermID,
				LogPosition:      commitPosition,
				LeaderMemberID:   h.leaderID,
			},
		)
	}
}

// rapidRecordingLog implements election.RecordingLogLookup over a fixed entry
// slice.
type rapidRecordingLog struct {
	entries []recordinglog.Entry
}

func (l *rapidRecordingLog) FindTermEntry(leadershipTermID int64) *recordinglog.Entry {
	for i := range l.entries {
		if l.entries[i].LeadershipTermID == leadershipTermID {
			return &l.entries[i]
		}
	}
	return nil
}

func (l *rapidRecordingLog) truncate(leadershipTermID, logPosition int64) {
	if entry := l.FindTermEntry(leadershipTermID); entry != nil {
		entry.LogPosition = logPosition
	}
}

// rapidLogHost adds a recording log and a member quorum to bagCommitHost.
//
// TruncateLogEntry also lowers the term entry, the prepare return and the
// follower join position, as an archive truncation does. Quorum and leader
// position match HostAdapter.
type rapidLogHost struct {
	*bagCommitHost
	log                      *rapidRecordingLog
	agent                    *recordingAgentHandler
	members                  []*election.Member
	thisMember               *election.Member
	ranked                   []int64
	leaderHeartbeatTimeoutNs int64
}

func (h *rapidLogHost) TruncateLogEntry(leadershipTermID, logPosition int64) error {
	if err := h.bagCommitHost.TruncateLogEntry(leadershipTermID, logPosition); err != nil {
		return err
	}
	h.log.truncate(leadershipTermID, logPosition)
	h.agent.appendPositionReturn = min(h.agent.appendPositionReturn, logPosition)
	h.followerLogJoinPosition = min(h.followerLogJoinPosition, logPosition)
	return nil
}

func (h *rapidLogHost) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	return min(
		election.QuorumPosition(h.members, h.ranked, nowNs, h.leaderHeartbeatTimeoutNs),
		appendPosition,
	)
}

func (h *rapidLogHost) UpdateLeaderPosition(nowNs, appendPosition, _ int64) int {
	h.thisMember.LogPosition = appendPosition
	h.thisMember.TimeOfLastAppendPositionNs = nowNs
	return 0
}

// rapidBindOption configures one node's host at bind time.
type rapidBindOption func(*rapidBindOptions)

type rapidBindOptions struct {
	recordingLog *rapidRecordingLog
}

// withRapidRecordingLog wraps the node's host in rapidLogHost over log.
func withRapidRecordingLog(log *rapidRecordingLog) rapidBindOption {
	return func(o *rapidBindOptions) { o.recordingLog = log }
}

// crashSnapshot holds election log identity restored on mid-run restart.
type crashSnapshot struct {
	leadershipTerm    int64
	logPosition       int64
	appendPosition    int64
	logLeadershipTerm int64
}

type rapidNode struct {
	id               int32
	live             bool
	initLoaded       bool
	maxCandidateSeen int64
	election         *election.Election
	host             *recordingElectionHost
	publisher        *election.SbePublisher
	clock            *FakeClock
	epochClock       *FakeEpochClock
	cfg              election.ElectionConfig
	nodeState        *recordingNodeState
	peerIDs          []int32
	// crashSnap holds the last crash log identity for restart restore. Set on
	// crash alongside live=false, so a dead node always has it and restartNode
	// dereferences without a nil guard.
	crashSnap *crashSnapshot
	// bindOptions carry over to restartNode.
	bindOptions []rapidBindOption
}

type winEdgeObserver struct {
	cluster *electionRapidCluster
	node    *rapidNode
	prev    election.ElectionState
}

func (o *winEdgeObserver) OnElectionStateChange(s election.ElectionState) {
	prev := o.prev
	o.prev = s

	n := o.node
	if !n.live || n.election == nil {
		return
	}

	// HandleError (and any path) re-enters Init; durable reload can lower
	// in-memory candidateTerm (e.g. NLT install raised it without ProposeMax).
	// Reopen candidate-term tracking until the next successful leave-Init.
	if s == election.ElectionStateInit {
		n.initLoaded = false
		return
	}
	if prev == election.ElectionStateInit && !n.initLoaded {
		n.initLoaded = true
		n.maxCandidateSeen = election.CandidateTermIDForTest(n.election)
	}

	if s != election.ElectionStateLeaderLogReplication {
		return
	}
	leader := n.election.Leader()
	if leader == nil || leader.ID != n.id {
		return
	}

	c := o.cluster
	term := n.election.LeadershipTermID()
	if existing, ok := c.leaderWins[term]; ok && existing != n.id {
		c.noteEdgeFail("two self-leaders term=%d members=%d and %d", term, existing, n.id)
	} else {
		c.leaderWins[term] = n.id
	}

	if prev != election.ElectionStateCandidateBallot {
		return
	}
	ct := election.CandidateTermIDForTest(n.election)
	if term != ct {
		c.noteEdgeFail("ballot self-win leadershipTerm=%d candidateTerm=%d member=%d", term, ct, n.id)
	}

	// Ballot self-win needs quorum support; unanimous is a quorum subset when
	// graceful-closed is NullMemberID.
	members := election.MembersForTest(n.election)
	if !election.IsQuorumLeader(members, ct) {
		c.noteEdgeFail("self-win without quorum term=%d member=%d", term, n.id)
	}
}

type electionRapidCluster struct {
	t     *testing.T
	nodes []*rapidNode
	// bag is the in-flight network: publish always enqueues, and at rapidBagCap
	// the oldest drops (lossy FIFO). deliverAllHolding prepends held messages onto
	// mid-pass enqueues without re-capping, so len(bag) can exceed rapidBagCap.
	bag []pendingMsg
	// blocked[from,to]: hold-until-open at deliver (publish always enqueues).
	blocked    map[[2]int32]bool
	leaderWins map[int64]int32
	// edgeFail holds the first invariant failure from a self-win transition or
	// a NewLeadershipTerm send.
	edgeFail string
	// crashWeighted selects rapidActions crashWeight over weight.
	crashWeighted bool
	// clockDeltaGen is fixed after construct for AdvanceClock draws.
	clockDeltaGen *rapid.Generator[int64]
}

func (c *electionRapidCluster) noteEdgeFail(format string, args ...any) {
	if c.edgeFail != "" {
		return
	}
	c.edgeFail = fmt.Sprintf(format, args...)
}

func rapidClusterMemberPtrs(n int) []*election.Member {
	members := make([]election.Member, n)
	for i := range members {
		members[i] = election.Member{ID: int32(i)}
	}
	return election.MembersPtrs(members)
}

func newElectionRapidCluster(t *testing.T, n int) *electionRapidCluster {
	t.Helper()
	require.GreaterOrEqual(t, n, 2)

	c := newRapidClusterShell(t, n)
	for id := range n {
		c.nodes[id] = c.constructNode(int32(id), n)
	}

	c.clockDeltaGen = rapidClockDeltaGen(c.nodes[0].cfg)

	// Drive every node through Init so durable candidate terms load before tracking.
	// winEdgeObserver baselined initLoaded/maxCandidateSeen on leave-Init.
	c.tickAll()
	for _, node := range c.nodes {
		require.NotEqual(t, election.ElectionStateInit, node.election.State())
		require.True(t, node.initLoaded)
	}
	return c
}

// newRapidClusterShell returns a cluster with n empty node slots.
func newRapidClusterShell(t *testing.T, n int) *electionRapidCluster {
	return &electionRapidCluster{
		t:          t,
		nodes:      make([]*rapidNode, n),
		blocked:    make(map[[2]int32]bool),
		leaderWins: make(map[int64]int32),
	}
}

func rapidClockDeltaGen(cfg election.ElectionConfig) *rapid.Generator[int64] {
	return rapid.SampledFrom([]int64{
		cfg.ElectionStatusIntervalNs,
		cfg.ElectionTimeoutNs / 4,
		cfg.ElectionTimeoutNs,
		cfg.LeaderHeartbeatTimeoutNs,
		cfg.StartupCanvassTimeoutNs,
		cfg.LeaderHeartbeatIntervalNs,
	})
}

// rapidNodeConfig is the one config shape for first boot and restart.
// StartupCanvassTimeoutNs shrinks from the production 60s so one AdvanceClock
// can leave extended canvass under the multi-node bag-delivered action budget.
func rapidNodeConfig(
	id int32,
	startup bool,
	term, logPos, appendPos int64,
) election.ElectionConfig {
	cfg := election.ElectionTestConfig()
	cfg.MemberID = id
	cfg.IsNodeStartup = startup
	cfg.LeadershipTermID = term
	cfg.LogPosition = logPos
	cfg.AppendPosition = appendPos
	cfg.StartupCanvassTimeoutNs = cfg.ElectionTimeoutNs
	return cfg
}

func (c *electionRapidCluster) constructNode(id int32, n int) *rapidNode {
	c.t.Helper()
	cfg := rapidNodeConfig(id, true, rapidSeedTerm, rapidSeedLogPos, rapidSeedAppendPos)
	return c.constructNodeWithConfig(id, n, cfg)
}

// constructNodeWithConfig builds a live node from cfg. The agent's prepare
// returns cfg.AppendPosition.
func (c *electionRapidCluster) constructNodeWithConfig(
	id int32,
	n int,
	cfg election.ElectionConfig,
	opts ...rapidBindOption,
) *rapidNode {
	c.t.Helper()

	memberPtrs := rapidClusterMemberPtrs(n)
	_, _, clock, epochClock, nodeState := electionDepsForMemberPtrs(
		c.t, memberPtrs, id,
	)

	peerIDs := make([]int32, 0, n-1)
	for i := range n {
		if int32(i) != id {
			peerIDs = append(peerIDs, int32(i))
		}
	}

	node := &rapidNode{
		id:         id,
		live:       true,
		clock:      clock,
		epochClock: epochClock,
		nodeState:  nodeState,
		peerIDs:    peerIDs,
	}

	c.bindElection(c.t, node, cfg, memberPtrs, cfg.AppendPosition, opts...)
	return node
}

// closeStores releases every node's NodeStateFile fd and mmap. Per-check
// property closures defer this so stores do not accumulate across rapid checks
// and shrink replays; the outer t.Cleanup close is then an idempotent no-op.
func (c *electionRapidCluster) closeStores() {
	for _, n := range c.nodes {
		require.NoError(c.t, n.nodeState.store.Close())
	}
}

// bindElection builds host/agent/publisher and NewElection for a live node.
// Reuses nodeState (open NodeStateFile) and clocks across mid-run restarts.
//
// tb is the require failure target: c.t at construct time, *rapid.T under Check.
func (c *electionRapidCluster) bindElection(
	tb require.TestingT,
	node *rapidNode,
	cfg election.ElectionConfig,
	memberPtrs []*election.Member,
	appendReturn int64,
	opts ...rapidBindOption,
) {
	node.bindOptions = opts
	var options rapidBindOptions
	for _, opt := range opts {
		opt(&options)
	}

	host, agent := newRecordingHostAndAgent(
		node.clock, consensus.RoleFollower, cfg.LogPosition, appendReturn,
	)
	pub := &election.SbePublisher{}

	obs := &winEdgeObserver{cluster: c, node: node}

	fromID := node.id
	pub.SentHandler = func(msg election.SentMessage) {
		c.checkNewLeadershipTermSend(fromID, host.Role(), msg)
		c.enqueue(fromID, msg)
	}

	thisMember := memberPtrForID(memberPtrs, node.id)
	require.NotNil(tb, thisMember, "member id %d not in cluster", node.id)

	commitHost := &bagCommitHost{
		recordingElectionHost: host,
		publisher:             pub,
		leaderID:              node.id,
		peerIDs:               node.peerIDs,
	}
	var electionHost election.ElectionHost = commitHost
	var lookup election.RecordingLogLookup
	if log := options.recordingLog; log != nil {
		lookup = log
		electionHost = &rapidLogHost{
			bagCommitHost:            commitHost,
			log:                      log,
			agent:                    agent,
			members:                  memberPtrs,
			thisMember:               thisMember,
			ranked:                   make([]int64, election.QuorumThreshold(len(memberPtrs))),
			leaderHeartbeatTimeoutNs: cfg.LeaderHeartbeatTimeoutNs,
		}
	}

	deps := election.ElectionDeps{
		Handler:            agent,
		Host:               electionHost,
		RecordingLogLookup: lookup,
		Publisher:          pub,
		NanoClock:          node.clock.TimeNs,
		EpochClock:         node.epochClock.TimeMs,
		NodeState:          node.nodeState,
		Members:            memberPtrs,
		ThisMember:         thisMember,
		NominationRand:     func() float64 { return 0 },
		StateObserver:      obs,
	}

	node.cfg = cfg
	node.host = host
	node.publisher = pub
	node.election = election.NewElection(cfg, deps)
}

func (c *electionRapidCluster) checkNewLeadershipTermSend(
	from int32,
	role consensus.Role,
	msg election.SentMessage,
) {
	if msg.TemplateID != uint16(clusterwire.NewLeadershipTermSbeTemplateID) {
		return
	}
	fields := msg.Fields.(codecs.NewLeadershipTermFields)
	if fields.LogLeadershipTermID > fields.LeadershipTermID {
		c.noteEdgeFail(
			"NewLeadershipTerm log term %d above leadership term %d member=%d",
			fields.LogLeadershipTermID, fields.LeadershipTermID, from,
		)
	}
	if role != consensus.RoleLeader {
		c.noteEdgeFail("NewLeadershipTerm from role %v member=%d", role, from)
	}
}

func (c *electionRapidCluster) enqueue(from int32, msg election.SentMessage) {
	if len(c.bag) >= rapidBagCap {
		c.bag = c.bag[1:]
	}
	c.bag = append(c.bag, pendingMsg{from: from, msg: msg})
}

func (c *electionRapidCluster) isBlocked(from, to int32) bool {
	return c.blocked[[2]int32{from, to}]
}

// messageHeld is true when deliver must keep p in the bag: blocked edge or
// dead target (hold-until-open / hold-until-restart).
func (c *electionRapidCluster) messageHeld(p pendingMsg) bool {
	if c.isBlocked(p.from, p.to()) {
		return true
	}
	target := c.nodes[p.to()]
	return !target.live || target.election == nil
}

func (c *electionRapidCluster) tickOne(n *rapidNode) {
	if !n.live || n.election == nil {
		return
	}
	n.election.DoWork(n.clock.nowNs)
}

func (c *electionRapidCluster) tickAll() {
	for _, n := range c.nodes {
		c.tickOne(n)
	}
}

func (c *electionRapidCluster) advanceClock(deltaNs int64) {
	// Advance all nodes (including dead) so restart resumes with coherent time.
	for _, n := range c.nodes {
		advanceFakeClocks(n.clock, n.epochClock, deltaNs)
	}
}

func (c *electionRapidCluster) deliver(p pendingMsg) {
	c.nodes[p.to()].election.HandleConsensusMessage(p.msg.Wire, 0)
}

// tryDeliver delivers p when the edge is open and the target is live. Returns
// false when the caller must leave p in the bag (hold-until-open /
// hold-until-restart).
func (c *electionRapidCluster) tryDeliver(p pendingMsg) bool {
	if c.messageHeld(p) {
		return false
	}
	c.deliver(p)
	return true
}

// deliverAllHolding tries every bag message once. Held blocked/dead-target
// messages stay first in the bag; publishes during the pass already sit after.
// Returns how many messages were held (not delivered).
func (c *electionRapidCluster) deliverAllHolding() (held int) {
	pending := c.bag
	c.bag = nil
	var heldMsgs []pendingMsg
	for _, msg := range pending {
		if !c.tryDeliver(msg) {
			heldMsgs = append(heldMsgs, msg)
		}
	}
	if len(heldMsgs) > 0 {
		c.bag = append(heldMsgs, c.bag...)
	}
	return len(heldMsgs)
}

func (c *electionRapidCluster) crashNode(n *rapidNode) {
	n.crashSnap = &crashSnapshot{
		leadershipTerm:    n.election.LeadershipTermID(),
		logPosition:       n.election.LogPosition(),
		appendPosition:    n.election.AppendPosition(),
		logLeadershipTerm: election.LogLeadershipTermIDForTest(n.election),
	}
	n.live = false
	n.initLoaded = false

	// Drop Election so no further DoWork; keep nodeState store open.
	n.election = nil
}

// restartNode rebuilds Election on a crashed node. tb receives require failures
// so rapid Check can shrink when restart is invoked from an action.
func (c *electionRapidCluster) restartNode(tb require.TestingT, n *rapidNode) {
	snap := n.crashSnap
	memberPtrs := rapidClusterMemberPtrs(len(c.nodes))
	cfg := rapidNodeConfig(n.id, false, snap.leadershipTerm, snap.logPosition, snap.appendPosition)

	// Fresh host/agent/publisher; clocks stay forward-only on the node.
	c.bindElection(tb, n, cfg, memberPtrs, snap.appendPosition, n.bindOptions...)

	// NewElection sets logLeadershipTermID = cfg.LeadershipTermID. Restore the
	// pre-crash log term so canvass/vote CompareLog identity matches crash.
	// (Canvass entry reseeds thisMember from leadershipTerm/appendPosition;
	// only the Election log field survives for publish and vote accept.)
	election.SetLogLeadershipTermIDForTest(n.election, snap.logLeadershipTerm)

	// Drive Init so handleInit loads durableCandidateTermID before tracking.
	// Observer re-baselines maxCandidateSeen on leave-Init.
	n.live = true
	n.election.DoWork(n.clock.nowNs)
	require.NotEqual(tb, election.ElectionStateInit, n.election.State())
	require.True(tb, n.initLoaded)
}

func (c *electionRapidCluster) checkInvariants(rt *rapid.T) {
	rt.Helper()

	require.Empty(rt, c.edgeFail)

	liveLeaders := make(map[int64]int32, len(c.nodes))
	for _, n := range c.nodes {
		// Durable store matches the write oracle for every node (live or crashed).
		durable, err := n.nodeState.CandidateTerm()
		require.NoError(rt, err)
		require.Equalf(rt, n.nodeState.expectedCandidateTerm(), durable.CandidateTermID(),
			"durable candidate term mismatch member=%d", n.id)

		if !n.live || n.election == nil {
			continue
		}

		// Candidate term non-decreasing after first successful init load.
		if n.initLoaded {
			ct := election.CandidateTermIDForTest(n.election)
			require.GreaterOrEqualf(rt, ct, n.maxCandidateSeen,
				"candidate term decreased member=%d seen=%d now=%d",
				n.id, n.maxCandidateSeen, ct)
			n.maxCandidateSeen = ct
		}

		st := n.election.State()
		if !isSelfLeaderState(st) {
			continue
		}
		term := n.election.LeadershipTermID()
		if existing, ok := liveLeaders[term]; ok {
			require.Equalf(rt, existing, n.id,
				"two live self-leaders term=%d via state members=%d and %d state=%v",
				term, existing, n.id, st)
		}
		liveLeaders[term] = n.id

		if winner, ok := c.leaderWins[term]; ok {
			require.Equalf(rt, winner, n.id,
				"live self-leader %d disagrees with recorded winner %d term=%d",
				n.id, winner, term)
		}
	}
}

func (c *electionRapidCluster) drawNode(rt *rapid.T, label string, wantLive bool) *rapidNode {
	rt.Helper()
	matching := make([]*rapidNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.live == wantLive {
			matching = append(matching, n)
		}
	}
	if len(matching) == 0 {
		if wantLive {
			rt.Skip("no live nodes")
		}
		rt.Skip("no dead nodes")
	}
	return matching[rapid.IntRange(0, len(matching)-1).Draw(rt, label)]
}

func (c *electionRapidCluster) drawLiveNode(rt *rapid.T, label string) *rapidNode {
	rt.Helper()
	return c.drawNode(rt, label, true)
}

func (c *electionRapidCluster) drawDeadNode(rt *rapid.T, label string) *rapidNode {
	rt.Helper()
	return c.drawNode(rt, label, false)
}

func (c *electionRapidCluster) drawClockDelta(rt *rapid.T) int64 {
	rt.Helper()
	return c.clockDeltaGen.Draw(rt, "clockDelta")
}

func (c *electionRapidCluster) actTickAll(_ *rapid.T) { c.tickAll() }

func (c *electionRapidCluster) actTickOne(rt *rapid.T) {
	c.tickOne(c.drawLiveNode(rt, "tickNode"))
}

func (c *electionRapidCluster) actAdvanceClock(rt *rapid.T) {
	c.advanceClock(c.drawClockDelta(rt))
}

func (c *electionRapidCluster) actDeliverOne(rt *rapid.T) {
	if !c.deliverDrawn(rt, "bagIdx") {
		rt.Skip("bag empty, edge blocked or target dead")
	}
}

// deliverDrawn delivers the bag message at an index drawn under label.
// Returns false when the bag is empty or the message is held.
func (c *electionRapidCluster) deliverDrawn(rt *rapid.T, label string) bool {
	rt.Helper()
	if len(c.bag) == 0 {
		return false
	}
	idx := rapid.IntRange(0, len(c.bag)-1).Draw(rt, label)
	msg := c.bag[idx]
	if c.messageHeld(msg) {
		return false
	}

	// Remove before deliver: delivery can enqueue and evict bag[0] at cap.
	c.bag = append(c.bag[:idx], c.bag[idx+1:]...)
	c.deliver(msg)
	return true
}

func (c *electionRapidCluster) actDeliverAll(rt *rapid.T) {
	if len(c.bag) == 0 {
		rt.Skip("bag empty")
	}
	pending := len(c.bag)
	held := c.deliverAllHolding()
	if held == pending {
		rt.Skip("all bag edges blocked or targets dead")
	}
}

func (c *electionRapidCluster) actSeedPeerLogs(rt *rapid.T) {
	target := c.drawLiveNode(rt, "seedTarget")
	peerID := rapid.Int32Range(0, int32(len(c.nodes)-1)).Draw(rt, "seedPeer")
	if peerID == target.id {
		rt.Skip("seed peer is target")
	}
	logTerm := rapid.Int64Range(0, 32).Draw(rt, "seedLogTerm")
	useNull := rapid.Bool().Draw(rt, "seedNullPos")
	var logPos int64
	if useNull {
		logPos = election.NullPosition
	} else {
		logPos = rapid.Int64Range(0, 1<<20).Draw(rt, "seedLogPos")
	}
	injectCanvassPosition(target.election, peerID, logTerm, logPos)
}

func (c *electionRapidCluster) actPartition(rt *rapid.T) {
	a := rapid.Int32Range(0, int32(len(c.nodes)-1)).Draw(rt, "partA")
	b := rapid.Int32Range(0, int32(len(c.nodes)-1)).Draw(rt, "partB")
	if a == b {
		rt.Skip("partition needs distinct members")
	}
	c.blocked[[2]int32{a, b}] = true
	c.blocked[[2]int32{b, a}] = true
}

func (c *electionRapidCluster) actHeal(rt *rapid.T) {
	if len(c.blocked) == 0 {
		rt.Skip("no partitions")
	}
	if rapid.Bool().Draw(rt, "healAll") {
		clear(c.blocked)
		return
	}
	keys := make([][2]int32, 0, len(c.blocked))
	for k := range c.blocked {
		keys = append(keys, k)
	}

	// Sorted keys so failfile index draw replays the same edge.
	slices.SortFunc(keys, func(a, b [2]int32) int {
		if c := cmp.Compare(a[0], b[0]); c != 0 {
			return c
		}
		return cmp.Compare(a[1], b[1])
	})
	k := keys[rapid.IntRange(0, len(keys)-1).Draw(rt, "healKey")]

	// Partition is undirected; heal both directions of the pair.
	delete(c.blocked, k)
	delete(c.blocked, [2]int32{k[1], k[0]})
}

func (c *electionRapidCluster) actCrash(rt *rapid.T) {
	n := c.drawLiveNode(rt, "crashNode")
	c.crashNode(n)
}

func (c *electionRapidCluster) actRestart(rt *rapid.T) {
	n := c.drawDeadNode(rt, "restartNode")
	c.restartNode(rt, n)
}

func (c *electionRapidCluster) actionWeight(a rapidAction) int {
	if c.crashWeighted {
		return a.crashWeight
	}
	return a.weight
}

func (c *electionRapidCluster) applyAction(rt *rapid.T, kind int) {
	rt.Helper()
	remaining := kind
	for _, a := range rapidActions {
		if remaining < c.actionWeight(a) {
			a.run(c, rt)
			return
		}
		remaining -= c.actionWeight(a)
	}
}

func (c *electionRapidCluster) runActions(rt *rapid.T) {
	rt.Helper()
	total := 0
	for _, a := range rapidActions {
		total += c.actionWeight(a)
	}
	rt.Repeat(map[string]func(*rapid.T){
		"act": func(rt *rapid.T) {
			kind := rapid.IntRange(0, total-1).Draw(rt, "actKind")
			c.applyAction(rt, kind)
		},
		"": func(rt *rapid.T) {
			c.checkInvariants(rt)
		},
	})
}
