package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

type FakeClock struct {
	nowNs int64
}

func (c *FakeClock) TimeNs() int64 { return c.nowNs }

type FakeEpochClock struct {
	nowMs int64
}

func (c *FakeEpochClock) TimeMs() int64 { return c.nowMs }

type recordingNodeState struct {
	store        election.CandidateTermStore
	proposeCalls []struct {
		candidateTermID int64
		logPosition     int64
		timestampMs     int64
	}
}

func (r *recordingNodeState) CandidateTerm() (election.CandidateTerm, error) {
	return r.store.CandidateTerm()
}

func (r *recordingNodeState) UpdateCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) error {
	return r.store.UpdateCandidateTermID(candidateTermID, logPosition, timestampMs)
}

func (r *recordingNodeState) ProposeMaxCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) (int64, error) {
	r.proposeCalls = append(r.proposeCalls, struct {
		candidateTermID int64
		logPosition     int64
		timestampMs     int64
	}{candidateTermID, logPosition, timestampMs})
	return r.store.ProposeMaxCandidateTermID(candidateTermID, logPosition, timestampMs)
}

func (r *recordingNodeState) Close() error { return nil }

// failingNodeState reads from store but fails every write with err.
type failingNodeState struct {
	store election.CandidateTermStore
	err   error
}

func (f *failingNodeState) CandidateTerm() (election.CandidateTerm, error) {
	return f.store.CandidateTerm()
}

func (f *failingNodeState) UpdateCandidateTermID(int64, int64, int64) error {
	return f.err
}

func (f *failingNodeState) ProposeMaxCandidateTermID(int64, int64, int64) (int64, error) {
	return 0, f.err
}

func (f *failingNodeState) Close() error { return nil }

type joinLogAsLeaderCall struct {
	leadershipTermID int64
	logPosition      int64
	logSessionID     int32
	isStartup        bool
}

type recordingAgentHandler struct {
	electiontest.NoopAgentHandler

	appendPositionReturn int64

	prepareCalls       int
	prepareLogPosition int64
	prepareNowNs       int64
	joinLeaderCalls    []joinLogAsLeaderCall
	joinFollowerCalls  int
	electionCompleteNs []int64
}

func (h *recordingAgentHandler) PrepareForNewLeadership(logPosition, nowNs int64) int64 {
	h.prepareCalls++
	h.prepareLogPosition = logPosition
	h.prepareNowNs = nowNs
	return h.appendPositionReturn
}

func (h *recordingAgentHandler) JoinLogAsLeader(
	leadershipTermID, logPosition int64,
	logSessionID int32,
	isStartup bool,
) {
	h.joinLeaderCalls = append(h.joinLeaderCalls, joinLogAsLeaderCall{
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
		logSessionID:     logSessionID,
		isStartup:        isStartup,
	})
}

func (h *recordingAgentHandler) JoinLogAsFollower(consensus.FollowerLogJoinParams, int64) bool {
	h.joinFollowerCalls++
	return true
}

func (h *recordingAgentHandler) ElectionComplete(nowNs int64) {
	h.electionCompleteNs = append(h.electionCompleteNs, nowNs)
}

// appendPositionFailPublisher fails AppendPosition and counts attempts;
// other publishes pass through to the embedded SbePublisher.
type appendPositionFailPublisher struct {
	*election.SbePublisher
	attempts int
}

func (p *appendPositionFailPublisher) AppendPosition(
	election.PublicationTarget,
	int64, int64,
	int32,
	uint8,
) bool {
	p.attempts++
	return false
}

type recordingStateObserver struct {
	states []election.ElectionState
}

func (o *recordingStateObserver) OnElectionStateChange(s election.ElectionState) {
	o.states = append(o.states, s)
}

func electionDepsForMembers(t *testing.T, members []election.Member) (
	election.ElectionConfig,
	election.ElectionDeps,
	*FakeClock,
	*FakeEpochClock,
	*recordingNodeState,
) {
	t.Helper()
	return electionDepsForMemberPtrs(t, election.MembersPtrs(members), members[0].ID)
}

func electionDepsForMemberPtrs(t *testing.T, memberPtrs []*election.Member, memberID int32) (
	election.ElectionConfig,
	election.ElectionDeps,
	*FakeClock,
	*FakeEpochClock,
	*recordingNodeState,
) {
	t.Helper()

	clock := &FakeClock{nowNs: 1_000_000_000}
	epochClock := &FakeEpochClock{nowMs: 42_000}

	store, err := election.OpenNodeStateFile(t.TempDir(), true, recordinglog.FileSyncNone)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	nodeState := &recordingNodeState{store: store}

	thisMember := memberPtrForID(memberPtrs, memberID)
	require.NotNil(t, thisMember, "member id %d not in cluster", memberID)

	cfg := election.ElectionTestConfig()
	cfg.MemberID = memberID

	deps := election.ElectionDeps{
		Handler:    electiontest.NoopAgentHandler{},
		Host:       electiontest.NoopElectionHost{},
		Publisher:  &election.SbePublisher{},
		NanoClock:  clock.TimeNs,
		EpochClock: epochClock.TimeMs,
		NodeState:  nodeState,
		Members:    memberPtrs,
		ThisMember: thisMember,
	}
	return cfg, deps, clock, epochClock, nodeState
}

func memberPtrForID(memberPtrs []*election.Member, memberID int32) *election.Member {
	for _, m := range memberPtrs {
		if m.ID == memberID {
			return m
		}
	}
	return nil
}

// Baseline term and positions shared by testElectionDeps and newElectionFixture.
const (
	testLeadershipTermID = 5
	testLogPosition      = 100
	testAppendPosition   = 200
)

func testElectionDeps(t *testing.T) (
	election.ElectionConfig,
	election.ElectionDeps,
	*FakeClock,
	*FakeEpochClock,
	*recordingNodeState,
) {
	t.Helper()

	cfg, deps, clock, epochClock, nodeState := electionDepsForMembers(t, []election.Member{
		{ID: 0},
		{ID: 1},
		{ID: 2},
	})
	cfg.LeadershipTermID = testLeadershipTermID
	cfg.LogPosition = testLogPosition
	cfg.AppendPosition = testAppendPosition
	return cfg, deps, clock, epochClock, nodeState
}

func singleNodeElectionDeps(t *testing.T) (election.ElectionConfig, election.ElectionDeps) {
	t.Helper()

	cfg, deps, _, _, _ := electionDepsForMembers(t, []election.Member{{ID: 0}})
	return cfg, deps
}

func requireNodeStatePrefixHex(t *testing.T, buf []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(buf), n)
	t.Logf("node-state prefix: %x", buf[:n])
}

func requireNodeStatePrefixEqual(t *testing.T, got, want []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(got), n)
	require.GreaterOrEqual(t, len(want), n)
	want = want[:n]
	requireNodeStatePrefixHex(t, got, n)
	require.Equal(t, want, got[:n])
}
