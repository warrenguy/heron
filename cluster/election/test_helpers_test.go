package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
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

func advanceFakeClocks(clock *FakeClock, epoch *FakeEpochClock, deltaNs int64) {
	clock.nowNs += deltaNs
	epoch.nowMs += deltaNs / 1_000_000
}

type recordingNodeState struct {
	store        election.CandidateTermStore
	proposeCalls []struct {
		candidateTermID int64
		logPosition     int64
		timestampMs     int64
	}
	// durableSeen mirrors the wrapped fresh-file store's candidate term:
	// UpdateCandidateTermID sets it, ProposeMaxCandidateTermID takes the max,
	// NullValue before any write. Oracle for durable candidate-term invariants.
	durableSeen    int64
	durableTracked bool
}

func (r *recordingNodeState) expectedCandidateTerm() int64 {
	if !r.durableTracked {
		return election.NullValue
	}
	return r.durableSeen
}

func (r *recordingNodeState) CandidateTerm() (election.CandidateTerm, error) {
	return r.store.CandidateTerm()
}

func (r *recordingNodeState) UpdateCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) error {
	r.durableSeen = candidateTermID
	r.durableTracked = true
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
	if !r.durableTracked {
		r.durableSeen = election.NullValue
		r.durableTracked = true
	}
	if candidateTermID > r.durableSeen {
		r.durableSeen = candidateTermID
	}
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
	joinFollowerReturn   bool
	prepareErr           error
	scriptPreparePending bool
	joinLeaderErr        error
	joinFollowerErr      error

	prepareCalls       int
	prepareLogPosition int64
	prepareNowNs       int64
	prepareInProgress  bool
	joinLeaderCalls    []joinLogAsLeaderCall
	joinFollowerCalls  int
	joinFollowerParams []consensus.FollowerLogJoinParams
	joinFollowerNowNs  []int64
	electionCompleteNs []int64
}

func (h *recordingAgentHandler) PrepareForNewLeadership(
	logPosition int64,
	nowNs int64,
) (
	int64,
	bool,
	error,
) {
	h.prepareCalls++
	h.prepareLogPosition = logPosition
	h.prepareNowNs = nowNs
	if h.prepareErr != nil || h.scriptPreparePending {
		// A pending return keeps the episode open. An error closes the episode,
		// same as Agent.PrepareForNewLeadership.
		h.prepareInProgress = h.prepareErr == nil
		return recordinglog.NullPosition, false, h.prepareErr
	}
	h.prepareInProgress = false
	return h.appendPositionReturn, true, nil
}

func (h *recordingAgentHandler) IsPrepareInProgress() bool { return h.prepareInProgress }

func (h *recordingAgentHandler) JoinLogAsLeader(
	leadershipTermID, logPosition int64,
	logSessionID int32,
	isStartup bool,
) error {
	h.joinLeaderCalls = append(h.joinLeaderCalls, joinLogAsLeaderCall{
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
		logSessionID:     logSessionID,
		isStartup:        isStartup,
	})
	return h.joinLeaderErr
}

func (h *recordingAgentHandler) JoinLogAsFollower(
	params consensus.FollowerLogJoinParams,
	nowNs int64,
) (bool, error) {
	h.joinFollowerCalls++
	h.joinFollowerParams = append(h.joinFollowerParams, params)
	h.joinFollowerNowNs = append(h.joinFollowerNowNs, nowNs)
	return h.joinFollowerReturn, h.joinFollowerErr
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
	codecs.AppendPositionFields,
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

type recordingLogCoherencySpy struct {
	calls []recordinglog.EnsureCoherentParams
	err   error

	commitCalls []commitLogPositionCall
	commitErr   error
	forceCalls  int
	forceErr    error
}

type commitLogPositionCall struct {
	leadershipTermID int64
	logPosition      int64
}

func (s *recordingLogCoherencySpy) EnsureCoherent(p recordinglog.EnsureCoherentParams) error {
	s.calls = append(s.calls, p)
	return s.err
}

func (s *recordingLogCoherencySpy) CommitLogPosition(leadershipTermID, logPosition int64) error {
	s.commitCalls = append(s.commitCalls, commitLogPositionCall{
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
	})
	return s.commitErr
}

func (s *recordingLogCoherencySpy) Force(recordinglog.FileSyncLevel) error {
	s.forceCalls++
	return s.forceErr
}

// publisherRoundtripCase drives one message through any Publisher and checks
// the decoded wire. Shared by the SbePublisher and AeronPublisher roundtrip
// tests so template coverage cannot drift between implementations.
type publisherRoundtripCase struct {
	name       string
	publish    func(p election.Publisher)
	templateID uint16
	decode     func(wire []byte) (any, bool)
	want       any
}

func publisherRoundtripCases(dst election.PublicationTarget) []publisherRoundtripCase {
	return []publisherRoundtripCase{
		{
			name: "CanvassPosition",
			publish: func(p election.Publisher) {
				p.CanvassPosition(
					dst,
					codecs.CanvassPositionFields{
						LogLeadershipTermID: 1,
						LogPosition:         2,
						LeadershipTermID:    3,
						FollowerMemberID:    4,
					},
				)
			},
			templateID: clusterwire.CanvassPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeCanvassPositionAt(wire, 0)
			},
			want: codecs.CanvassPositionFields{
				LogLeadershipTermID: 1,
				LogPosition:         2,
				LeadershipTermID:    3,
				FollowerMemberID:    4,
				ProtocolVersion:     election.ProtocolSemanticVersion(),
			},
		},
		{
			name: "RequestVote",
			publish: func(p election.Publisher) {
				p.RequestVote(
					dst,
					codecs.RequestVoteFields{
						LogLeadershipTermID: 10,
						LogPosition:         20,
						CandidateTermID:     30,
						CandidateMemberID:   40,
					},
				)
			},
			templateID: clusterwire.RequestVoteSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeRequestVoteAt(wire, 0)
			},
			want: codecs.RequestVoteFields{
				LogLeadershipTermID: 10,
				LogPosition:         20,
				CandidateTermID:     30,
				CandidateMemberID:   40,
				ProtocolVersion:     election.ProtocolSemanticVersion(),
			},
		},
		{
			name: "Vote",
			publish: func(p election.Publisher) {
				p.PlaceVote(
					dst,
					codecs.VoteFields{
						CandidateTermID:     1,
						LogLeadershipTermID: 2,
						LogPosition:         3,
						CandidateMemberID:   4,
						FollowerMemberID:    5,
						Vote:                clusterwire.BooleanType_TRUE,
					},
				)
			},
			templateID: clusterwire.VoteSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeVoteAt(wire, 0)
			},
			want: codecs.VoteFields{
				CandidateTermID:     1,
				LogLeadershipTermID: 2,
				LogPosition:         3,
				CandidateMemberID:   4,
				FollowerMemberID:    5,
				Vote:                clusterwire.BooleanType_TRUE,
			},
		},
		{
			name: "NewLeadershipTerm_v16",
			publish: func(p election.Publisher) {
				p.NewLeadershipTerm(
					dst,
					codecs.NewLeadershipTermFields{
						LogLeadershipTermID:     1,
						NextLeadershipTermID:    2,
						NextTermBaseLogPosition: 3,
						NextLogPosition:         4,
						LeadershipTermID:        5,
						TermBaseLogPosition:     6,
						LogPosition:             7,
						LeaderRecordingID:       8,
						Timestamp:               9,
						LeaderMemberID:          10,
						LogSessionID:            11,
						AppVersion:              12,
						IsStartup:               clusterwire.BooleanType_FALSE,
						CommitPosition:          99,
					},
				)
			},
			templateID: clusterwire.NewLeadershipTermSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeNewLeadershipTermAt(wire, 0)
			},
			want: codecs.NewLeadershipTermFields{
				LogLeadershipTermID:     1,
				NextLeadershipTermID:    2,
				NextTermBaseLogPosition: 3,
				NextLogPosition:         4,
				LeadershipTermID:        5,
				TermBaseLogPosition:     6,
				LogPosition:             7,
				LeaderRecordingID:       8,
				Timestamp:               9,
				LeaderMemberID:          10,
				LogSessionID:            11,
				AppVersion:              12,
				IsStartup:               clusterwire.BooleanType_FALSE,
				CommitPosition:          99,
			},
		},
		{
			name: "AppendPosition",
			publish: func(p election.Publisher) {
				p.AppendPosition(
					dst,
					codecs.AppendPositionFields{
						LeadershipTermID: 7,
						LogPosition:      8,
						FollowerMemberID: 9,
						Flags:            0x02,
					},
				)
			},
			templateID: clusterwire.AppendPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeAppendPositionAt(wire, 0)
			},
			want: codecs.AppendPositionFields{
				LeadershipTermID: 7,
				LogPosition:      8,
				FollowerMemberID: 9,
				Flags:            0x02,
			},
		},
		{
			name: "CommitPosition",
			publish: func(p election.Publisher) {
				p.CommitPosition(
					dst,
					codecs.CommitPositionFields{
						LeadershipTermID: 11,
						LogPosition:      12,
						LeaderMemberID:   13,
					},
				)
			},
			templateID: clusterwire.CommitPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				return codecs.TryDecodeCommitPositionAt(wire, 0)
			},
			want: codecs.CommitPositionFields{
				LeadershipTermID: 11,
				LogPosition:      12,
				LeaderMemberID:   13,
			},
		},
	}
}
