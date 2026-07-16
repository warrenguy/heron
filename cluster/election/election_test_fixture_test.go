package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
)

const fixtureLogSessionID int32 = 777

type fixtureParams struct {
	singleNode        bool
	memberID          int32
	appointedLeaderID int32
	appVersion        int32
	isNodeStartup     bool
	leadershipTermID  int64
	logPosition       int64
	appendPosition    int64
	hostRole          consensus.Role
	stateObserver     *recordingStateObserver
}

type fixtureOption func(*fixtureParams)

func withSingleNode() fixtureOption {
	return func(p *fixtureParams) { p.singleNode = true }
}

func withAppointedLeaderID(id int32) fixtureOption {
	return func(p *fixtureParams) { p.appointedLeaderID = id }
}

func withNodeStartup(startup bool) fixtureOption {
	return func(p *fixtureParams) { p.isNodeStartup = startup }
}

func withPositions(leadershipTermID, logPosition, appendPosition int64) fixtureOption {
	return func(p *fixtureParams) {
		p.leadershipTermID = leadershipTermID
		p.logPosition = logPosition
		p.appendPosition = appendPosition
	}
}

func withMemberID(id int32) fixtureOption {
	return func(p *fixtureParams) { p.memberID = id }
}

func withHostRole(role consensus.Role) fixtureOption {
	return func(p *fixtureParams) { p.hostRole = role }
}

func withStateObserver(o *recordingStateObserver) fixtureOption {
	return func(p *fixtureParams) { p.stateObserver = o }
}

func withAppVersion(v int32) fixtureOption {
	return func(p *fixtureParams) { p.appVersion = v }
}

func newRecordingHostAndAgent(
	clock *FakeClock,
	role consensus.Role,
	logPosition, appendPosition int64,
) (*recordingElectionHost, *recordingAgentHandler) {
	host := &recordingElectionHost{
		clock:               clock,
		role:                role,
		logSessionID:        fixtureLogSessionID,
		quorumPosition:      appendPosition,
		localLogRecordingID: int64(election.NullValue),

		// Join succeeds immediately by default; override followerLogJoinOK or
		// agent.joinFollowerReturn for stall/timeout paths.
		followerLogJoinOK:       true,
		followerLogJoinPosition: logPosition,
	}
	agent := &recordingAgentHandler{
		appendPositionReturn: appendPosition,
		joinFollowerReturn:   true,
	}
	return host, agent
}

// electionFixture mirrors Java ElectionTest.newElection: an Election over a
// recording host and agent driven by a manually advanced clock.
type electionFixture struct {
	t          *testing.T
	election   *election.Election
	host       *recordingElectionHost
	agent      *recordingAgentHandler
	observer   *recordingStateObserver
	publisher  *election.SbePublisher
	clock      *FakeClock
	epochClock *FakeEpochClock
	nodeState  *recordingNodeState
	cfg        election.ElectionConfig
}

func newElectionFixture(t *testing.T, opts ...fixtureOption) *electionFixture {
	t.Helper()

	p := fixtureParams{
		isNodeStartup:     true,
		leadershipTermID:  testLeadershipTermID,
		logPosition:       testLogPosition,
		appendPosition:    testAppendPosition,
		appointedLeaderID: election.NullMemberID,
		hostRole:          consensus.RoleFollower,
	}
	for _, opt := range opts {
		opt(&p)
	}

	members := []election.Member{{ID: 0}, {ID: 1}, {ID: 2}}
	if p.singleNode {
		members = []election.Member{{ID: 0}}
	}

	cfg, deps, clock, epochClock, nodeState := electionDepsForMemberPtrs(
		t, election.MembersPtrs(members), p.memberID,
	)
	cfg.IsNodeStartup = p.isNodeStartup
	cfg.AppointedLeaderID = p.appointedLeaderID
	cfg.AppVersion = p.appVersion
	cfg.LeadershipTermID = p.leadershipTermID
	cfg.LogPosition = p.logPosition
	cfg.AppendPosition = p.appendPosition

	host, agent := newRecordingHostAndAgent(clock, p.hostRole, p.logPosition, p.appendPosition)
	deps.Host = host
	deps.Handler = agent
	deps.NominationRand = func() float64 { return 0 }
	if p.stateObserver != nil {
		deps.StateObserver = p.stateObserver
	}

	return &electionFixture{
		t:          t,
		election:   election.NewElection(cfg, deps),
		host:       host,
		agent:      agent,
		observer:   p.stateObserver,
		publisher:  deps.Publisher.(*election.SbePublisher),
		clock:      clock,
		epochClock: epochClock,
		nodeState:  nodeState,
		cfg:        cfg,
	}
}

func (fx *electionFixture) doWork() int { return fx.election.DoWork(fx.clock.nowNs) }

func (fx *electionFixture) advance(deltaNs int64) {
	advanceFakeClocks(fx.clock, fx.epochClock, deltaNs)
}

func (fx *electionFixture) driveUntilState(target election.ElectionState, maxIter int) {
	fx.t.Helper()
	for range maxIter {
		if fx.election.State() == target {
			return
		}
		fx.doWork()
		fx.advance(fx.cfg.ElectionStatusIntervalNs)
	}
	require.Equalf(
		fx.t,
		target,
		fx.election.State(),
		"did not reach %v within %d iterations",
		target,
		maxIter,
	)
}

// injectBaselineCanvassPositions reports both followers at the fixture's
// baseline term and append position.
func (fx *electionFixture) injectBaselineCanvassPositions() {
	fx.injectCanvassPosition(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	fx.injectCanvassPosition(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
}

// canvassTo canvasses both followers at the baseline positions, then drives
// the election to target.
func (fx *electionFixture) canvassTo(target election.ElectionState) {
	fx.t.Helper()
	fx.doWork()
	fx.injectBaselineCanvassPositions()
	fx.driveUntilState(target, 30)
}

func (fx *electionFixture) member(id int32) *election.Member {
	return election.MemberForTest(fx.election, id)
}

func injectCanvassPosition(e *election.Election, followerID int32, logTerm, logPos int64) {
	e.OnCanvassPosition(codecs.CanvassPositionFields{
		LogLeadershipTermID: logTerm,
		LogPosition:         logPos,
		LeadershipTermID:    logTerm,
		FollowerMemberID:    followerID,
	})
}

func (fx *electionFixture) injectCanvassPosition(followerID int32, logTerm, logPos int64) {
	injectCanvassPosition(fx.election, followerID, logTerm, logPos)
}

func (fx *electionFixture) injectRequestVote(
	candidateID int32,
	candidateTerm, logTerm, logPos int64,
) {
	fx.election.OnRequestVote(codecs.RequestVoteFields{
		LogLeadershipTermID: logTerm,
		LogPosition:         logPos,
		CandidateTermID:     candidateTerm,
		CandidateMemberID:   candidateID,
	})
}

// injectVote delivers a follower's vote for this node's candidacy. OnVote only
// tallies votes whose CandidateMemberID is the local candidate, so the helper
// fixes it to ThisMemberID.
func (fx *electionFixture) injectVote(followerID int32, logTerm, logPos int64, vote bool) {
	voteType := clusterwire.BooleanType_FALSE
	if vote {
		voteType = clusterwire.BooleanType_TRUE
	}
	fx.election.OnVote(codecs.VoteFields{
		CandidateTermID:     election.CandidateTermIDForTest(fx.election),
		LogLeadershipTermID: logTerm,
		LogPosition:         logPos,
		CandidateMemberID:   fx.election.ThisMemberID(),
		FollowerMemberID:    followerID,
		Vote:                voteType,
	})
}

// injectNewLeadershipTerm delivers a NewLeadershipTerm anchored at the
// fixture's baseline log term and position: the log term matches this node's
// so the install path runs, and the successor-term fields carry the null
// sentinels a leader with no recording log publishes.
func (fx *electionFixture) injectNewLeadershipTerm(leaderID int32, term int64) {
	fx.election.OnNewLeadershipTerm(installNewLeadershipTermFields(fx.cfg, leaderID, term))
}

func (fx *electionFixture) injectAppendPosition(followerID int32, term, pos int64) {
	fx.election.OnAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: term,
		LogPosition:      pos,
		FollowerMemberID: followerID,
	})
}

// requireVoteSentFor asserts the election entered FollowerBallot and published
// a single TRUE vote for candidateID at the baseline append position.
func (fx *electionFixture) requireVoteSentFor(candidateID int32) {
	fx.t.Helper()
	require.Equal(fx.t, election.ElectionStateFollowerBallot, fx.election.State())
	require.Len(fx.t, fx.publisher.Sent, 1)
	sent := fx.publisher.Sent[0]
	require.Equal(fx.t, uint16(clusterwire.VoteSbeTemplateID), sent.TemplateID)
	vote := sent.Fields.(codecs.VoteFields)
	require.Equal(fx.t, clusterwire.BooleanType_TRUE, vote.Vote)
	require.Equal(fx.t, fx.election.AppendPosition(), vote.LogPosition)
	require.Equal(fx.t, candidateID, vote.CandidateMemberID)
}

// requireRequestVoteBroadcast runs one work cycle and asserts a RequestVote
// went to each follower carrying the append position, not the log position.
func (fx *electionFixture) requireRequestVoteBroadcast() {
	fx.t.Helper()
	fx.publisher.Reset()
	fx.doWork()
	require.Len(fx.t, fx.publisher.Sent, 2)
	for _, msg := range fx.publisher.Sent {
		require.Equal(fx.t, uint16(clusterwire.RequestVoteSbeTemplateID), msg.TemplateID)
		rv := msg.Fields.(codecs.RequestVoteFields)
		require.Equal(fx.t, fx.election.AppendPosition(), rv.LogPosition)
	}
}

func (fx *electionFixture) requireLocalLeader() {
	fx.t.Helper()
	leader := fx.election.Leader()
	require.NotNil(fx.t, leader)
	require.Equal(fx.t, fx.election.ThisMemberID(), leader.ID)
}

// recordingElectionHost records role and quorum interactions the ballot and
// leader/follower completion paths drive.
type recordingElectionHost struct {
	electiontest.NoopElectionHost
	clock *FakeClock

	role consensus.Role

	logSessionID   int32
	quorumPosition int64

	// quorumSequence returns successive quorum values for commit/NLT interval
	// oracle tests; unset hosts keep using quorumPosition.
	quorumSequence  []int64
	quorumCallCount int

	localLogRecordingID     int64
	followerLogJoinOK       bool
	followerLogJoinPosition int64
	catchupNearLive         bool

	addLogPublicationCalls            []int64
	appendNewLeadershipTermEventCalls []int64
	publishCommitPositionCalls        []publishCommitPositionCall
	followerLogJoinReadyCalls         []int32
	catchupNearLiveCalls              []int64
	truncateCalls                     []truncateCall

	trackCatchupCalls []trackCatchupCall
}

type truncateCall struct {
	leadershipTermID int64
	logPosition      int64
}

type trackCatchupCall struct {
	memberID         int32
	leadershipTermID int64
	flags            uint8
}

type publishCommitPositionCall struct {
	commitPosition   int64
	leadershipTermID int64
}

func (h *recordingElectionHost) Role() consensus.Role { return h.role }

func (h *recordingElectionHost) RoleSet(role consensus.Role) { h.role = role }

func (h *recordingElectionHost) AddLogPublication(appendPosition int64) int32 {
	h.addLogPublicationCalls = append(h.addLogPublicationCalls, appendPosition)
	return h.logSessionID
}

func (h *recordingElectionHost) QuorumPositionBoundedByLeaderLog(int64, int64) int64 {
	if len(h.quorumSequence) > 0 {
		idx := h.quorumCallCount
		h.quorumCallCount++
		if idx < len(h.quorumSequence) {
			return h.quorumSequence[idx]
		}
		return h.quorumSequence[len(h.quorumSequence)-1]
	}
	return h.quorumPosition
}

func (h *recordingElectionHost) LocalLogRecordingID() int64 { return h.localLogRecordingID }

func (h *recordingElectionHost) PublishCommitPosition(commitPosition, leadershipTermID int64) {
	h.publishCommitPositionCalls = append(h.publishCommitPositionCalls, publishCommitPositionCall{
		commitPosition:   commitPosition,
		leadershipTermID: leadershipTermID,
	})
}

func (h *recordingElectionHost) FollowerLogJoinReady(
	logSessionID int32,
) (consensus.FollowerLogJoinParams, bool) {
	h.followerLogJoinReadyCalls = append(h.followerLogJoinReadyCalls, logSessionID)
	if !h.followerLogJoinOK {
		return consensus.FollowerLogJoinParams{}, false
	}
	return consensus.FollowerLogJoinParams{
		LogChannel:      "log=test",
		LogStreamID:     100,
		LogSessionID:    logSessionID,
		LogJoinPosition: h.followerLogJoinPosition,

		// Deliberately wrong: the election must overwrite this with its own
		// isLeaderStartup, so tests observe the overwrite direction.
		IsLeaderStartup: true,
	}, true
}

func (h *recordingElectionHost) TruncateLogEntry(leadershipTermID, logPosition int64) {
	h.truncateCalls = append(h.truncateCalls, truncateCall{
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
	})
}

func (h *recordingElectionHost) IsCatchupNearLive(position int64) bool {
	h.catchupNearLiveCalls = append(h.catchupNearLiveCalls, position)
	return h.catchupNearLive
}

// NewLogReplay returns an immediately done replay so leader and follower
// replay paths advance to the target position on the next tick.
func (h *recordingElectionHost) NewLogReplay(_, appendPosition int64) election.LogReplay {
	return &stubLogReplay{position: appendPosition}
}

type stubLogReplay struct {
	position int64
}

func (r *stubLogReplay) DoWork(int64) int { return 0 }

func (r *stubLogReplay) Position() int64 { return r.position }

func (r *stubLogReplay) IsDone() bool { return true }

func (h *recordingElectionHost) UpdateMemberLogPosition(
	member *election.Member,
	leadershipTermID, logPosition int64,
) {
	member.LeadershipTermID = leadershipTermID
	member.LogPosition = logPosition
	member.TimeOfLastAppendPositionNs = h.clock.TimeNs()
}

func (h *recordingElectionHost) AppendNewLeadershipTermEvent(nowNs int64) bool {
	h.appendNewLeadershipTermEventCalls = append(h.appendNewLeadershipTermEventCalls, nowNs)
	return true
}

func (h *recordingElectionHost) TrackCatchupCompletion(
	member *election.Member,
	leadershipTermID int64,
	flags uint8,
) {
	h.trackCatchupCalls = append(h.trackCatchupCalls, trackCatchupCall{
		memberID:         member.ID,
		leadershipTermID: leadershipTermID,
		flags:            flags,
	})
}

var _ election.ElectionHost = (*recordingElectionHost)(nil)

// newTimeoutElection builds the 3-node ElectionTest fixture for node memberID
// with Java-parity timeout defaults: null leadership term, zero positions,
// startup canvass, and a state observer recording every transition.
func newTimeoutElection(
	t *testing.T, memberID int32, opts ...fixtureOption,
) *electionFixture {
	t.Helper()

	base := []fixtureOption{
		withMemberID(memberID),
		withPositions(election.NullValue, 0, 0),
		withStateObserver(&recordingStateObserver{}),
	}
	return newElectionFixture(t, append(base, opts...)...)
}
