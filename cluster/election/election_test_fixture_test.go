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
	appointedLeaderID int32
	isNodeStartup     bool
	leadershipTermID  int64
	logPosition       int64
	appendPosition    int64
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

// electionFixture mirrors Java ElectionTest.newElection: an Election over a
// recording host and agent driven by a manually advanced clock.
type electionFixture struct {
	t          *testing.T
	election   *election.Election
	host       *recordingElectionHost
	agent      *recordingAgentHandler
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
		appointedLeaderID: int32(election.NullValue),
	}
	for _, opt := range opts {
		opt(&p)
	}

	members := []election.Member{{ID: 0}, {ID: 1}, {ID: 2}}
	if p.singleNode {
		members = []election.Member{{ID: 0}}
	}

	cfg, deps, clock, epochClock, nodeState := electionDepsForMembers(t, members)
	cfg.IsNodeStartup = p.isNodeStartup
	cfg.AppointedLeaderID = p.appointedLeaderID
	cfg.LeadershipTermID = p.leadershipTermID
	cfg.LogPosition = p.logPosition
	cfg.AppendPosition = p.appendPosition

	host := &recordingElectionHost{
		clock:          clock,
		role:           consensus.RoleFollower,
		logSessionID:   fixtureLogSessionID,
		quorumPosition: p.appendPosition,
	}
	agent := &recordingAgentHandler{appendPositionReturn: p.appendPosition}
	deps.Host = host
	deps.Handler = agent
	deps.NominationRand = func() float64 { return 0 }

	return &electionFixture{
		t:          t,
		election:   election.NewElection(cfg, deps),
		host:       host,
		agent:      agent,
		publisher:  deps.Publisher.(*election.SbePublisher),
		clock:      clock,
		epochClock: epochClock,
		nodeState:  nodeState,
		cfg:        cfg,
	}
}

func (fx *electionFixture) doWork() int { return fx.election.DoWork(fx.clock.nowNs) }

func (fx *electionFixture) advance(deltaNs int64) {
	fx.clock.nowNs += deltaNs
	fx.epochClock.nowMs += deltaNs / 1_000_000
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

func (fx *electionFixture) injectCanvassPosition(followerID int32, logTerm, logPos int64) {
	fx.election.OnCanvassPosition(codecs.CanvassPositionFields{
		LogLeadershipTermID: logTerm,
		LogPosition:         logPos,
		LeadershipTermID:    logTerm,
		FollowerMemberID:    followerID,
	})
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

func (fx *electionFixture) injectNewLeadershipTerm(leaderID int32, term int64, sessionID int32) {
	fx.election.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
		LeadershipTermID: term,
		LeaderMemberID:   leaderID,
		LogSessionID:     sessionID,
	})
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

	addLogPublicationCalls            []int64
	appendNewLeadershipTermEventCalls []int64
}

func (h *recordingElectionHost) Role() consensus.Role { return h.role }

func (h *recordingElectionHost) RoleSet(role consensus.Role) { h.role = role }

func (h *recordingElectionHost) AddLogPublication(appendPosition int64) int32 {
	h.addLogPublicationCalls = append(h.addLogPublicationCalls, appendPosition)
	return h.logSessionID
}

func (h *recordingElectionHost) QuorumPositionBoundedByLeaderLog(int64, int64) int64 {
	return h.quorumPosition
}

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

var _ election.ElectionHost = (*recordingElectionHost)(nil)
