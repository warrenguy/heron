package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/internal/electiontest"
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

	addLogPublicationErr error
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

func withAddLogPublicationErr(err error) fixtureOption {
	return func(p *fixtureParams) { p.addLogPublicationErr = err }
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

		// HandleError syncs logPosition from CommitPosition. Seed the commit at the
		// consumed log position so error paths do not collapse logPosition to zero.
		// Tests that probe the sync seed a distinct commit.
		commitPosition: logPosition,

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

// useRecordingHost sets a recording host and agent in role on deps. Positions
// come from cfg.
func useRecordingHost(
	deps *election.ElectionDeps,
	clock *FakeClock,
	cfg election.ElectionConfig,
	role consensus.Role,
) {
	deps.Host, deps.Handler = newRecordingHostAndAgent(
		clock,
		role,
		cfg.LogPosition,
		cfg.AppendPosition,
	)
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
	host.addLogPublicationErr = p.addLogPublicationErr
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

	// ensureCatchupLogDestination is returned from EnsureCatchupLogDestination
	// when non-empty. Empty keeps the default no-op.
	ensureCatchupLogDestination string

	commitPosition        int64
	logJoined             bool
	catchupLogDestination string

	// liveLogDestination is the fake live URI. Empty until AddLiveLogDestination
	// succeeds or a test sets it.
	liveLogDestination string

	// addLiveLogDestinationFailLeft counts AddLiveLogDestination calls that
	// leave LiveLogDestination empty (driver refuse or back-pressure).
	addLiveLogDestinationFailLeft int

	// newLogReplication overrides Noop nil factory for duty-cycle tests.
	newLogReplication func(
		leaderArchiveEndpoint, responseArchiveEndpoint string,
		leaderRecordingID, stopPosition, nowNs int64,
	) election.LogReplication

	// newLogReplay overrides the done stub for Close and routing tests.
	newLogReplay func(logPosition, appendPosition int64) election.LogReplay

	terminating bool

	catchupPollResult int
	catchupPollErr    error

	catchupPollHook func(limitPosition, nowNs int64)

	addLogPublicationErr error

	addLogPublicationCalls            []int64
	appendNewLeadershipTermEventCalls []int64
	publishCommitPositionCalls        []publishCommitPositionCall
	followerLogJoinReadyCalls         []int32
	catchupNearLiveCalls              []int64
	catchupInitiatedCalls             []int64
	catchupPollCalls                  []catchupPollCall
	addLiveLogDestinationCalls        int
	newLogReplicationCalls            []newLogReplicationCall
	setLogRecordingIDCalls            []int64
	truncateCalls                     []truncateCall
	truncateErr                       error

	trackCatchupCalls                []trackCatchupCall
	removeCatchupLogDestinationCalls int
}

type catchupPollCall struct {
	limitPosition int64
	nowNs         int64
}

type newLogReplicationCall struct {
	leaderArchiveEndpoint   string
	responseArchiveEndpoint string
	leaderRecordingID       int64
	stopPosition            int64
	nowNs                   int64
}

type truncateCall struct {
	leadershipTermID int64
	logPosition      int64
}

type trackCatchupCall struct {
	memberID    int32
	logPosition int64
	flags       uint8
}

type publishCommitPositionCall struct {
	commitPosition   int64
	leadershipTermID int64
}

func (h *recordingElectionHost) Role() consensus.Role { return h.role }

func (h *recordingElectionHost) RoleSet(role consensus.Role) { h.role = role }

func (h *recordingElectionHost) AddLogPublication(appendPosition int64) (int32, error) {
	h.addLogPublicationCalls = append(h.addLogPublicationCalls, appendPosition)
	return h.logSessionID, h.addLogPublicationErr
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

func (h *recordingElectionHost) TruncateLogEntry(leadershipTermID, logPosition int64) error {
	h.truncateCalls = append(h.truncateCalls, truncateCall{
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
	})
	return h.truncateErr
}

func (h *recordingElectionHost) IsCatchupNearLive(position int64) bool {
	h.catchupNearLiveCalls = append(h.catchupNearLiveCalls, position)
	return h.catchupNearLive
}

func (h *recordingElectionHost) LiveLogDestination() string { return h.liveLogDestination }

// AddLiveLogDestination records a call. Fails addLiveLogDestinationFailLeft
// times without setting LiveLogDestination, then stores a non-empty live URI.
func (h *recordingElectionHost) AddLiveLogDestination() {
	h.addLiveLogDestinationCalls++
	if h.addLiveLogDestinationFailLeft > 0 {
		h.addLiveLogDestinationFailLeft--
		return
	}
	if h.liveLogDestination == "" {
		h.liveLogDestination = "aeron:udp?endpoint=live-log"
	}
}

func (h *recordingElectionHost) NewLogReplication(
	leaderArchiveEndpoint, responseArchiveEndpoint string,
	leaderRecordingID, stopPosition, nowNs int64,
) election.LogReplication {
	h.newLogReplicationCalls = append(h.newLogReplicationCalls, newLogReplicationCall{
		leaderArchiveEndpoint:   leaderArchiveEndpoint,
		responseArchiveEndpoint: responseArchiveEndpoint,
		leaderRecordingID:       leaderRecordingID,
		stopPosition:            stopPosition,
		nowNs:                   nowNs,
	})
	if h.newLogReplication != nil {
		return h.newLogReplication(
			leaderArchiveEndpoint,
			responseArchiveEndpoint,
			leaderRecordingID,
			stopPosition,
			nowNs,
		)
	}
	return nil
}

func (h *recordingElectionHost) CatchupInitiated(nowNs int64) {
	h.catchupInitiatedCalls = append(h.catchupInitiatedCalls, nowNs)
}

func (h *recordingElectionHost) CatchupPoll(limitPosition, nowNs int64) (int, error) {
	h.catchupPollCalls = append(h.catchupPollCalls, catchupPollCall{
		limitPosition: limitPosition,
		nowNs:         nowNs,
	})
	if h.catchupPollHook != nil {
		h.catchupPollHook(limitPosition, nowNs)
	}
	return h.catchupPollResult, h.catchupPollErr
}

func (h *recordingElectionHost) EnsureCatchupLogDestination(string) string {
	if h.ensureCatchupLogDestination != "" {
		h.catchupLogDestination = h.ensureCatchupLogDestination
	}
	return h.ensureCatchupLogDestination
}

func (h *recordingElectionHost) CommitPosition() int64 { return h.commitPosition }

func (h *recordingElectionHost) IsLogJoinedForSession(int32) bool { return h.logJoined }

func (h *recordingElectionHost) CatchupLogDestination() string { return h.catchupLogDestination }

func (h *recordingElectionHost) RemoveCatchupLogDestination() {
	h.removeCatchupLogDestinationCalls++
	h.catchupLogDestination = ""
}

func (h *recordingElectionHost) SetLogRecordingID(recordingID int64) {
	h.setLogRecordingIDCalls = append(h.setLogRecordingIDCalls, recordingID)
	h.localLogRecordingID = recordingID
}

// NewLogReplay returns an immediately done replay so leader and follower
// replay paths advance to the target position on the next tick.
func (h *recordingElectionHost) NewLogReplay(logPosition, appendPosition int64) election.LogReplay {
	if h.newLogReplay != nil {
		return h.newLogReplay(logPosition, appendPosition)
	}
	return &spyLogReplay{position: appendPosition, done: true}
}

type spyLogReplay struct {
	position   int64
	done       bool
	closeCalls int
	doWork     func(nowNs int64) int
}

func (r *spyLogReplay) DoWork(nowNs int64) int {
	if r.doWork != nil {
		return r.doWork(nowNs)
	}
	return 0
}

func (r *spyLogReplay) Position() int64 { return r.position }

func (r *spyLogReplay) IsDone() bool { return r.done }

func (r *spyLogReplay) Close() { r.closeCalls++ }

func (h *recordingElectionHost) IsTerminating() bool { return h.terminating }

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
	logPosition int64,
	flags uint8,
) {
	h.trackCatchupCalls = append(h.trackCatchupCalls, trackCatchupCall{
		memberID:    member.ID,
		logPosition: logPosition,
		flags:       flags,
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

// installLeaderForTest installs a NewLeadershipTerm from member 1, then sets
// state. Deadlines count from the install.
func installLeaderForTest(
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	state election.ElectionState,
) *election.Election {
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerBallot)
	e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID))
	election.SetStateForTest(e, state)
	return e
}

// driveFollowerLogJoinToReady adopts a successor term via NewLeadershipTerm,
// replays, and joins the follower log, asserting the FollowerReady transition
// and returning the adopted term. Callers set deps.RecordingLog before the
// drive; Host and Handler are installed here.
func driveFollowerLogJoinToReady(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	clock *FakeClock,
) int64 {
	t.Helper()

	deps.Host = &recordingElectionHost{
		clock:                   clock,
		localLogRecordingID:     testRecordingID,
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition,
	}
	deps.Handler = &recordingAgentHandler{joinFollowerReturn: true}
	e := election.NewElection(cfg, deps)

	adoptedTerm := cfg.LeadershipTermID + 2
	election.SetStateForTest(e, election.ElectionStateCanvass)
	e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, adoptedTerm))
	require.Equal(t, election.ElectionStateFollowerReplay, e.State())
	require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))

	election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerReady, e.State())
	require.Equal(t, adoptedTerm, election.LogLeadershipTermIDForTest(e))
	return adoptedTerm
}

// forceFollowerCatchup installs a NewLeadershipTerm that records the notified
// commit and catchup join bounds, then injects FOLLOWER_CATCHUP.
//
// joinAhead is added to cfg.AppendPosition for fields.LogPosition. Caller must
// wire deps.Host before the call.
//
// Returns: the election and the recorded join position.
func forceFollowerCatchup(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	commitPosition, joinAhead int64,
) (*election.Election, int64) {
	t.Helper()

	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)
	joinPos := cfg.AppendPosition + joinAhead
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.CommitPosition = commitPosition
	fields.LogPosition = joinPos
	e.OnNewLeadershipTerm(fields)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
	return e, joinPos
}

// driveToFollowerCatchupInit installs a leader term whose live log position is
// ahead of this member, routing the follower through the no-replay branch into
// FOLLOWER_CATCHUP_INIT. Returns the catchup join position.
//
// Sets catchupEndpoint on this member and clears recorded publisher traffic, so
// the caller sees catchup sends only.
func driveToFollowerCatchupInit(fx *electionFixture, catchupEndpoint string) int64 {
	fx.t.Helper()

	e := fx.election
	fx.doWork()
	require.Equal(fx.t, election.ElectionStateCanvass, e.State())

	catchupJoinPosition := fx.cfg.AppendPosition + 100
	fields := installNewLeadershipTermFields(fx.cfg, 1, fx.cfg.LeadershipTermID)
	fields.LogPosition = catchupJoinPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(fx.t, election.ElectionStateFollowerReplay, e.State())

	fx.doWork()
	require.Equal(fx.t, election.ElectionStateFollowerCatchupInit, e.State())

	thisMember := fx.member(fx.cfg.MemberID)
	require.NotNil(fx.t, thisMember)
	thisMember.CatchupEndpoint = catchupEndpoint

	fx.publisher.Reset()
	return catchupJoinPosition
}

// followerLogReplicationFields builds an installing NewLeadershipTerm whose
// successor term base sits past this node's append position.
func followerLogReplicationFields(
	cfg election.ElectionConfig,
	nextTermBase int64,
	nextLogPosition int64,
) codecs.NewLeadershipTermFields {
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.TermBaseLogPosition = cfg.AppendPosition + 100
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = nextTermBase
	fields.NextLogPosition = nextLogPosition
	return fields
}

// driveToFollowerLogReplication installs a successor term whose base position
// this node has not reached, routing the follower into
// FOLLOWER_LOG_REPLICATION.
func driveToFollowerLogReplication(
	t *testing.T,
	e *election.Election,
	cfg election.ElectionConfig,
	nextTermBase, nextLogPosition int64,
) {
	t.Helper()

	election.SetStateForTest(e, election.ElectionStateCanvass)
	e.OnNewLeadershipTerm(followerLogReplicationFields(cfg, nextTermBase, nextLogPosition))
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

// hostWithLogReplication wires a recordingElectionHost whose replication
// factory returns the given helper.
func hostWithLogReplication(
	clock *FakeClock,
	lr election.LogReplication,
) *recordingElectionHost {
	return &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			return lr
		},
	}
}

// replicationStopAhead puts the leader's next term base this far past the
// append position, so the install routes into FOLLOWER_LOG_REPLICATION.
const replicationStopAhead int64 = 50

// installNewLeadershipTermFields builds a NewLeadershipTerm anchored at cfg's
// baseline log term and position with null successor-term sentinels, matching
// what a leader with no recording log publishes.
func installNewLeadershipTermFields(
	cfg election.ElectionConfig,
	leaderID int32,
	term int64,
) codecs.NewLeadershipTermFields {
	return codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     cfg.LeadershipTermID,
		NextLeadershipTermID:    election.NullValue,
		NextTermBaseLogPosition: election.NullPosition,
		NextLogPosition:         election.NullPosition,
		LeadershipTermID:        term,
		TermBaseLogPosition:     cfg.LogPosition,
		LogPosition:             cfg.LogPosition,
		LeaderMemberID:          leaderID,
		LogSessionID:            fixtureLogSessionID,
	}
}

// countingLogReplicationForTest is a duty-cycle double for the
// FOLLOWER_LOG_REPLICATION and Init stop tests. Position changes only when a
// test sets it.
//
// Behaviour:
//   - DoWork returns work, or err once. The error closes the double unless
//     holdOpen is true. A production helper also closes itself on failure.
//   - Close counts each call and closes the double unless holdOpen is true.
//   - OnSignal counts each call.
type countingLogReplicationForTest struct {
	doWorkCalls int
	signalCalls int
	work        int
	err         error
	done        bool
	recordingID int64
	position    int64
	closeCalls  int
	closed      bool
	holdOpen    bool
}

func (c *countingLogReplicationForTest) DoWork(int64) (int, error) {
	c.doWorkCalls++
	if err := c.err; err != nil {
		c.err = nil
		c.closed = !c.holdOpen
		return 0, err
	}
	return c.work, nil
}

func (c *countingLogReplicationForTest) RecordingID() int64 { return c.recordingID }

func (c *countingLogReplicationForTest) Position() int64 { return c.position }

func (c *countingLogReplicationForTest) IsDone() bool { return c.done }

func (c *countingLogReplicationForTest) OnSignal(int64, int64, int64, election.RecordingSignal) {
	c.signalCalls++
}

func (c *countingLogReplicationForTest) Close() {
	c.closeCalls++
	c.closed = !c.holdOpen
}

func (c *countingLogReplicationForTest) IsClosed() bool { return c.closed }

// truncateInstallErrs holds the inject errors for one truncate install round.
type truncateInstallErrs struct {
	truncate error
	commit   error
	force    error
}

// truncateInstallResult holds the election, spies, and pre-install tracked
// positions after one OnNewLeadershipTerm that took the truncate path.
type truncateInstallResult struct {
	e            *election.Election
	host         *recordingElectionHost
	spy          *recordingLogCoherencySpy
	termID       int64
	appendBefore int64
	termBefore   int64
	fields       codecs.NewLeadershipTermFields
}

const truncateInstallBase int64 = 128

// runTruncateInstall calls OnNewLeadershipTerm with next-term base below the
// append position so the install takes the truncate path.
//
// hostCommit seeds Host.CommitPosition. HandleError syncs logPosition from that
// value. On truncate success, logPosition ends capped at the truncate end when
// host commit is higher.
func runTruncateInstall(
	t *testing.T,
	errs truncateInstallErrs,
	hostCommit int64,
) truncateInstallResult {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	return runTruncateInstallOver(t, cfg, deps, clock, errs, hostCommit)
}

// runTruncateInstallOver runs runTruncateInstall over cfg and deps.
func runTruncateInstallOver(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	clock *FakeClock,
	errs truncateInstallErrs,
	hostCommit int64,
) truncateInstallResult {
	t.Helper()

	cfg.LogPosition = cfg.AppendPosition
	host := &recordingElectionHost{
		clock:               clock,
		localLogRecordingID: 42,
		truncateErr:         errs.truncate,
		commitPosition:      hostCommit,
	}
	deps.Host = host
	spy := &recordingLogCoherencySpy{commitErr: errs.commit, forceErr: errs.force}
	deps.RecordingLog = spy

	// Init reruns prepare on a retry drive; a zero return would clobber the
	// append position and skip the truncate branch on the second install.
	deps.Handler = &recordingAgentHandler{appendPositionReturn: election.NullPosition}
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	result := truncateInstallResult{
		e:            e,
		host:         host,
		spy:          spy,
		termID:       cfg.LeadershipTermID,
		appendBefore: e.AppendPosition(),
		termBefore:   election.LogLeadershipTermIDForTest(e),
	}
	require.Less(t, truncateInstallBase, result.appendBefore)

	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = truncateInstallBase
	result.fields = fields
	e.OnNewLeadershipTerm(fields)
	return result
}

// replicationFixture is one election in FOLLOWER_LOG_REPLICATION. The first
// Init pass has run, so a later Init pass stops a helper.
type replicationFixture struct {
	cfg          election.ElectionConfig
	clock        *FakeClock
	host         *recordingElectionHost
	handler      *recordingAgentHandler
	spy          *recordingLogCoherencySpy
	publisher    election.Publisher
	e            *election.Election
	stopPosition int64
}

// newReplicationFixture builds a replicationFixture over an SbePublisher.
func newReplicationFixture(t *testing.T, lr election.LogReplication) *replicationFixture {
	t.Helper()
	return newReplicationFixtureWith(t, lr, &election.SbePublisher{})
}

// newReplicationFixtureWith builds a replicationFixture over publisher.
func newReplicationFixtureWith(
	t *testing.T,
	lr election.LogReplication,
	publisher election.Publisher,
) *replicationFixture {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	deps.Publisher = publisher
	return newReplicationFixtureOver(t, cfg, deps, clock, lr)
}

// newReplicationFixtureOver builds a replicationFixture over cfg and deps.
// Prepare returns the append position, so the first Init pass keeps it.
func newReplicationFixtureOver(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	clock *FakeClock,
	lr election.LogReplication,
) *replicationFixture {
	t.Helper()

	host := hostWithLogReplication(clock, lr)
	host.localLogRecordingID = 42
	handler := &recordingAgentHandler{appendPositionReturn: cfg.AppendPosition}
	spy := &recordingLogCoherencySpy{}
	deps.Host = host
	deps.Handler = handler
	deps.RecordingLog = spy
	e := election.NewElection(cfg, deps)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	stopPosition := cfg.AppendPosition + replicationStopAhead
	driveToFollowerLogReplication(t, e, cfg, stopPosition, election.NullPosition)
	return &replicationFixture{
		cfg:          cfg,
		clock:        clock,
		host:         host,
		handler:      handler,
		spy:          spy,
		publisher:    deps.Publisher,
		e:            e,
		stopPosition: stopPosition,
	}
}

// appendPositions returns the AppendPosition sends to member 1, the leader.
func (fx *replicationFixture) appendPositions(t *testing.T) []int64 {
	t.Helper()

	pub, ok := fx.publisher.(*election.SbePublisher)
	require.True(t, ok)
	var positions []int64
	for _, msg := range pub.Sent {
		if msg.TemplateID != clusterwire.AppendPositionSbeTemplateID {
			continue
		}
		fields, ok := msg.Fields.(codecs.AppendPositionFields)
		require.True(t, ok)
		require.Equal(t, int32(1), msg.Dst.MemberID)
		require.Equal(t, fx.e.LeadershipTermID(), fields.LeadershipTermID)
		require.Equal(t, fx.cfg.MemberID, fields.FollowerMemberID)
		require.Equal(t, consensus.AppendPositionFlagNone, fields.Flags)
		positions = append(positions, fields.LogPosition)
	}
	return positions
}
