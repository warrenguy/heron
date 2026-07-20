package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type orderRunner struct {
	n int
}

func (r *orderRunner) DoWork(int64) int { return r.n }

func (r *orderRunner) HandleConsensusMessage([]byte, int) bool { return false }

type stubPositionTable struct{}

func (s *stubPositionTable) UpdateMemberPosition(
	memberID int32,
	leadershipTermID int64,
	logPosition int64,
) {
}

func (s *stubPositionTable) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	return appendPosition
}

type stubConsensusEgress struct{}

func (s *stubConsensusEgress) PublishCommitPosition(commitPosition, leadershipTermID int64) {
}

func (s *stubConsensusEgress) PublishAppendPosition(
	leadershipTermID int64,
	logPosition int64,
	flags uint8,
) bool {
	return true
}

func TestProposeMaxCommit_monotonic(t *testing.T) {
	commit := new(atomic.Int64)
	a := NewAgent(AgentConfig{Commit: commit})

	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(10), a.ProposeMaxCommit(10))
	require.Equal(t, int64(10), a.CommitPosition())
	require.Equal(t, int64(10), a.ProposeMaxCommit(5))
	require.Equal(t, int64(10), a.CommitPosition())
	require.Equal(t, int64(20), a.ProposeMaxCommit(20))
	require.Equal(t, int64(20), a.CommitPosition())
}

func TestProposeMaxCommit_nilCommit(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(0), a.ProposeMaxCommit(42))
	require.Equal(t, int64(0), a.CommitPosition())
}

func TestUpdateLeaderPosition_nilCommit_workZero(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, 0, a.UpdateLeaderPosition(1, 100, 50))
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
}

func TestUpdateLeaderPosition_advancesCommit_workOne(t *testing.T) {
	commit := new(atomic.Int64)
	a := NewAgent(AgentConfig{Commit: commit})
	require.Equal(t, 1, a.UpdateLeaderPosition(1, 100, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, 0, a.UpdateLeaderPosition(2, 100, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, 1, a.UpdateLeaderPosition(3, 100, 75))
	require.Equal(t, int64(75), a.CommitPosition())
	require.Equal(t, int64(100), a.ProposeMaxCommit(100))
	require.Equal(t, 0, a.UpdateLeaderPosition(4, 100, 50))
	require.Equal(t, int64(100), a.CommitPosition())
}

func TestUpdateLeaderPosition_doesNotWriteTimeOfLastLeaderUpdateNs(t *testing.T) {
	commit := new(atomic.Int64)
	a := NewAgent(AgentConfig{Commit: commit})
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, 1, a.UpdateLeaderPosition(9_000_000_000, 100, 50))
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	a.timeOfLastLeaderUpdateNs = 42
	require.Equal(t, 0, a.UpdateLeaderPosition(9_000_000_001, 100, 50))
	require.Equal(t, int64(42), a.TimeOfLastLeaderUpdateNs())
}

func TestSetHostSeams_wiresAndClears(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())

	pt := &stubPositionTable{}
	eg := &stubConsensusEgress{}
	a.SetHostSeams(pt, eg)
	require.Equal(t, PositionTable(pt), a.Positions())
	require.Equal(t, ConsensusEgress(eg), a.Egress())

	a.SetHostSeams(nil, nil)
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())
}

func TestSetElection_andGetter(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Election())
	r := &orderRunner{n: 1}
	a.SetElection(r)
	require.Equal(t, ElectionRunner(r), a.Election())
	a.SetElection(nil)
	require.Nil(t, a.Election())
}

func TestHostDelegate_stubsSmoke(t *testing.T) {
	a := NewAgent(AgentConfig{})

	require.Equal(t, RoleFollower, a.Role())
	a.RoleSet(RoleCandidate)
	require.Equal(t, RoleCandidate, a.Role())

	a.LiveLogDestinationSet("live")
	require.Equal(t, "live", a.LiveLogDestination())
	a.CatchupLogDestinationSet("catchup")
	require.Equal(t, "catchup", a.CatchupLogDestination())

	require.Equal(t, int32(-1), a.AddLogPublication(100))
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	a.SetLogRecordingID(99)
	require.Equal(t, int64(99), a.LocalLogRecordingID())

	params, ready := a.FollowerLogJoinReady(7)
	require.False(t, ready)
	require.Equal(t, FollowerLogJoinParams{}, params)

	replay := a.NewLogReplay(10, 200)
	require.NotNil(t, replay)
	require.True(t, replay.IsDone())
	require.Equal(t, int64(200), replay.Position())
	require.Equal(t, 0, replay.DoWork(1))

	require.Nil(t, a.NewLogReplication("leader", "response", 1, 2, 3))
	require.Equal(t, 0, a.CatchupPoll(0, 0))
	a.CatchupInitiated(0)
	a.StopAllCatchups()
	require.False(t, a.IsCatchupNearLive(0))
	require.False(t, a.AppendNewLeadershipTermEvent(0))
	require.Equal(t, 0, a.PollArchiveEvents())
	a.TruncateLogEntry(0, 0)
	a.AwaitLocalSocketsClosed(0)
}

func TestAgentHandler_prepareJoinComplete(t *testing.T) {
	r := &orderRunner{n: 1}
	pt := &stubPositionTable{}
	eg := &stubConsensusEgress{}
	a := NewAgent(AgentConfig{
		Election:  r,
		Positions: pt,
		Egress:    eg,
		Commit:    new(atomic.Int64),
		Role:      RoleLeader,
	})

	got := a.PrepareForNewLeadership(1234, 99)
	require.Equal(t, int64(1234), got)
	require.Equal(t, RoleFollower, a.Role())

	a.JoinLogAsLeader(1, 10, 2, true)
	require.Equal(t, RoleLeader, a.Role())

	require.False(t, a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 3}, 1))

	a.ElectionComplete(1)
	require.Nil(t, a.Election())
	require.Equal(t, PositionTable(pt), a.Positions())
	require.Equal(t, ConsensusEgress(eg), a.Egress())
}

func TestNewAgent_defaults(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Nil(t, a.Election())
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())

	require.Equal(t, RoleLeader, NewAgent(AgentConfig{Role: RoleLeader}).Role())
}
