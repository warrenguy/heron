package electiontest

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

// NoopAgentHandler is a zero-value AgentHandler stub for election wiring tests.
type NoopAgentHandler struct{}

func (NoopAgentHandler) PrepareForNewLeadership(int64, int64) (int64, error) {
	return 0, nil
}

func (NoopAgentHandler) JoinLogAsLeader(int64, int64, int32, bool) error {
	return nil
}

func (NoopAgentHandler) JoinLogAsFollower(
	consensus.FollowerLogJoinParams,
	int64,
) (bool, error) {
	return false, nil
}

func (NoopAgentHandler) ElectionComplete(int64) {}

// NoopElectionHost is a zero-value ElectionHost stub for election wiring tests.
type NoopElectionHost struct{}

func (NoopElectionHost) Role() consensus.Role { return consensus.RoleFollower }

func (NoopElectionHost) RoleSet(consensus.Role) {}

func (NoopElectionHost) QuorumPositionBoundedByLeaderLog(int64, int64) int64 { return 0 }

func (NoopElectionHost) UpdateLeaderPosition(int64, int64, int64) int { return 0 }

func (NoopElectionHost) TimeOfLastLeaderUpdateNs() int64 { return 0 }

func (NoopElectionHost) UpdateMemberLogPosition(*election.Member, int64, int64) {}

func (NoopElectionHost) TrackCatchupCompletion(*election.Member, int64, uint8) {}

func (NoopElectionHost) NewLogReplay(int64, int64) election.LogReplay { return nil }

func (NoopElectionHost) NewLogReplication(
	string,
	string,
	int64,
	int64,
	int64,
) election.LogReplication {
	return nil
}

func (NoopElectionHost) CatchupPoll(int64, int64) int { return 0 }

func (NoopElectionHost) CatchupInitiated(int64) {}

func (NoopElectionHost) StopAllCatchups() {}

func (NoopElectionHost) IsCatchupNearLive(int64) bool { return false }

func (NoopElectionHost) LiveLogDestination() string { return "" }

func (NoopElectionHost) LiveLogDestinationSet(string) {}

func (NoopElectionHost) CatchupLogDestination() string { return "" }

func (NoopElectionHost) CatchupLogDestinationSet(string) {}

func (NoopElectionHost) PublishCommitPosition(int64, int64) {}

func (NoopElectionHost) AddLogPublication(int64) int32 { return election.NullMemberID }

func (NoopElectionHost) AppendNewLeadershipTermEvent(int64) bool { return false }

func (NoopElectionHost) PollArchiveEvents() int { return 0 }

func (NoopElectionHost) LocalLogRecordingID() int64 { return election.NullValue }

func (NoopElectionHost) SetLogRecordingID(int64) {}

func (NoopElectionHost) TruncateLogEntry(int64, int64) {}

func (NoopElectionHost) AwaitLocalSocketsClosed(int64) {}

func (NoopElectionHost) FollowerLogJoinReady(int32) (consensus.FollowerLogJoinParams, bool) {
	return consensus.FollowerLogJoinParams{}, false
}

func (NoopElectionHost) SetLeaderMemberID(int32) {}

func (NoopElectionHost) ApplyLeadershipTerm(int64) {}

var _ consensus.AgentHandler = (*NoopAgentHandler)(nil)

var _ election.ElectionHost = (*NoopElectionHost)(nil)
