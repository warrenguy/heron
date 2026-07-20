package election

import (
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
)

// RecordingSignal is the archive recording signal code.
type RecordingSignal = archivewire.RecordingSignal

// LogReplay aliases [consensus.LogReplay] so election call sites keep the short
// name.
type LogReplay = consensus.LogReplay

// LogReplication aliases [consensus.LogReplication] so election call sites keep
// the short name.
type LogReplication = consensus.LogReplication

// ElectionHost is the election-to-agent duty-cycle boundary.
// Implemented by ConsensusModuleAgent; mocked in election tests.
type ElectionHost interface {
	Role() consensus.Role
	RoleSet(role consensus.Role)

	QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64
	UpdateLeaderPosition(nowNs, appendPosition, quorumPosition int64) int
	TimeOfLastLeaderUpdateNs() int64

	UpdateMemberLogPosition(member *Member, leadershipTermID, logPosition int64)
	TrackCatchupCompletion(member *Member, leadershipTermID int64, flags uint8)

	NewLogReplay(logPosition, appendPosition int64) LogReplay
	NewLogReplication(
		leaderArchiveEndpoint, responseArchiveEndpoint string,
		leaderRecordingID, stopPosition, nowNs int64,
	) LogReplication

	CatchupPoll(limitPosition, nowNs int64) int
	CatchupInitiated(nowNs int64)
	StopAllCatchups()
	IsCatchupNearLive(position int64) bool

	LiveLogDestination() string
	LiveLogDestinationSet(destination string)
	CatchupLogDestination() string
	CatchupLogDestinationSet(destination string)

	PublishCommitPosition(commitPosition, leadershipTermID int64)
	AddLogPublication(appendPosition int64) int32
	AppendNewLeadershipTermEvent(nowNs int64) bool

	PollArchiveEvents() int
	LocalLogRecordingID() int64
	SetLogRecordingID(recordingID int64)

	// TruncateLogEntry records an explicit cluster-log truncation request.
	// Production agents log and schedule archive truncate; test hosts record
	// the call only. Does not mutate recording.log TERM rows.
	TruncateLogEntry(leadershipTermID, logPosition int64)
	AwaitLocalSocketsClosed(registrationID int64)

	// FollowerLogJoinReady reports whether the live log image for logSessionID
	// is available to join and returns its join parameters. The election
	// overwrites IsLeaderStartup with its own value before the join.
	FollowerLogJoinReady(logSessionID int32) (consensus.FollowerLogJoinParams, bool)
}
