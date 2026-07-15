package election

import (
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
)

// RecordingSignal is the archive recording signal code (io.aeron.archive.codecs.RecordingSignal).
type RecordingSignal = archivewire.RecordingSignal

// LogReplay is a stub duty-cycle helper for leader/follower replay paths.
// Replace with archive-backed implementation when agent wiring lands.
type LogReplay interface {
	DoWork(nowNs int64) int
	Position() int64
	IsDone() bool
}

// LogReplication is a stub duty-cycle helper for follower log replication.
type LogReplication interface {
	DoWork(nowNs int64) int
	RecordingID() int64
	IsDone() bool
	OnSignal(correlationID, recordingID, position int64, signal RecordingSignal)
}

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
	TruncateLogEntry(leadershipTermID, logPosition int64)
	AwaitLocalSocketsClosed(registrationID int64)

	// FollowerLogJoinReady reports whether the live log image for logSessionID
	// is available to join and returns its join parameters. The election
	// overwrites IsLeaderStartup with its own value before the join.
	FollowerLogJoinReady(logSessionID int32) (consensus.FollowerLogJoinParams, bool)
}
