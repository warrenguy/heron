package election

import (
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/consensus"
)

// RecordingSignal is [archivewire.RecordingSignal].
type RecordingSignal = archivewire.RecordingSignal

// LogReplay is [consensus.LogReplay].
type LogReplay = consensus.LogReplay

// LogReplication is [consensus.LogReplication].
type LogReplication = consensus.LogReplication

// CatchupPositionFields is [codecs.CatchupPositionFields].
type CatchupPositionFields = codecs.CatchupPositionFields

// ElectionHost is the election-to-agent duty-cycle boundary.
//
// Implemented by [HostAdapter]; mocked in election tests.
type ElectionHost interface {
	Role() consensus.Role
	RoleSet(role consensus.Role)

	QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64
	UpdateLeaderPosition(nowNs, appendPosition, quorumPosition int64) int
	TimeOfLastLeaderUpdateNs() int64

	UpdateMemberLogPosition(member *Member, leadershipTermID, logPosition int64)

	// TrackCatchupCompletion drives leader-side catchup stop when logPosition
	// reaches the leader log. logPosition is the AppendPosition value, not
	// member.LogPosition. Flagged messages keep member.LogPosition stale.
	TrackCatchupCompletion(
		member *Member,
		logPosition int64,
		flags uint8,
	)

	NewLogReplay(logPosition, appendPosition int64) LogReplay
	NewLogReplication(
		leaderArchiveEndpoint, responseArchiveEndpoint string,
		leaderRecordingID, stopPosition, nowNs int64,
	) LogReplication

	CatchupPoll(limitPosition, nowNs int64) (int, error)
	CatchupInitiated(nowNs int64)
	IsCatchupNearLive(position int64) bool

	// LiveLogDestination returns the live log receive-destination URI.
	// Empty when the live feed is not attached.
	LiveLogDestination() string

	// AddLiveLogDestination adds the live log channel as a receive destination.
	// No-op when the live feed is already attached. Idempotent.
	//
	// On failure LiveLogDestination stays empty. The next tick retries.
	AddLiveLogDestination()

	// EnsureCatchupLogDestination opens the follower catchup receive path for
	// catchupEndpoint. Returns the destination channel URI, or empty when not
	// ready. Idempotent once open.
	EnsureCatchupLogDestination(catchupEndpoint string) string

	// CatchupLogDestination returns the catchup receive-destination URI.
	// Empty when catchup is not active.
	CatchupLogDestination() string

	// RemoveCatchupLogDestination removes the catchup receive destination and
	// restores the live destination when the live feed is absent. Idempotent.
	RemoveCatchupLogDestination()

	// CommitPosition returns the local commit value (0 when unset).
	CommitPosition() int64

	// IsLogJoinedForSession reports whether the bound log image carries
	// logSessionID.
	IsLogJoinedForSession(logSessionID int32) bool

	// OnCatchupPosition starts leader-side catchup for the follower in fields.
	// May no-op when the host refuses the request. Does not return an error.
	OnCatchupPosition(fields CatchupPositionFields)

	PublishCommitPosition(commitPosition, leadershipTermID int64)

	// AddLogPublication returns the log publication session id for the new
	// term, or null session id (-1) when the host holds no publication. The
	// host may rebuild the publication at appendPosition first. On a rebuild
	// error the election discards the id and resets.
	AddLogPublication(appendPosition int64) (int32, error)
	AppendNewLeadershipTermEvent(nowNs int64) bool

	PollArchiveEvents() int
	LocalLogRecordingID() int64
	// SetLogRecordingID stores the local log recording id.
	//
	// Production hosts do not overwrite while a cluster log recording is running.
	SetLogRecordingID(recordingID int64)

	// TruncateLogEntry removes every recorded byte at or above logPosition and
	// lowers the log adapter's reported position to it. Does not write
	// recording.log; the election lowers the term entry.
	//
	// Returns an error when the truncate did not run. The caller must not lower
	// any tracked position on an error.
	TruncateLogEntry(leadershipTermID, logPosition int64) error
	AwaitLocalSocketsClosed(registrationID int64)

	// FollowerLogJoinReady reports whether the live log image for logSessionID
	// is available to join and returns its join parameters. The election
	// overwrites IsLeaderStartup with its own value before the join.
	FollowerLogJoinReady(logSessionID int32) (consensus.FollowerLogJoinParams, bool)

	// SetLeaderMemberID sets the known leader id for AppendPosition publish and
	// steady notify gates. Member id 0 is valid; use NullMemberID for unset.
	SetLeaderMemberID(id int32)

	// ApplyLeadershipTerm copies the adopted term onto the agent for steady
	// ingress gates after ElectionComplete (follower install handoff).
	ApplyLeadershipTerm(term int64)
}
