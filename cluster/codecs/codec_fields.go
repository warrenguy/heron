package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

type SessionMessageHeaderFields = IngressHeaderFields

type SessionEventFields struct {
	ClusterSessionID int64
	CorrelationID    int64
	LeadershipTermID int64
	LeaderMemberID   int32
	Code             clusterwire.EventCode
	Version          int32
	// LeaderHeartbeatTimeoutNs is math.MinInt64 when actingVersion < 13 (field absent).
	// When actingVersion >= 13 and the encoder omitted SetLeaderHeartbeatTimeoutNs,
	// the flyweight reads wire zeros and this decodes as 0 (not SbeNullValueInt64).
	LeaderHeartbeatTimeoutNs int64
	Detail                   []byte
}

type SessionConnectRequestFields struct {
	CorrelationID      int64
	ResponseStreamID   int32
	Version            int32
	ResponseChannel    []byte
	EncodedCredentials []byte
	ClientInfo         []byte
}

type SessionCloseRequestFields struct {
	LeadershipTermID int64
	ClusterSessionID int64
}

type SessionKeepAliveFields struct {
	LeadershipTermID int64
	ClusterSessionID int64
}

type NewLeaderEventFields struct {
	LeadershipTermID int64
	ClusterSessionID int64
	LeaderMemberID   int32
	IngressEndpoints []byte
}

type ChallengeFields struct {
	CorrelationID    int64
	ClusterSessionID int64
	EncodedChallenge []byte
}

type ChallengeResponseFields struct {
	CorrelationID      int64
	ClusterSessionID   int64
	EncodedCredentials []byte
}

type AdminRequestFields struct {
	LeadershipTermID int64
	ClusterSessionID int64
	CorrelationID    int64
	RequestType      clusterwire.AdminRequestType
	Payload          []byte
}

type AdminResponseFields struct {
	ClusterSessionID int64
	CorrelationID    int64
	RequestType      clusterwire.AdminRequestType
	ResponseCode     clusterwire.AdminResponseCode
	Message          []byte
	Payload          []byte
}

type CanvassPositionFields struct {
	LogLeadershipTermID int64
	LogPosition         int64
	LeadershipTermID    int64
	FollowerMemberID    int32
	// ProtocolVersion is 0 when actingVersion < 9 (field absent on wire).
	ProtocolVersion int32
}

type RequestVoteFields struct {
	LogLeadershipTermID int64
	LogPosition         int64
	CandidateTermID     int64
	CandidateMemberID   int32
	// ProtocolVersion is 0 when actingVersion < 9 (field absent on wire).
	ProtocolVersion int32
}

type VoteFields struct {
	CandidateTermID     int64
	LogLeadershipTermID int64
	LogPosition         int64
	CandidateMemberID   int32
	FollowerMemberID    int32
	Vote                clusterwire.BooleanType
}

type NewLeadershipTermFields struct {
	LogLeadershipTermID     int64
	NextLeadershipTermID    int64
	NextTermBaseLogPosition int64
	NextLogPosition         int64
	LeadershipTermID        int64
	TermBaseLogPosition     int64
	LogPosition             int64
	LeaderRecordingID       int64
	Timestamp               int64
	LeaderMemberID          int32
	LogSessionID            int32
	// AppVersion is version_t. Always written on encode, zero included:
	// exclusive TryClaim buffers are not zeroed.
	AppVersion int32
	IsStartup  clusterwire.BooleanType
	// CommitPosition is 0 when actingVersion < 15 (field absent on wire).
	CommitPosition int64
}

// NewLeadershipTermEventFields holds fixed body fields for NewLeadershipTermEvent
// (SBE template id 24, cluster log event).
type NewLeadershipTermEventFields struct {
	LeadershipTermID    int64
	LogPosition         int64
	Timestamp           int64
	TermBaseLogPosition int64
	LeaderMemberID      int32
	LogSessionID        int32
	// TimeUnit is written when actingVersion >= NewLeadershipTermEventTimeUnitMinVersion.
	// Always SetTimeUnit on encode at that version. Wire zero is MILLIS (0), not NULL_VALUE.
	TimeUnit clusterwire.ClusterTimeUnit
	// AppVersion is version_t. Always SetAppVersion when actingVersion >= min version
	// (including 0): exclusive TryClaim does not zero payload.
	AppVersion int32
}

type AppendPositionFields struct {
	LeadershipTermID int64
	LogPosition      int64
	FollowerMemberID int32
	// Flags is 0 when actingVersion < 8 (field absent on wire).
	Flags uint8
}

type CommitPositionFields struct {
	LeadershipTermID int64
	LogPosition      int64
	LeaderMemberID   int32
}

// CatchupPositionFields holds CatchupPosition body fields (template 56).
//
// CatchupEndpoint names the follower unicast endpoint the leader archive
// replays to.
type CatchupPositionFields struct {
	LeadershipTermID int64
	LogPosition      int64
	FollowerMemberID int32
	CatchupEndpoint  string
}

// StopCatchupFields holds StopCatchup body fields (template 57).
type StopCatchupFields struct {
	LeadershipTermID int64
	FollowerMemberID int32
}
