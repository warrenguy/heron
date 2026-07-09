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
