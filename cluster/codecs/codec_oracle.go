package codecs

import (
	"math"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// ClusterCodecMaxVarLen sizes cluster codec var-field slots in test and
// cross-language wire oracles. Largest committed fixture var field is 32 B.
const ClusterCodecMaxVarLen = 256

var SessionEventLeaderHeartbeatMinVersion = uint16((&clusterwire.SessionEvent{}).
	LeaderHeartbeatTimeoutNsSinceVersion())

var SessionConnectClientInfoMinVersion = uint16((&clusterwire.SessionConnectRequest{}).
	ClientInfoSinceVersion())

var SessionEventVersionMinVersion = uint16((&clusterwire.SessionEvent{}).
	VersionSinceVersion())

var SessionConnectVersionMinVersion = uint16((&clusterwire.SessionConnectRequest{}).
	VersionSinceVersion())

// NewLeadershipTermEventTimeUnitMinVersion is the actingVersion that admits
// optional timeUnit and appVersion on NewLeadershipTermEvent (sinceVersion 4).
//
// Derived from the generated flyweight TimeUnitSinceVersion.
var NewLeadershipTermEventTimeUnitMinVersion = uint16(
	(&clusterwire.NewLeadershipTermEvent{}).TimeUnitSinceVersion(),
)

// SessionEventOracleFields applies LeaderHeartbeatTimeoutNs oracle rules.
//
// math.MinInt64 omits on encode at actingVersion >= 13; decode reads 0. Below
// that version the field is absent and reads math.MinInt64.
func SessionEventOracleFields(fields SessionEventFields, actingVersion uint16) SessionEventFields {
	if actingVersion < SessionEventLeaderHeartbeatMinVersion {
		fields.LeaderHeartbeatTimeoutNs = math.MinInt64
		return fields
	}
	if fields.LeaderHeartbeatTimeoutNs == math.MinInt64 {
		fields.LeaderHeartbeatTimeoutNs = 0
	}
	return fields
}

// NewLeadershipTermEventOracleFields applies optional-field oracle rules for
// NewLeadershipTermEvent (template 24).
//
// Below NewLeadershipTermEventTimeUnitMinVersion, timeUnit and appVersion are
// absent on wire: TimeUnit becomes NULL_VALUE and AppVersion becomes 0.
// At and above that version, encode always writes both fields (including
// AppVersion 0), so fields roundtrip as-is.
func NewLeadershipTermEventOracleFields(
	fields NewLeadershipTermEventFields,
	actingVersion uint16,
) NewLeadershipTermEventFields {
	if actingVersion < NewLeadershipTermEventTimeUnitMinVersion {
		fields.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
		fields.AppVersion = 0
	}
	return fields
}

// JoinLogIsStandbyMinVersion is the actingVersion that admits IsStandby on
// JoinLog (sinceVersion 16). Derived from the generated flyweight
// IsStandbySinceVersion.
var JoinLogIsStandbyMinVersion = uint16(
	(&clusterwire.JoinLog{}).IsStandbySinceVersion(),
)

// JoinLogOracleFields applies IsStandby oracle rules for JoinLog (template 40).
//
// Below JoinLogIsStandbyMinVersion the field is absent on the wire and decodes
// as BooleanType_NULL_VALUE. At and above that version encode always writes
// the field, including FALSE, so fields roundtrip as-is.
func JoinLogOracleFields(fields JoinLogFields, actingVersion uint16) JoinLogFields {
	if actingVersion < JoinLogIsStandbyMinVersion {
		fields.IsStandby = clusterwire.BooleanType_NULL_VALUE
	}
	return fields
}

// ClusterActionRequestFlagsMinVersion is the actingVersion that admits Flags
// on ClusterActionRequest (sinceVersion 11). Derived from the generated
// flyweight FlagsSinceVersion.
var ClusterActionRequestFlagsMinVersion = uint16(
	(&clusterwire.ClusterActionRequest{}).FlagsSinceVersion(),
)

// SnapshotMarkerTimeUnitMinVersion is the actingVersion that admits optional
// timeUnit and appVersion on SnapshotMarker (sinceVersion 4). Derived from the
// generated flyweight TimeUnitSinceVersion.
var SnapshotMarkerTimeUnitMinVersion = uint16(
	(&clusterwire.SnapshotMarker{}).TimeUnitSinceVersion(),
)

// SnapshotMarkerOracleFields applies optional-field oracle rules for
// SnapshotMarker (template 100).
//
// Below SnapshotMarkerTimeUnitMinVersion the wire omits timeUnit and
// appVersion. TimeUnit becomes NULL_VALUE and AppVersion becomes 0. At and
// above that version fields roundtrip as-is.
func SnapshotMarkerOracleFields(
	fields SnapshotMarkerFields,
	actingVersion uint16,
) SnapshotMarkerFields {
	if actingVersion < SnapshotMarkerTimeUnitMinVersion {
		fields.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
		fields.AppVersion = 0
	}
	return fields
}

// ConsensusModuleNextServiceSessionIDMinVersion is the actingVersion that
// admits optional nextServiceSessionId, logServiceSessionId and
// pendingMessageCapacity on ConsensusModule (sinceVersion 3). Derived from the
// generated flyweight NextServiceSessionIdSinceVersion.
var ConsensusModuleNextServiceSessionIDMinVersion = uint16(
	(&clusterwire.ConsensusModule{}).NextServiceSessionIdSinceVersion(),
)

// ConsensusModuleOracleFields applies optional-field oracle rules for
// ConsensusModule (template 105).
//
// Below ConsensusModuleNextServiceSessionIDMinVersion the wire omits the
// three optional fields. NextServiceSessionID and LogServiceSessionID become
// math.MinInt64. PendingMessageCapacity becomes 0. At and above that version
// fields roundtrip as-is.
func ConsensusModuleOracleFields(
	fields ConsensusModuleFields,
	actingVersion uint16,
) ConsensusModuleFields {
	if actingVersion < ConsensusModuleNextServiceSessionIDMinVersion {
		fields.NextServiceSessionID = math.MinInt64
		fields.LogServiceSessionID = math.MinInt64
		fields.PendingMessageCapacity = 0
	}
	return fields
}
