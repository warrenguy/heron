package sbe

import (
	"math"

	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

// ClusterCodecMaxVarLen sizes cluster codec var-field slots in test and
// cross-language wire oracles. Largest committed fixture var field is 32 B.
const ClusterCodecMaxVarLen = 256

var SessionEventLeaderHeartbeatMinVersion = uint16(
	(&gensbe.SessionEvent{}).LeaderHeartbeatTimeoutNsSinceVersion(),
)

var SessionConnectClientInfoMinVersion = uint16(
	(&gensbe.SessionConnectRequest{}).ClientInfoSinceVersion(),
)

var SessionEventVersionMinVersion = uint16(
	(&gensbe.SessionEvent{}).VersionSinceVersion(),
)

var SessionConnectVersionMinVersion = uint16(
	(&gensbe.SessionConnectRequest{}).VersionSinceVersion(),
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
