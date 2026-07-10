package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

var (
	CanvassProtocolVersionMinVersion = uint16(
		(&clusterwire.CanvassPosition{}).ProtocolVersionSinceVersion(),
	)
	RequestVoteProtocolVersionMinVersion = uint16(
		(&clusterwire.RequestVote{}).ProtocolVersionSinceVersion(),
	)
	AppendPositionFlagsMinVersion = uint16(
		(&clusterwire.AppendPosition{}).FlagsSinceVersion(),
	)
	NewLeadershipTermCommitPositionMinVersion = uint16(
		(&clusterwire.NewLeadershipTerm{}).CommitPositionSinceVersion(),
	)
)

func CanvassPositionOracleFields(
	fields CanvassPositionFields,
	actingVersion uint16,
) CanvassPositionFields {
	if actingVersion < CanvassProtocolVersionMinVersion {
		fields.ProtocolVersion = 0
	}
	return fields
}

func RequestVoteOracleFields(fields RequestVoteFields, actingVersion uint16) RequestVoteFields {
	if actingVersion < RequestVoteProtocolVersionMinVersion {
		fields.ProtocolVersion = 0
	}
	return fields
}

func NewLeadershipTermOracleFields(
	fields NewLeadershipTermFields,
	actingVersion uint16,
) NewLeadershipTermFields {
	if actingVersion < NewLeadershipTermCommitPositionMinVersion {
		fields.CommitPosition = 0
	}
	return fields
}

func AppendPositionOracleFields(
	fields AppendPositionFields,
	actingVersion uint16,
) AppendPositionFields {
	if actingVersion < AppendPositionFlagsMinVersion {
		fields.Flags = 0
	}
	return fields
}
