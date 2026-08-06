package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

func clusterFixedBlockLength(blockLength uint16) (minBlock, maxBlock uint16, ok bool) {
	return blockLength, blockLength, true
}

// ClusterRootBlockLengthBounds returns min and max blockLength accepted for templateID
// at actingVersion.
func ClusterRootBlockLengthBounds(
	templateID, actingVersion uint16,
) (minBlock, maxBlock uint16, ok bool) {
	switch templateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionMessageHeaderSbeBlockLength)

	case clusterwire.SessionEventSbeTemplateID:
		maxBlock = clusterwire.SessionEventSbeBlockLength

		switch {
		case actingVersion < SessionEventVersionMinVersion:
			return 32, maxBlock, true
		case actingVersion < SessionEventLeaderHeartbeatMinVersion:
			return 36, maxBlock, true
		default:
			return clusterFixedBlockLength(maxBlock)
		}

	case clusterwire.SessionConnectRequestSbeTemplateID:
		maxBlock = clusterwire.SessionConnectRequestSbeBlockLength

		if actingVersion < SessionConnectVersionMinVersion {
			return 12, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.SessionCloseRequestSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionCloseRequestSbeBlockLength)
	case clusterwire.SessionKeepAliveSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionKeepAliveSbeBlockLength)
	case clusterwire.NewLeaderEventSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.NewLeaderEventSbeBlockLength)

	case clusterwire.NewLeadershipTermEventSbeTemplateID:
		maxBlock = clusterwire.NewLeadershipTermEventSbeBlockLength
		if actingVersion < NewLeadershipTermEventTimeUnitMinVersion {
			// ids 1-6 only (timeUnit + appVersion sinceVersion 4).
			return 40, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.ChallengeSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.ChallengeSbeBlockLength)
	case clusterwire.ChallengeResponseSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.ChallengeResponseSbeBlockLength)
	case clusterwire.AdminRequestSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.AdminRequestSbeBlockLength)
	case clusterwire.AdminResponseSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.AdminResponseSbeBlockLength)

	case clusterwire.CanvassPositionSbeTemplateID:
		maxBlock = clusterwire.CanvassPositionSbeBlockLength

		if actingVersion < CanvassProtocolVersionMinVersion {
			return 28, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.RequestVoteSbeTemplateID:
		maxBlock = clusterwire.RequestVoteSbeBlockLength

		if actingVersion < RequestVoteProtocolVersionMinVersion {
			return 28, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.VoteSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.VoteSbeBlockLength)

	case clusterwire.NewLeadershipTermSbeTemplateID:
		maxBlock = clusterwire.NewLeadershipTermSbeBlockLength

		if actingVersion < NewLeadershipTermCommitPositionMinVersion {
			return 88, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.AppendPositionSbeTemplateID:
		maxBlock = clusterwire.AppendPositionSbeBlockLength

		if actingVersion < AppendPositionFlagsMinVersion {
			return 20, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.CommitPositionSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.CommitPositionSbeBlockLength)

	case clusterwire.CatchupPositionSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.CatchupPositionSbeBlockLength)

	case clusterwire.StopCatchupSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.StopCatchupSbeBlockLength)

	case clusterwire.TimerEventSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.TimerEventSbeBlockLength)
	case clusterwire.SessionOpenEventSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionOpenEventSbeBlockLength)
	case clusterwire.SessionCloseEventSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.SessionCloseEventSbeBlockLength)
	case clusterwire.ServiceAckSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.ServiceAckSbeBlockLength)

	case clusterwire.JoinLogSbeTemplateID:
		maxBlock = clusterwire.JoinLogSbeBlockLength
		if actingVersion < JoinLogIsStandbyMinVersion {
			// ids 1-7 only (IsStandby sinceVersion 16).
			return 36, maxBlock, true
		}
		return clusterFixedBlockLength(maxBlock)

	case clusterwire.RequestServiceAckSbeTemplateID:
		return clusterFixedBlockLength(clusterwire.RequestServiceAckSbeBlockLength)

	default:
		return 0, 0, false
	}
}

// ClusterVarFieldCount returns the number of variable-length fields for templateID
// at actingVersion.
func ClusterVarFieldCount(templateID, actingVersion uint16) (count int, ok bool) {
	switch templateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID,
		clusterwire.SessionCloseRequestSbeTemplateID,
		clusterwire.SessionKeepAliveSbeTemplateID,
		clusterwire.NewLeadershipTermEventSbeTemplateID:
		return 0, true

	case clusterwire.SessionEventSbeTemplateID,
		clusterwire.NewLeaderEventSbeTemplateID,
		clusterwire.ChallengeSbeTemplateID,
		clusterwire.ChallengeResponseSbeTemplateID,
		clusterwire.AdminRequestSbeTemplateID,
		clusterwire.CatchupPositionSbeTemplateID,
		clusterwire.JoinLogSbeTemplateID:
		return 1, true

	case clusterwire.SessionConnectRequestSbeTemplateID:
		if actingVersion >= SessionConnectClientInfoMinVersion {
			return 3, true
		}
		return 2, true

	case clusterwire.AdminResponseSbeTemplateID,
		clusterwire.SessionOpenEventSbeTemplateID:
		return 2, true

	case clusterwire.CanvassPositionSbeTemplateID,
		clusterwire.RequestVoteSbeTemplateID,
		clusterwire.VoteSbeTemplateID,
		clusterwire.NewLeadershipTermSbeTemplateID,
		clusterwire.AppendPositionSbeTemplateID,
		clusterwire.CommitPositionSbeTemplateID,
		clusterwire.StopCatchupSbeTemplateID,
		clusterwire.TimerEventSbeTemplateID,
		clusterwire.SessionCloseEventSbeTemplateID,
		clusterwire.ServiceAckSbeTemplateID,
		clusterwire.RequestServiceAckSbeTemplateID:
		return 0, true

	default:
		return 0, false
	}
}
