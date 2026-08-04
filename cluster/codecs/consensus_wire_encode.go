package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

// CanvassPositionBlockLength returns the CanvassPosition root block length for actingVersion.
func CanvassPositionBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.CanvassPositionSbeTemplateID,
		actingVersion,
		clusterwire.CanvassPositionSbeBlockLength,
	)
}

// RequestVoteBlockLength returns the RequestVote root block length for actingVersion.
func RequestVoteBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.RequestVoteSbeTemplateID,
		actingVersion,
		clusterwire.RequestVoteSbeBlockLength,
	)
}

// NewLeadershipTermBlockLength returns the NewLeadershipTerm root block length for actingVersion.
func NewLeadershipTermBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.NewLeadershipTermSbeTemplateID,
		actingVersion,
		clusterwire.NewLeadershipTermSbeBlockLength,
	)
}

// AppendPositionBlockLength returns the AppendPosition root block length for actingVersion.
func AppendPositionBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.AppendPositionSbeTemplateID,
		actingVersion,
		clusterwire.AppendPositionSbeBlockLength,
	)
}

// EncodeCanvassPosition writes a cluster CanvassPosition at offset.
func EncodeCanvassPosition(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields CanvassPositionFields,
) (wireEnd int, ok bool) {
	blockLength := CanvassPositionBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.CanvassPositionSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.CanvassPosition
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLogLeadershipTermId(fields.LogLeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetFollowerMemberId(fields.FollowerMemberID)
			if actingVersion >= CanvassProtocolVersionMinVersion {
				msg.SetProtocolVersion(fields.ProtocolVersion)
			}
		},
	)
}

// EncodeRequestVote writes a cluster RequestVote at offset.
func EncodeRequestVote(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields RequestVoteFields,
) (wireEnd int, ok bool) {
	blockLength := RequestVoteBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.RequestVoteSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.RequestVote
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLogLeadershipTermId(fields.LogLeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetCandidateTermId(fields.CandidateTermID).
				SetCandidateMemberId(fields.CandidateMemberID)
			if actingVersion >= RequestVoteProtocolVersionMinVersion {
				msg.SetProtocolVersion(fields.ProtocolVersion)
			}
		},
	)
}

// EncodeVote writes a cluster Vote at offset.
func EncodeVote(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields VoteFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.VoteSbeBlockLength,
		clusterwire.VoteSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.Vote
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetCandidateTermId(fields.CandidateTermID).
				SetLogLeadershipTermId(fields.LogLeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetCandidateMemberId(fields.CandidateMemberID).
				SetFollowerMemberId(fields.FollowerMemberID).
				SetVote(fields.Vote)
		},
	)
}

// EncodeNewLeadershipTerm writes a cluster NewLeadershipTerm at offset.
func EncodeNewLeadershipTerm(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields NewLeadershipTermFields,
) (wireEnd int, ok bool) {
	blockLength := NewLeadershipTermBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.NewLeadershipTermSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.NewLeadershipTerm
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLogLeadershipTermId(fields.LogLeadershipTermID).
				SetNextLeadershipTermId(fields.NextLeadershipTermID).
				SetNextTermBaseLogPosition(fields.NextTermBaseLogPosition).
				SetNextLogPosition(fields.NextLogPosition).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetTermBaseLogPosition(fields.TermBaseLogPosition).
				SetLogPosition(fields.LogPosition).
				SetLeaderRecordingId(fields.LeaderRecordingID).
				SetTimestamp(fields.Timestamp).
				SetLeaderMemberId(fields.LeaderMemberID).
				SetLogSessionId(fields.LogSessionID).
				SetIsStartup(fields.IsStartup)

			// AppVersion always writes, zero included: exclusive tryClaim term
			// buffers are not zeroed, so omit would leave residual bytes.
			msg.SetAppVersion(fields.AppVersion)
			if actingVersion >= NewLeadershipTermCommitPositionMinVersion {
				msg.SetCommitPosition(fields.CommitPosition)
			}
		},
	)
}

// EncodeAppendPosition writes a cluster AppendPosition at offset.
func EncodeAppendPosition(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields AppendPositionFields,
) (wireEnd int, ok bool) {
	blockLength := AppendPositionBlockLength(actingVersion)
	return encodeWithClusterWireHeader(
		buf,
		offset,
		blockLength,
		clusterwire.AppendPositionSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.AppendPosition
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLeadershipTermId(fields.LeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetFollowerMemberId(fields.FollowerMemberID)
			if actingVersion >= AppendPositionFlagsMinVersion {
				msg.SetFlags(fields.Flags)
			}
		},
	)
}

// EncodeCommitPosition writes a cluster CommitPosition at offset.
func EncodeCommitPosition(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields CommitPositionFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.CommitPositionSbeBlockLength,
		clusterwire.CommitPositionSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.CommitPosition
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLeadershipTermId(fields.LeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetLeaderMemberId(fields.LeaderMemberID)
		},
	)
}

// EncodeCatchupPosition writes a cluster CatchupPosition at offset.
//
// CatchupEndpoint is the var-ascii follower endpoint the leader archive
// replays to. Wire length includes the endpoint payload.
func EncodeCatchupPosition(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields CatchupPositionFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.CatchupPositionSbeBlockLength,
		clusterwire.CatchupPositionSbeTemplateID,
		actingVersion,
		[]int{len(fields.CatchupEndpoint)},
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.CatchupPosition
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLeadershipTermId(fields.LeadershipTermID).
				SetLogPosition(fields.LogPosition).
				SetFollowerMemberId(fields.FollowerMemberID).
				PutCatchupEndpoint(fields.CatchupEndpoint)
		},
	)
}

// EncodeStopCatchup writes a cluster StopCatchup at offset.
func EncodeStopCatchup(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields StopCatchupFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.StopCatchupSbeBlockLength,
		clusterwire.StopCatchupSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset int, blockLength, actingVersion uint16) {
			var msg clusterwire.StopCatchup
			msg.Wrap(dst, uint64(bodyOffset), uint64(len(dst)), uint64(blockLength), uint64(actingVersion))
			msg.SetLeadershipTermId(fields.LeadershipTermID).
				SetFollowerMemberId(fields.FollowerMemberID)
		},
	)
}
