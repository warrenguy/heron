package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

func decodeCanvassPositionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) CanvassPositionFields {
	var msg clusterwire.CanvassPosition
	msg.WrapForDecode(buf, body, block, version, bufLen)
	protocolVersion := msg.ProtocolVersion()
	if version < uint64(CanvassProtocolVersionMinVersion) {
		protocolVersion = 0
	}
	return CanvassPositionFields{
		LogLeadershipTermID: msg.LogLeadershipTermId(),
		LogPosition:         msg.LogPosition(),
		LeadershipTermID:    msg.LeadershipTermId(),
		FollowerMemberID:    msg.FollowerMemberId(),
		ProtocolVersion:     protocolVersion,
	}
}

func decodeRequestVoteFields(
	buf []byte,
	body, block, version, bufLen uint64,
) RequestVoteFields {
	var msg clusterwire.RequestVote
	msg.WrapForDecode(buf, body, block, version, bufLen)
	protocolVersion := msg.ProtocolVersion()
	if version < uint64(RequestVoteProtocolVersionMinVersion) {
		protocolVersion = 0
	}
	return RequestVoteFields{
		LogLeadershipTermID: msg.LogLeadershipTermId(),
		LogPosition:         msg.LogPosition(),
		CandidateTermID:     msg.CandidateTermId(),
		CandidateMemberID:   msg.CandidateMemberId(),
		ProtocolVersion:     protocolVersion,
	}
}

func decodeVoteFields(buf []byte, body, block, version, bufLen uint64) VoteFields {
	var msg clusterwire.Vote
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return VoteFields{
		CandidateTermID:     msg.CandidateTermId(),
		LogLeadershipTermID: msg.LogLeadershipTermId(),
		LogPosition:         msg.LogPosition(),
		CandidateMemberID:   msg.CandidateMemberId(),
		FollowerMemberID:    msg.FollowerMemberId(),
		Vote:                msg.Vote(),
	}
}

func decodeNewLeadershipTermFields(
	buf []byte,
	body, block, version, bufLen uint64,
) NewLeadershipTermFields {
	var msg clusterwire.NewLeadershipTerm
	msg.WrapForDecode(buf, body, block, version, bufLen)
	commitPosition := msg.CommitPosition()
	if version < uint64(NewLeadershipTermCommitPositionMinVersion) {
		commitPosition = 0
	}
	return NewLeadershipTermFields{
		LogLeadershipTermID:     msg.LogLeadershipTermId(),
		NextLeadershipTermID:    msg.NextLeadershipTermId(),
		NextTermBaseLogPosition: msg.NextTermBaseLogPosition(),
		NextLogPosition:         msg.NextLogPosition(),
		LeadershipTermID:        msg.LeadershipTermId(),
		TermBaseLogPosition:     msg.TermBaseLogPosition(),
		LogPosition:             msg.LogPosition(),
		LeaderRecordingID:       msg.LeaderRecordingId(),
		Timestamp:               msg.Timestamp(),
		LeaderMemberID:          msg.LeaderMemberId(),
		LogSessionID:            msg.LogSessionId(),
		AppVersion:              msg.AppVersion(),
		IsStartup:               msg.IsStartup(),
		CommitPosition:          commitPosition,
	}
}

func decodeAppendPositionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) AppendPositionFields {
	var msg clusterwire.AppendPosition
	msg.WrapForDecode(buf, body, block, version, bufLen)
	flags := msg.Flags()
	if version < uint64(AppendPositionFlagsMinVersion) {
		flags = 0
	}
	return AppendPositionFields{
		LeadershipTermID: msg.LeadershipTermId(),
		LogPosition:      msg.LogPosition(),
		FollowerMemberID: msg.FollowerMemberId(),
		Flags:            flags,
	}
}

func decodeCommitPositionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) CommitPositionFields {
	var msg clusterwire.CommitPosition
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return CommitPositionFields{
		LeadershipTermID: msg.LeadershipTermId(),
		LogPosition:      msg.LogPosition(),
		LeaderMemberID:   msg.LeaderMemberId(),
	}
}

// tryDecodeConsensusAt parses the cluster header at offset, checks the template
// ID, and decodes the fixed block via decode. It stays allocation-free: T is a
// concrete field struct (no interface boxing) and decode is a package-level
// function reference, so each instantiation is monomorphized.
func tryDecodeConsensusAt[T any](
	buf []byte,
	offset int,
	templateID uint16,
	decode func(buf []byte, body, block, version, bufLen uint64) T,
) (T, bool) {
	layout, ok := ClusterMessageLayoutAt(buf, offset, 0)
	if !ok || layout.TemplateID != templateID {
		var zero T
		return zero, false
	}
	return decode(
		buf,
		uint64(layout.BodyOffset),
		uint64(layout.BlockLength),
		uint64(layout.ActingVersion),
		uint64(len(buf)),
	), true
}

func TryDecodeCanvassPositionAt(buf []byte, offset int) (CanvassPositionFields, bool) {
	return tryDecodeConsensusAt(
		buf, offset, clusterwire.CanvassPositionSbeTemplateID, decodeCanvassPositionFields,
	)
}

func TryDecodeRequestVoteAt(buf []byte, offset int) (RequestVoteFields, bool) {
	return tryDecodeConsensusAt(
		buf, offset, clusterwire.RequestVoteSbeTemplateID, decodeRequestVoteFields,
	)
}

func TryDecodeVoteAt(buf []byte, offset int) (VoteFields, bool) {
	return tryDecodeConsensusAt(buf, offset, clusterwire.VoteSbeTemplateID, decodeVoteFields)
}

func TryDecodeNewLeadershipTermAt(buf []byte, offset int) (NewLeadershipTermFields, bool) {
	return tryDecodeConsensusAt(
		buf, offset, clusterwire.NewLeadershipTermSbeTemplateID, decodeNewLeadershipTermFields,
	)
}

func TryDecodeAppendPositionAt(buf []byte, offset int) (AppendPositionFields, bool) {
	return tryDecodeConsensusAt(
		buf, offset, clusterwire.AppendPositionSbeTemplateID, decodeAppendPositionFields,
	)
}

func TryDecodeCommitPositionAt(buf []byte, offset int) (CommitPositionFields, bool) {
	return tryDecodeConsensusAt(
		buf, offset, clusterwire.CommitPositionSbeTemplateID, decodeCommitPositionFields,
	)
}
