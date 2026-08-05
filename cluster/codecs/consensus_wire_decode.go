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

func decodeCatchupPositionFields(
	buf []byte,
	body, block, version, bufLen uint64,
) CatchupPositionFields {
	var msg clusterwire.CatchupPosition
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return CatchupPositionFields{
		LeadershipTermID: msg.LeadershipTermId(),
		LogPosition:      msg.LogPosition(),
		FollowerMemberID: msg.FollowerMemberId(),
		CatchupEndpoint:  msg.GetCatchupEndpointAsString(),
	}
}

func decodeStopCatchupFields(
	buf []byte,
	body, block, version, bufLen uint64,
) StopCatchupFields {
	var msg clusterwire.StopCatchup
	msg.WrapForDecode(buf, body, block, version, bufLen)
	return StopCatchupFields{
		LeadershipTermID: msg.LeadershipTermId(),
		FollowerMemberID: msg.FollowerMemberId(),
	}
}

// tryDecodeClusterAt parses the cluster header at offset, checks the template
// ID, and decodes the fixed block via decode. The path stays allocation-free:
// T is a concrete field struct (no interface boxing) and decode is a
// package-level function reference, so each instantiation is monomorphized.
//
// A positive maxVarLen scans the var fields without cloning and rejects the
// message when a var length exceeds the bound or overruns the buffer. Fixed
// templates pass 0 and skip the scan.
func tryDecodeClusterAt[T any](
	buf []byte,
	offset int,
	templateID uint16,
	maxVarLen int,
	decode func(buf []byte, body, block, version, bufLen uint64) T,
) (fields T, ok bool) {
	layout, ok := ClusterMessageLayoutAt(buf, offset, maxVarLen)
	if !ok || layout.TemplateID != templateID {
		return fields, false
	}
	if maxVarLen > 0 {
		varOffset := layout.BodyOffset + int(layout.BlockLength)
		if _, _, scanOK := scanTemplateVarFields(
			buf,
			varOffset,
			layout.TemplateID,
			layout.ActingVersion,
			maxVarLen,
			false,
		); !scanOK {
			return fields, false
		}
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
	return tryDecodeClusterAt(
		buf, offset, clusterwire.CanvassPositionSbeTemplateID, 0, decodeCanvassPositionFields,
	)
}

func TryDecodeRequestVoteAt(buf []byte, offset int) (RequestVoteFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.RequestVoteSbeTemplateID, 0, decodeRequestVoteFields,
	)
}

func TryDecodeVoteAt(buf []byte, offset int) (VoteFields, bool) {
	return tryDecodeClusterAt(buf, offset, clusterwire.VoteSbeTemplateID, 0, decodeVoteFields)
}

func TryDecodeNewLeadershipTermAt(buf []byte, offset int) (NewLeadershipTermFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.NewLeadershipTermSbeTemplateID, 0, decodeNewLeadershipTermFields,
	)
}

func TryDecodeAppendPositionAt(buf []byte, offset int) (AppendPositionFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.AppendPositionSbeTemplateID, 0, decodeAppendPositionFields,
	)
}

func TryDecodeCommitPositionAt(buf []byte, offset int) (CommitPositionFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.CommitPositionSbeTemplateID, 0, decodeCommitPositionFields,
	)
}

// TryDecodeCatchupPositionAt decodes CatchupPosition including the var-ascii
// catchup endpoint. Passes ClusterDecodeMaxVarLen so the endpoint length is
// validated before decode.
func TryDecodeCatchupPositionAt(buf []byte, offset int) (CatchupPositionFields, bool) {
	return tryDecodeClusterAt(
		buf,
		offset,
		clusterwire.CatchupPositionSbeTemplateID,
		ClusterDecodeMaxVarLen,
		decodeCatchupPositionFields,
	)
}

// TryDecodeStopCatchupAt decodes a fixed-length StopCatchup message.
func TryDecodeStopCatchupAt(buf []byte, offset int) (StopCatchupFields, bool) {
	return tryDecodeClusterAt(
		buf, offset, clusterwire.StopCatchupSbeTemplateID, 0, decodeStopCatchupFields,
	)
}
