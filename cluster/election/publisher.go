package election

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// PublicationTarget identifies where a consensus message is sent.
// SbePublisher records target; maps to *Member publication when live publications wire in.
type PublicationTarget struct {
	MemberID int32
}

type Publisher interface {
	CanvassPosition(
		dst PublicationTarget,
		logLeadershipTermID, logPosition, leadershipTermID int64,
		followerMemberID int32,
	)
	RequestVote(
		dst PublicationTarget,
		logLeadershipTermID, logPosition, candidateTermID int64,
		candidateMemberID int32,
	) bool
	PlaceVote(
		dst PublicationTarget,
		candidateTermID, logLeadershipTermID, logPosition int64,
		candidateMemberID, followerMemberID int32,
		vote bool,
	)
	NewLeadershipTerm(
		dst PublicationTarget,
		fields codecs.NewLeadershipTermFields,
	)
	AppendPosition(
		dst PublicationTarget,
		leadershipTermID, logPosition int64,
		followerMemberID int32,
		flags uint8,
	) bool
	CommitPosition(
		dst PublicationTarget,
		leadershipTermID, logPosition int64,
		leaderMemberID int32,
	)
}

type SentMessage struct {
	TemplateID uint16
	Dst        PublicationTarget
	Fields     any
	Wire       []byte
}

func actingVersion(ver uint16) uint16 {
	if ver == 0 {
		return clusterwire.CanvassPositionSbeSchemaVersion
	}
	return ver
}

// protocolVersionFor gates the optional ProtocolVersion field on the acting
// version; minVersion is the message's own *ProtocolVersionMinVersion oracle.
func protocolVersionFor(actingVersion, minVersion uint16) int32 {
	if actingVersion >= minVersion {
		return ProtocolSemanticVersion()
	}
	return 0
}

func boolToClusterwire(v bool) clusterwire.BooleanType {
	if v {
		return clusterwire.BooleanType_TRUE
	}
	return clusterwire.BooleanType_FALSE
}
