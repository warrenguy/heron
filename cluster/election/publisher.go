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

// Publisher sends consensus messages to a peer. Methods take codecs field
// structs so call sites name each field; ProtocolVersion on canvass and
// request-vote is filled from ActingVersion inside each implementation.
type Publisher interface {
	CanvassPosition(dst PublicationTarget, fields codecs.CanvassPositionFields)
	RequestVote(dst PublicationTarget, fields codecs.RequestVoteFields) bool
	PlaceVote(dst PublicationTarget, fields codecs.VoteFields)
	NewLeadershipTerm(dst PublicationTarget, fields codecs.NewLeadershipTermFields)
	AppendPosition(dst PublicationTarget, fields codecs.AppendPositionFields) bool
	CommitPosition(dst PublicationTarget, fields codecs.CommitPositionFields)
	// CatchupPosition asks the leader to archive-replay from LogPosition to
	// CatchupEndpoint. Returns false when the send does not complete.
	CatchupPosition(dst PublicationTarget, fields codecs.CatchupPositionFields) bool
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

// applyCanvassProtocolVersion sets ProtocolVersion from the publisher acting version.
func applyCanvassProtocolVersion(
	ver uint16,
	fields codecs.CanvassPositionFields,
) codecs.CanvassPositionFields {
	fields.ProtocolVersion = protocolVersionFor(
		ver,
		codecs.CanvassProtocolVersionMinVersion,
	)
	return fields
}

// applyRequestVoteProtocolVersion sets ProtocolVersion from the publisher acting version.
func applyRequestVoteProtocolVersion(
	ver uint16,
	fields codecs.RequestVoteFields,
) codecs.RequestVoteFields {
	fields.ProtocolVersion = protocolVersionFor(
		ver,
		codecs.RequestVoteProtocolVersionMinVersion,
	)
	return fields
}
