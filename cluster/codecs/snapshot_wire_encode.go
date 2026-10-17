package codecs

import "github.com/warrenguy/heron/cluster/codecs/clusterwire"

// ConsensusModuleSnapshotTypeID is the SnapshotMarker typeId of a consensus
// module snapshot.
const ConsensusModuleSnapshotTypeID int64 = 1

// ServiceSnapshotTypeID is the SnapshotMarker typeId of a clustered service
// snapshot.
const ServiceSnapshotTypeID int64 = 2

// SnapshotMarkerBlockLength returns the SnapshotMarker root block length for
// actingVersion.
//
// Below SnapshotMarkerTimeUnitMinVersion the min root is 32 (ids 1-5). At and
// above that version the full schema block is 40.
func SnapshotMarkerBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.SnapshotMarkerSbeTemplateID,
		actingVersion,
		clusterwire.SnapshotMarkerSbeBlockLength,
	)
}

// ConsensusModuleBlockLength returns the ConsensusModule root block length for
// actingVersion.
//
// Below ConsensusModuleNextServiceSessionIDMinVersion the min root is 8
// (id 1). At and above that version the full schema block is 28.
func ConsensusModuleBlockLength(actingVersion uint16) uint16 {
	return ClusterMinRootBlockLength(
		clusterwire.ConsensusModuleSbeTemplateID,
		actingVersion,
		clusterwire.ConsensusModuleSbeBlockLength,
	)
}

var (
	snapshotMarkerEncodedLen = int(
		(&clusterwire.SnapshotMarker{}).SbeBlockAndHeaderLength(),
	)
	timerEncodedLen = int(
		(&clusterwire.Timer{}).SbeBlockAndHeaderLength(),
	)
	consensusModuleEncodedLen = int(
		(&clusterwire.ConsensusModule{}).SbeBlockAndHeaderLength(),
	)
)

// SnapshotMarkerEncodedLen is MessageHeader plus full schema block (8+40=48).
//
// Encoding below SnapshotMarkerTimeUnitMinVersion writes a shorter wire
// (8+32=40). A claim sized by this length must encode at or above that
// version. Otherwise the trailing 8 claimed bytes stay unwritten.
func SnapshotMarkerEncodedLen() int {
	return snapshotMarkerEncodedLen
}

// TimerEncodedLen is MessageHeader plus full schema block (8+16=24).
func TimerEncodedLen() int {
	return timerEncodedLen
}

// ConsensusModuleEncodedLen is MessageHeader plus full schema block (8+28=36).
//
// Encoding below ConsensusModuleNextServiceSessionIDMinVersion writes a
// shorter wire (8+8=16). A claim sized by this length must encode at or above
// that version. Otherwise the trailing 20 claimed bytes stay unwritten.
func ConsensusModuleEncodedLen() int {
	return consensusModuleEncodedLen
}

// EncodeSnapshotMarker writes a cluster SnapshotMarker (SBE template id 100)
// at offset.
//
// Required body fields (ids 1-5) always write. When actingVersion >=
// SnapshotMarkerTimeUnitMinVersion, TimeUnit and AppVersion always write,
// including 0. Exclusive TryClaim does not zero the claimed buffer.
func EncodeSnapshotMarker(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields SnapshotMarkerFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		SnapshotMarkerBlockLength(actingVersion),
		clusterwire.SnapshotMarkerSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset uint64, blockLength, actingVersion uint16) {
			var msg clusterwire.SnapshotMarker
			msg.Wrap(dst, bodyOffset, uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetTypeId(fields.TypeID).
				SetLogPosition(fields.LogPosition).
				SetLeadershipTermId(fields.LeadershipTermID).
				SetIndex(fields.Index).
				SetMark(fields.Mark)
			if actingVersion >= SnapshotMarkerTimeUnitMinVersion {
				msg.SetTimeUnit(fields.TimeUnit).
					SetAppVersion(fields.AppVersion)
			}
		},
	)
}

// EncodeClientSession writes a cluster ClientSession (SBE template id 102) at
// offset. Size the claim with ClusterWireMessageLen. ok is false when a var
// field exceeds ClusterEncodeMaxVarLen.
func EncodeClientSession(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ClientSessionFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.ClientSessionSbeBlockLength,
		clusterwire.ClientSessionSbeTemplateID,
		actingVersion,
		[]int{len(fields.ResponseChannel), len(fields.EncodedPrincipal)},
		func(dst []byte, bodyOffset uint64, blockLength, actingVersion uint16) {
			var msg clusterwire.ClientSession
			msg.Wrap(dst, bodyOffset, uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetClusterSessionId(fields.ClusterSessionID).
				SetResponseStreamId(fields.ResponseStreamID)
			msg.PutResponseChannel(clusterVarString(fields.ResponseChannel))
			msg.PutEncodedPrincipal(clusterVarString(fields.EncodedPrincipal))
		},
	)
}

// EncodeTimer writes a cluster Timer (SBE template id 104) at offset.
func EncodeTimer(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields TimerFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		clusterwire.TimerSbeBlockLength,
		clusterwire.TimerSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset uint64, blockLength, actingVersion uint16) {
			var msg clusterwire.Timer
			msg.Wrap(dst, bodyOffset, uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetCorrelationId(fields.CorrelationID).
				SetDeadline(fields.Deadline)
		},
	)
}

// EncodeConsensusModule writes a cluster ConsensusModule (SBE template id 105)
// at offset.
//
// NextSessionID always writes. When actingVersion >=
// ConsensusModuleNextServiceSessionIDMinVersion, NextServiceSessionID,
// LogServiceSessionID and PendingMessageCapacity always write, including 0.
// Exclusive TryClaim does not zero the claimed buffer.
func EncodeConsensusModule(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields ConsensusModuleFields,
) (wireEnd int, ok bool) {
	return encodeWithClusterWireHeader(
		buf,
		offset,
		ConsensusModuleBlockLength(actingVersion),
		clusterwire.ConsensusModuleSbeTemplateID,
		actingVersion,
		nil,
		func(dst []byte, bodyOffset uint64, blockLength, actingVersion uint16) {
			var msg clusterwire.ConsensusModule
			msg.Wrap(dst, bodyOffset, uint64(len(dst)), uint64(blockLength), uint64(actingVersion)).
				SetNextSessionId(fields.NextSessionID)
			if actingVersion >= ConsensusModuleNextServiceSessionIDMinVersion {
				msg.SetNextServiceSessionId(fields.NextServiceSessionID).
					SetLogServiceSessionId(fields.LogServiceSessionID).
					SetPendingMessageCapacity(fields.PendingMessageCapacity)
			}
		},
	)
}
