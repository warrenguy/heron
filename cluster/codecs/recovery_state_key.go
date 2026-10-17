package codecs

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

// RecoveryStateCounterTypeID is the recovery state counter type id (Java
// AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID).
const RecoveryStateCounterTypeID int32 = 204

const (
	recoveryKeyLeadershipTermIDOffset     = 0
	recoveryKeyLogPositionOffset          = recoveryKeyLeadershipTermIDOffset + atomicbuffer.SizeInt64
	recoveryKeyTimestampOffset            = recoveryKeyLogPositionOffset + atomicbuffer.SizeInt64
	recoveryKeyClusterIDOffset            = recoveryKeyTimestampOffset + atomicbuffer.SizeInt64
	recoveryKeyServiceCountOffset         = recoveryKeyClusterIDOffset + atomicbuffer.SizeInt32
	recoveryKeySnapshotRecordingIDsOffset = recoveryKeyServiceCountOffset + atomicbuffer.SizeInt32
)

// RecoveryStateMaxServiceCount is the most snapshot recording ids a counter
// key holds.
const RecoveryStateMaxServiceCount = (cnc.CounterMetadataMaxKeyLength -
	recoveryKeySnapshotRecordingIDsOffset) / atomicbuffer.SizeInt64

// RecoveryStateKeyFields is the key of the recovery state counter.
//
// SnapshotRecordingIDs holds one recording id per service, indexed by service
// id. The decoder leaves it nil for a key with no services.
type RecoveryStateKeyFields struct {
	LeadershipTermID     int64
	LogPosition          int64
	Timestamp            int64
	ClusterID            int32
	SnapshotRecordingIDs []int64
}

// SnapshotRecordingID returns the recording id for serviceID. Returns 0 and
// false when serviceID is out of range.
func (f RecoveryStateKeyFields) SnapshotRecordingID(serviceID int32) (int64, bool) {
	if serviceID < 0 || int(serviceID) >= len(f.SnapshotRecordingIDs) {
		return 0, false
	}
	return f.SnapshotRecordingIDs[serviceID], true
}

func recoveryStateKeyLength(serviceCount int) int {
	return recoveryKeySnapshotRecordingIDsOffset + serviceCount*atomicbuffer.SizeInt64
}

// EncodeRecoveryStateKey writes the recovery state counter key at offset. The
// bytes match Java RecoveryState.allocate. The key ends at keyEnd.
//
// Returns 0 and false for a negative offset, more than
// RecoveryStateMaxServiceCount ids, or a short buf. A refused encode leaves
// buf unchanged.
func EncodeRecoveryStateKey(
	buf []byte,
	offset int,
	fields RecoveryStateKeyFields,
) (keyEnd int, ok bool) {
	serviceCount := len(fields.SnapshotRecordingIDs)
	if serviceCount > RecoveryStateMaxServiceCount {
		return 0, false
	}
	keyLength := recoveryStateKeyLength(serviceCount)
	if offset < 0 || offset > len(buf)-keyLength {
		return 0, false
	}

	key := atomicbuffer.Wrap(buf[offset : offset+keyLength])
	key.PutInt64(recoveryKeyLeadershipTermIDOffset, fields.LeadershipTermID)
	key.PutInt64(recoveryKeyLogPositionOffset, fields.LogPosition)
	key.PutInt64(recoveryKeyTimestampOffset, fields.Timestamp)
	key.PutInt32(recoveryKeyClusterIDOffset, fields.ClusterID)
	//nolint:gosec // G115: RecoveryStateMaxServiceCount bounds the count
	key.PutInt32(recoveryKeyServiceCountOffset, int32(serviceCount))
	for i, recordingID := range fields.SnapshotRecordingIDs {
		key.PutInt64(recoveryKeySnapshotRecordingIDsOffset+i*atomicbuffer.SizeInt64, recordingID)
	}
	return offset + keyLength, true
}

// TryDecodeRecoveryStateKeyAt reads the recovery state counter key at offset.
// Pass the whole key region. The service count sets how many bytes the decode
// reads.
//
// Returns RecoveryStateKeyFields{} and false for a negative offset, a service
// count outside 0 to RecoveryStateMaxServiceCount, or too few bytes after
// offset. Java RecoveryState.allocate bounds the service count. The Java
// readers trust the stored count. The bound here stops a corrupt count from
// reading past the key.
func TryDecodeRecoveryStateKeyAt(buf []byte, offset int) (RecoveryStateKeyFields, bool) {
	if offset < 0 || offset > len(buf)-recoveryKeySnapshotRecordingIDsOffset {
		return RecoveryStateKeyFields{}, false
	}
	key := atomicbuffer.Wrap(buf[offset:])
	serviceCount := int(key.Int32At(recoveryKeyServiceCountOffset))
	if serviceCount < 0 || serviceCount > RecoveryStateMaxServiceCount {
		return RecoveryStateKeyFields{}, false
	}
	if offset > len(buf)-recoveryStateKeyLength(serviceCount) {
		return RecoveryStateKeyFields{}, false
	}

	fields := RecoveryStateKeyFields{
		LeadershipTermID: key.Int64At(recoveryKeyLeadershipTermIDOffset),
		LogPosition:      key.Int64At(recoveryKeyLogPositionOffset),
		Timestamp:        key.Int64At(recoveryKeyTimestampOffset),
		ClusterID:        key.Int32At(recoveryKeyClusterIDOffset),
	}
	if serviceCount > 0 {
		fields.SnapshotRecordingIDs = make([]int64, serviceCount)
		for i := range fields.SnapshotRecordingIDs {
			fields.SnapshotRecordingIDs[i] = key.Int64At(
				recoveryKeySnapshotRecordingIDsOffset + i*atomicbuffer.SizeInt64,
			)
		}
	}
	return fields, true
}
