package logbuffer

import "github.com/warrenguy/heron/atomicbuffer"

// RawTailVolatile reads active-term count then that partition's raw tail.
//
// Standalone acquire fences between the two volatile loads match C aeron ordering;
// do not rely on Go volatile alone across both fields.
//
// Transient count/tail skew during rotation is accepted; matches C/Java rawTailVolatile.
//
// Callers re-read on hot paths.
func RawTailVolatile(meta atomicbuffer.AtomicBuffer) (int64, error) {
	atomicbuffer.AcquireFence()
	activeTermCount := meta.GetInt32Volatile(OffsetActiveTermCount)
	partition := IndexByTermCount(activeTermCount)
	atomicbuffer.AcquireFence()
	return GetRawTail(meta, partition)
}

func ActiveTermCount(meta atomicbuffer.AtomicBuffer) int32 {
	atomicbuffer.AcquireFence()
	return meta.GetInt32Volatile(OffsetActiveTermCount)
}

func GetRawTail(meta atomicbuffer.AtomicBuffer, partitionIndex int) (int64, error) {
	if err := ValidateTermIndex(partitionIndex, "partition index"); err != nil {
		return 0, err
	}
	return meta.GetInt64Volatile(OffsetTermTailCounter(partitionIndex)), nil
}

func PutRawTail(meta atomicbuffer.AtomicBuffer, partitionIndex int, rawTail int64) error {
	if err := ValidateTermIndex(partitionIndex, "partition index"); err != nil {
		return err
	}
	meta.PutInt64Release(OffsetTermTailCounter(partitionIndex), rawTail)
	return nil
}

func CasRawTail(
	meta atomicbuffer.AtomicBuffer,
	partitionIndex int,
	expect int64,
	update int64,
) (bool, error) {
	if err := ValidateTermIndex(partitionIndex, "partition index"); err != nil {
		return false, err
	}
	return meta.CompareAndSwapInt64(
		OffsetTermTailCounter(partitionIndex),
		expect,
		update,
	), nil
}

func CasActiveTermCount(
	meta atomicbuffer.AtomicBuffer,
	expect int32,
	update int32,
) bool {
	return meta.CompareAndSwapInt32(OffsetActiveTermCount, expect, update)
}

// RotateLog mirrors C aeron_logbuffer_rotate_log: the raw-tail CAS loop may exit
// without updating the next partition when the expected term id is absent, but
// CasActiveTermCount still runs unconditionally (C parity).
//
// Publication handleEndOfLog calls this after a PAD frame when reserved space crosses the
// term end.
//
// Callers must retry Offer/TryClaim on AdminAction.
func RotateLog(
	meta atomicbuffer.AtomicBuffer,
	currentTermCount int32,
	currentTermID int32,
) bool {
	nextTermID := AddWrapI32(currentTermID, 1)
	nextTermCount := AddWrapI32(currentTermCount, 1)
	nextIndex := IndexByTermCount(nextTermCount)
	expectedTermID := SubWrapI32(nextTermID, int32(PartitionCount))

	for {
		// C aeron ordering even though the prior IndexByTermCount bounds nextIndex.
		atomicbuffer.AcquireFence()
		rawTail, err := GetRawTail(meta, nextIndex)
		if err != nil {
			// nextIndex is always in [0, PartitionCount); kept for C parity.
			break
		}
		if expectedTermID != TermID(rawTail) {
			break
		}
		update := PackRawTail(nextTermID, 0)
		ok, err := CasRawTail(meta, nextIndex, rawTail, update)
		if err != nil || ok {
			break
		}
	}

	return CasActiveTermCount(meta, currentTermCount, nextTermCount)
}
