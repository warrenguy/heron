package logbuffer

import "fmt"

func ValidateTermIndex(index int, label string) error {
	if index < 0 || index >= PartitionCount {
		return fmt.Errorf("logbuffer: invalid %s: %d", label, index)
	}
	return nil
}

func AddWrapI32(a, b int32) int32 {
	return a + b
}

func SubWrapI32(a, b int32) int32 {
	return a - b
}

func PackRawTail(termID, termOffset int32) int64 {
	return (int64(termID) << 32) | int64(uint32(termOffset))
}

func TermID(rawTail int64) int32 {
	return int32(rawTail >> 32)
}

func TermOffset(rawTail int64, termLength int) int32 {
	//nolint:gosec // G115: the result is at most termLength, which CheckTermLength bounds
	return int32(min(rawTail&0xFFFFFFFF, int64(termLength)))
}

func ComputeTermCount(termID, initialTermID int32) int32 {
	return SubWrapI32(termID, initialTermID)
}

func IndexByTermCount(termCount int32) int {
	return int(uint32(termCount) % uint32(PartitionCount))
}

func IndexByTerm(initialTermID, activeTermID int32) int {
	return IndexByTermCount(ComputeTermCount(activeTermID, initialTermID))
}

// IndexByPosition maps stream position to term partition. Aeron stream positions
// are non-negative; signed shift matches Java >>> for position >= 0.
//
// Partition index and in-term offset (ComputeTermOffsetFromPosition) are different fields.
func IndexByPosition(position int64, positionBitsToShift int) int {
	shifted := position >> positionBitsToShift
	return int(uint64(shifted) % uint64(PartitionCount))
}

func ComputePosition(
	activeTermID int32,
	termOffset int,
	positionBitsToShift int,
	initialTermID int32,
) int64 {
	termCount := ComputeTermCount(activeTermID, initialTermID)
	return (int64(termCount) << positionBitsToShift) + int64(termOffset)
}

func ComputeTermBeginPosition(
	activeTermID int32,
	positionBitsToShift int,
	initialTermID int32,
) int64 {
	return ComputePosition(activeTermID, 0, positionBitsToShift, initialTermID)
}

func ComputeTermIDFromPosition(
	position int64,
	positionBitsToShift int,
	initialTermID int32,
) int32 {
	return AddWrapI32(int32(position>>positionBitsToShift), initialTermID)
}

// ComputeTermOffsetFromPosition returns the in-term byte offset (low bits of
// position). Not comparable to IndexByPosition, which returns partition index.
func ComputeTermOffsetFromPosition(position int64, positionBitsToShift int) int32 {
	mask := int64(1<<positionBitsToShift) - 1
	return int32(position & mask)
}
