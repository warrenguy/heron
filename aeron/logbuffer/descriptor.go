package logbuffer

import (
	"errors"
	"fmt"
	"math"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	PartitionCount = 3

	TermMinLength = 64 * 1024
	TermMaxLength = 1024 * 1024 * 1024
	PageMinSize   = 4 * 1024
	PageMaxSize   = 1024 * 1024 * 1024

	PaddingSize                        = 64
	DefaultFrameHeaderMaxLength        = atomic.CacheLineLength * 2
	MetaDataLength                     = PageMinSize
	FrameAlignment                     = 32
	LogMetaDataSectionIndex            = 3
	LogBufferTypeConcurrentPublication = 0
	LogBufferTypeExclusivePublication  = 1
	LogBufferTypePublicationImage      = 2
	MaxUDPPayloadLength                = 65504
)

func checkPowerOfTwoRange(name string, value, min, max int) error {
	if value < min {
		return fmt.Errorf("logbuffer: %s below minimum: %d < %d", name, value, min)
	}
	if value > max {
		return fmt.Errorf("logbuffer: %s above maximum: %d > %d", name, value, max)
	}
	if !atomic.IsPowerOfTwo(value) {
		return fmt.Errorf("logbuffer: %s not a power of two: %d", name, value)
	}
	return nil
}

func CheckTermLength(termLength int) error {
	return checkPowerOfTwoRange("term length", termLength, TermMinLength, TermMaxLength)
}

func CheckPageSize(pageSize int) error {
	return checkPowerOfTwoRange("page size", pageSize, PageMinSize, PageMaxSize)
}

func rawLogLength(termLength int) (int64, error) {
	maxTerm := int(math.MaxInt64 / int64(PartitionCount))
	if termLength > maxTerm {
		return 0, errors.New("logbuffer: computed length overflow")
	}
	return int64(termLength)*int64(PartitionCount) + int64(MetaDataLength), nil
}

// ComputeLogLength returns page-aligned file length as int. Valid on amd64/arm64
// targets where int is 64-bit; max layout fits int at TermMaxLength.
func ComputeLogLength(termLength, pageSize int) (int, error) {
	if err := CheckTermLength(termLength); err != nil {
		return 0, err
	}
	if err := CheckPageSize(pageSize); err != nil {
		return 0, err
	}
	raw, err := rawLogLength(termLength)
	if err != nil {
		return 0, err
	}
	return atomic.Align(int(raw), pageSize), nil
}

// SectionTermOffset returns the byte offset of term partition sectionIndex.
// Aeron: TermOffset(sectionIndex, termLength).
func SectionTermOffset(sectionIndex, termLength int) int {
	return sectionIndex * termLength
}

func MetaDataOffset(termLength int) int {
	return PartitionCount * termLength
}

func PositionBitsToShift(termLength int) int {
	return atomic.NumberOfTrailingZeroes(int32(termLength))
}

func ComputeMaxMessageLength(termLength int) int {
	maxLengthForTerm := termLength / 8
	if maxLengthForTerm < MaxMessageLength {
		return maxLengthForTerm
	}
	return MaxMessageLength
}

func AlignFrameLength(length int) int {
	return atomic.Align(length, FrameAlignment)
}

// ComputeFragmentedFrameLength returns the aligned term space for a payload
// split across DATA fragments (Java LogBufferDescriptor.computeFragmentedFrameLength).
func ComputeFragmentedFrameLength(length, maxPayloadLength int) int {
	if length <= 0 {
		return 0
	}
	fragmentCount := (length + maxPayloadLength - 1) / maxPayloadLength
	if fragmentCount == 1 {
		return AlignFragmentChunkLength(length)
	}
	lastChunk := length - (fragmentCount-1)*maxPayloadLength
	fullFrame := AlignFragmentChunkLength(maxPayloadLength)
	return (fragmentCount-1)*fullFrame + AlignFragmentChunkLength(lastChunk)
}

func AlignFragmentChunkLength(payloadBytes int) int {
	return AlignFrameLength(payloadBytes + DataHeaderLength)
}
