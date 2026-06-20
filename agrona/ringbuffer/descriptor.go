package ringbuffer

import (
	"errors"

	"github.com/warrenguy/heron/agrona/atomic"
)

const counterStride = 128

const TrailerLength = 6 * counterStride

const (
	TailOffset              = counterStride
	HeadCacheOffset         = 2 * counterStride
	HeadOffset              = 3 * counterStride
	CorrelationOffset       = 4 * counterStride
	ConsumerHeartbeatOffset = 5 * counterStride
)

var (
	errDataCapacityNotPowerOfTwo = errors.New("ringbuffer: data capacity must be power of two")
	// errInsufficientCapacity is for bufferCapacity too small at New*. Runtime ring-full
	// uses record.InsufficientCapacity (-2) from TryClaim/Write, not this error.
	errInsufficientCapacity = errors.New("ringbuffer: insufficient capacity")
)

func trailerIndex(dataCapacity, offset int) int {
	return dataCapacity + offset
}

func TailIndex(dataCapacity int) int {
	return trailerIndex(dataCapacity, TailOffset)
}

func HeadCacheIndex(dataCapacity int) int {
	return trailerIndex(dataCapacity, HeadCacheOffset)
}

func HeadIndex(dataCapacity int) int {
	return trailerIndex(dataCapacity, HeadOffset)
}

func CorrelationIndex(dataCapacity int) int {
	return trailerIndex(dataCapacity, CorrelationOffset)
}

func ConsumerHeartbeatIndex(dataCapacity int) int {
	return trailerIndex(dataCapacity, ConsumerHeartbeatOffset)
}

func TotalBufferLength(dataCapacity int) int {
	return dataCapacity + TrailerLength
}

func CheckCapacity(bufferCapacity, minCapacity int) (int, error) {
	dataCapacity, err := atomic.DataCapacity(bufferCapacity, TrailerLength, minCapacity)
	if err != nil {
		if errors.Is(err, atomic.ErrDataCapacityNotPowerOfTwo) {
			return 0, errDataCapacityNotPowerOfTwo
		}
		if errors.Is(err, atomic.ErrDataCapacityBelowMinimum) {
			return 0, errInsufficientCapacity
		}
		return 0, err
	}
	return dataCapacity, nil
}
