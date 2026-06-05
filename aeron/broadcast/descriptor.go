package broadcast

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	TrailerLength = 128

	// MinDataCapacity is the smallest power-of-two data region where one
	// aligned broadcast record can fit (Agrona maxMsgLength with alignment).
	MinDataCapacity = 16
)

const (
	TailIntentOffset = 0
	TailOffset       = 8
	LatestOffset     = 16
)

var (
	errDataCapacityNotPowerOfTwo = errors.New("broadcast: data capacity must be power of two")
	errDataCapacityTooSmall      = errors.New("broadcast: data capacity below minimum")
)

func TailIntentIndex(capacity int) int {
	return capacity + TailIntentOffset
}

func TailIndex(capacity int) int {
	return capacity + TailOffset
}

func LatestIndex(capacity int) int {
	return capacity + LatestOffset
}

func TotalBufferLength(dataCapacity int) int {
	return dataCapacity + TrailerLength
}

func CheckCapacity(bufferCapacity int) (int, error) {
	dataCapacity, err := atomic.DataCapacity(bufferCapacity, TrailerLength, MinDataCapacity)
	if err != nil {
		if errors.Is(err, atomic.ErrDataCapacityNotPowerOfTwo) {
			return 0, errDataCapacityNotPowerOfTwo
		}
		if errors.Is(err, atomic.ErrDataCapacityBelowMinimum) {
			return 0, errDataCapacityTooSmall
		}
		return 0, err
	}
	return dataCapacity, nil
}
