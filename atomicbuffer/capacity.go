package atomicbuffer

import "errors"

var (
	errNotPowerOfTwo = errors.New("atomicbuffer: data capacity is not a positive power of two")
	errBelowMinimum  = errors.New("atomicbuffer: data capacity below minimum")
)

// ErrDataCapacityNotPowerOfTwo when bufferCapacity minus trailerLength is not a power of two.
var ErrDataCapacityNotPowerOfTwo = errNotPowerOfTwo

// ErrDataCapacityBelowMinimum is returned when the data region is smaller than minDataCapacity.
var ErrDataCapacityBelowMinimum = errBelowMinimum

// DataCapacity returns bufferCapacity - trailerLength when the data region is a positive
// power of two and at least minDataCapacity.
func DataCapacity(bufferCapacity, trailerLength, minDataCapacity int) (int, error) {
	dataCapacity := bufferCapacity - trailerLength
	if !IsPowerOfTwo(dataCapacity) {
		return 0, errNotPowerOfTwo
	}
	if dataCapacity < minDataCapacity {
		return 0, errBelowMinimum
	}
	return dataCapacity, nil
}
