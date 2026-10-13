package atomicbuffer

import "errors"

// MaxDataCapacity is the largest data region DataCapacity accepts. Agrona
// buffers are limited to a Java int, which caps a power of two at 1 GiB.
const MaxDataCapacity = 1 << 30

var (
	errNotPowerOfTwo = errors.New("atomicbuffer: data capacity is not a positive power of two")
	errBelowMinimum  = errors.New("atomicbuffer: data capacity below minimum")
	errAboveMaximum  = errors.New("atomicbuffer: data capacity above maximum")
)

// ErrDataCapacityNotPowerOfTwo when bufferCapacity minus trailerLength is not a power of two.
var ErrDataCapacityNotPowerOfTwo = errNotPowerOfTwo

// ErrDataCapacityBelowMinimum is returned when the data region is smaller than minDataCapacity.
var ErrDataCapacityBelowMinimum = errBelowMinimum

// ErrDataCapacityAboveMaximum is returned when the data region is larger than MaxDataCapacity.
var ErrDataCapacityAboveMaximum = errAboveMaximum

// DataCapacity returns bufferCapacity - trailerLength when the data region is a positive
// power of two between minDataCapacity and MaxDataCapacity.
func DataCapacity(bufferCapacity, trailerLength, minDataCapacity int) (int, error) {
	dataCapacity := bufferCapacity - trailerLength
	if !IsPowerOfTwo(dataCapacity) {
		return 0, errNotPowerOfTwo
	}
	if dataCapacity < minDataCapacity {
		return 0, errBelowMinimum
	}
	if dataCapacity > MaxDataCapacity {
		return 0, errAboveMaximum
	}
	return dataCapacity, nil
}
