package cnc

import (
	"errors"
	"iter"

	"github.com/warrenguy/heron/atomicbuffer"
)

var errLocalSocketAddressStringTooLong = errors.New(
	"cnc: local socket address string exceeds the counter key",
)

const (
	// ChannelEndpointStatusActive mirrors Java ChannelEndpointStatus.ACTIVE.
	ChannelEndpointStatusActive int64 = 1

	// DriverLocalSocketAddressStatusTypeID mirrors Java
	// AeronCounters.DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID.
	DriverLocalSocketAddressStatusTypeID int32 = 14

	localSocketAddressChannelStatusIDOffset = 0
	localSocketAddressLengthOffset          = atomicbuffer.SizeInt32
	localSocketAddressStringOffset          = localSocketAddressLengthOffset + atomicbuffer.SizeInt32
	localSocketAddressMaxStringLength       = CounterMetadataMaxKeyLength -
		localSocketAddressStringOffset
)

// FindLocalSocketAddress returns the bind address linked to
// channelStatusCounterID when channelStatus is ChannelEndpointStatusActive.
// Matches Java LocalSocketAddressStatus.findAddress.
//
// Behaviour:
//   - Scans LOCAL_SOCKADDR counters from id 0. Stops at the first unused
//     record.
//   - Takes the first allocated record that links to channelStatusCounterID
//     and holds ChannelEndpointStatusActive.
//   - Returns false when that record holds no address.
//   - Returns false for a NullCounterID link.
func (reader *CountersReader) FindLocalSocketAddress(
	channelStatus int64,
	channelStatusCounterID int32,
) (
	string,
	bool,
) {
	if reader == nil || channelStatus != ChannelEndpointStatusActive {
		return "", false
	}
	for keyIndex := range reader.activeLocalSocketAddressKeys(channelStatusCounterID) {
		return reader.readLocalSocketAddress(keyIndex)
	}
	return "", false
}

// FindLocalSocketAddresses returns every bind address linked to
// channelStatusCounterID when channelStatus is ChannelEndpointStatusActive.
// Matches Java LocalSocketAddressStatus.findAddresses.
//
// Behaviour:
//   - Same scan as FindLocalSocketAddress. Takes every match, not the first.
//   - Skips a match that holds no address.
//   - Returns the addresses in counter id order, or nil.
func (reader *CountersReader) FindLocalSocketAddresses(
	channelStatus int64,
	channelStatusCounterID int32,
) []string {
	if reader == nil || channelStatus != ChannelEndpointStatusActive {
		return nil
	}
	var addresses []string
	for keyIndex := range reader.activeLocalSocketAddressKeys(channelStatusCounterID) {
		if address, ok := reader.readLocalSocketAddress(keyIndex); ok {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

// activeLocalSocketAddressKeys yields the key offset of each allocated
// LOCAL_SOCKADDR record that links to channelStatusCounterID and holds
// ChannelEndpointStatusActive. The scan runs from counter id 0 and stops at
// the first unused record.
func (reader *CountersReader) activeLocalSocketAddressKeys(
	channelStatusCounterID int32,
) iter.Seq[int] {
	return func(yield func(int) bool) {
		for counterID := int32(0); counterID <= reader.maxCounterID; counterID++ {
			metadataOffset := counterMetadataOffset(counterID)
			state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
			if state == CounterRecordUnused {
				return
			}
			if !reader.counterAllocatedOfTypeAt(
				metadataOffset,
				state,
				DriverLocalSocketAddressStatusTypeID,
			) {
				continue
			}

			keyIndex := metadataOffset + CounterMetadataKeyOffset
			if reader.metadata.GetInt32(keyIndex+localSocketAddressChannelStatusIDOffset) !=
				channelStatusCounterID {
				continue
			}

			if reader.values.VolatileInt64At(CounterOffset(counterID)) != ChannelEndpointStatusActive {
				continue
			}

			if !yield(keyIndex) {
				return
			}
		}
	}
}

// readLocalSocketAddress reads the ASCII address at key+8 with the length at
// key+4. The key does not use the Agrona putStringAscii layout. Returns false
// for a length that is not positive or that runs past the metadata buffer.
func (reader *CountersReader) readLocalSocketAddress(keyIndex int) (string, bool) {
	length := reader.metadata.GetInt32(keyIndex + localSocketAddressLengthOffset)
	if length <= 0 {
		return "", false
	}
	offset := keyIndex + localSocketAddressStringOffset
	end := offset + int(length)
	if end > reader.metadata.Capacity() {
		return "", false
	}
	return string(reader.metadata.Bytes()[offset:end]), true
}

// WriteLocalSocketAddressCounterRecord seeds a LOCAL_SOCKADDR counter for tests
// using driver-parity metadata layout (length at key+4, raw ASCII at key+8).
func WriteLocalSocketAddressCounterRecord(
	meta []byte,
	values []byte,
	counterID int32,
	state int32,
	registrationID int64,
	channelStatusCounterID int32,
	bindAddress string,
	value int64,
) error {
	if len(bindAddress) > localSocketAddressMaxStringLength {
		return errLocalSocketAddressStringTooLong
	}

	metaBuffer := atomicbuffer.Wrap(meta)
	valuesBuffer := atomicbuffer.Wrap(values)
	return writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		DriverLocalSocketAddressStatusTypeID,
		value,
		func(metaOffset, valuesOffset int) error {
			keyIndex := metaOffset + CounterMetadataKeyOffset
			metaBuffer.PutInt32(
				keyIndex+localSocketAddressChannelStatusIDOffset,
				channelStatusCounterID,
			)
			metaBuffer.PutInt32(
				keyIndex+localSocketAddressLengthOffset,
				//nolint:gosec // G115: checked against localSocketAddressMaxStringLength above
				int32(len(bindAddress)),
			)
			stringOffset := keyIndex + localSocketAddressStringOffset
			copy(metaBuffer.Bytes()[stringOffset:stringOffset+len(bindAddress)], bindAddress)
			valuesBuffer.PutInt64(
				valuesOffset+CounterValueRegistrationIDOffset,
				registrationID,
			)
			return nil
		},
	)
}
