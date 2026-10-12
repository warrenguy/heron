package cnc

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

var errLocalSocketAddressStringOutOfBounds = errors.New(
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
	localSocketAddressMaxStringLength       = counterMetaMaxKeyLength - localSocketAddressStringOffset
)

// FindLocalSocketAddress returns the bind address linked to channelStatusCounterID
// when channelStatus is ChannelEndpointStatusActive.
//
// Matches Java LocalSocketAddressStatus.findAddress: scans LOCAL_SOCKADDR counters,
// breaks on the first record whose linked channel-status id matches, and reads raw
// ASCII bytes at key+8 using the length at key+4 (not Agrona putStringAscii layout).
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

	for counterID := int32(0); counterID <= reader.maxCounterID; counterID++ {
		metadataOffset := counterMetadataOffset(counterID)
		state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
		if state == CounterRecordUnused {
			break
		}
		if state != CounterRecordAllocated {
			continue
		}
		if reader.metadata.GetInt32(metadataOffset+CounterMetadataTypeIDOffset) !=
			DriverLocalSocketAddressStatusTypeID {
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

		length := reader.metadata.GetInt32(keyIndex + localSocketAddressLengthOffset)
		address, ok := readLocalSocketAddressString(reader.metadata, keyIndex, length)
		if ok {
			return address, true
		}
		return "", false
	}

	return "", false
}

func readLocalSocketAddressString(
	metadata atomicbuffer.AtomicBuffer,
	keyIndex int,
	length int32,
) (
	string,
	bool,
) {
	if length <= 0 {
		return "", false
	}
	offset := keyIndex + localSocketAddressStringOffset
	end := offset + int(length)
	if end > metadata.Capacity() {
		return "", false
	}
	return string(metadata.Bytes()[offset:end]), true
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
			if len(bindAddress) > localSocketAddressMaxStringLength {
				return errLocalSocketAddressStringOutOfBounds
			}
			keyIndex := metaOffset + CounterMetadataKeyOffset
			metaBuffer.PutInt32(
				keyIndex+localSocketAddressChannelStatusIDOffset,
				channelStatusCounterID,
			)
			metaBuffer.PutInt32(
				keyIndex+localSocketAddressLengthOffset,
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
