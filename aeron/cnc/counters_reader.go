package cnc

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	CounterValueLength    = 128
	CounterMetadataLength = 512

	CounterRecordUnused    int32 = 0
	CounterRecordAllocated int32 = 1
	CounterRecordReclaimed int32 = -1
	NullCounterID          int32 = -1

	// NullValue mirrors Aeron.NULL_VALUE, the match-any or absent sentinel for id fields.
	NullValue                        int64 = -1
	SystemCounterTypeID              int32 = 0
	DriverPublisherLimitTypeID       int32 = 1
	DriverSubscriberPositionTypeID   int32 = 4
	DriverSendChannelStatusTypeID    int32 = 6
	DriverReceiveChannelStatusTypeID int32 = 7
	ClientHeartbeatTypeID            int32 = 11

	SystemCounterControlProtocolVersionID int32 = 43

	CounterMetadataStateOffset  = 0
	CounterMetadataTypeIDOffset = atomicbuffer.SizeInt32
	// Agrona FREE_FOR_REUSE_DEADLINE at byte 8.
	counterMetaDeadlineOffset = CounterMetadataTypeIDOffset + atomicbuffer.SizeInt32
	counterMetaKeyOffset      = counterMetaDeadlineOffset + atomicbuffer.SizeInt64
	// Agrona MAX_KEY_LENGTH.
	counterMetaMaxKeyLength = 112

	CounterMetadataFreeForReuseDeadlineOffset = counterMetaDeadlineOffset
	CounterMetadataKeyOffset                  = counterMetaKeyOffset
	CounterValueRegistrationIDOffset          = atomicbuffer.SizeInt64
)

type CountersReader struct {
	metadata     atomicbuffer.AtomicBuffer
	values       atomicbuffer.AtomicBuffer
	maxCounterID int32
}

// CounterSlot wraps one bound counter value slot in the CNC values buffer.
//
// Bind after validating the counter is active for the session.
//
// Get reads the value on hot paths without per-op counter ID validation.
type CounterSlot struct {
	counterID    int32
	valuesOffset int
	values       atomicbuffer.AtomicBuffer
}

func NewCounterSlot(counterID int32, values atomicbuffer.AtomicBuffer) CounterSlot {
	offset := CounterOffset(counterID)
	capacity := values.Capacity()
	if offset < 0 || offset+atomicbuffer.SizeInt64 > capacity {
		panic(fmt.Sprintf(
			"cnc: counter slot offset out of bounds: counterID=%d offset=%d capacity=%d",
			counterID,
			offset,
			capacity,
		))
	}
	return CounterSlot{
		counterID:    counterID,
		valuesOffset: offset,
		values:       values,
	}
}

func (slot *CounterSlot) Get() int64 {
	return slot.values.VolatileInt64At(slot.valuesOffset)
}

func (slot *CounterSlot) CounterID() int32 {
	return slot.counterID
}

func NewCountersReader(meta, values []byte) (*CountersReader, error) {
	if len(values) == 0 {
		return nil, errors.New("cnc: counters values buffer is empty")
	}
	if len(values)%CounterValueLength != 0 {
		return nil, fmt.Errorf(
			"cnc: counters values length %d is not a multiple of %d",
			len(values),
			CounterValueLength,
		)
	}

	counterCount := len(values) / CounterValueLength
	requiredMetaLength := counterCount * CounterMetadataLength
	if len(meta) < requiredMetaLength {
		return nil, fmt.Errorf(
			"cnc: counters metadata length %d insufficient for values (need %d)",
			len(meta),
			requiredMetaLength,
		)
	}

	return &CountersReader{
		metadata:     atomicbuffer.Wrap(meta[:requiredMetaLength]),
		values:       atomicbuffer.Wrap(values),
		maxCounterID: int32(counterCount - 1),
	}, nil
}

func (reader *CountersReader) validateCounterID(counterID int32) error {
	if reader == nil {
		return errors.New("cnc: counters reader is nil")
	}
	if counterID < 0 || counterID > reader.maxCounterID {
		return fmt.Errorf(
			"cnc: counter id %d out of range: maxCounterId=%d",
			counterID,
			reader.maxCounterID,
		)
	}
	return nil
}

func (reader *CountersReader) CounterValue(counterID int32) (int64, error) {
	if err := reader.validateCounterID(counterID); err != nil {
		return 0, err
	}
	return reader.values.VolatileInt64At(CounterOffset(counterID)), nil
}

func (reader *CountersReader) SetCounterValue(counterID int32, value int64) error {
	if err := reader.validateCounterID(counterID); err != nil {
		return err
	}
	reader.values.PutInt64Release(CounterOffset(counterID), value)
	return nil
}

func (reader *CountersReader) FindHeartbeatCounter(
	registrationID int64,
	typeID int32,
) (
	counterID int32,
	ok bool,
) {
	return reader.findCounter(func(id int32, metadataOffset int, state int32) bool {
		return reader.counterRecordMatchesAt(id, metadataOffset, state, registrationID, typeID)
	})
}

// FindCounterOfType returns the first allocated counter id carrying typeID,
// without matching a registration id.
//
// Returns: NullCounterID and false when no allocated record carries typeID.
func (reader *CountersReader) FindCounterOfType(typeID int32) (counterID int32, ok bool) {
	return reader.findCounter(func(_ int32, metadataOffset int, state int32) bool {
		return reader.counterAllocatedOfTypeAt(metadataOffset, state, typeID)
	})
}

// findCounter returns the first counter id where match reports true.
//
// Behaviour: scans from id 0 and stops at the first unused record. The driver
// allocates records densely from id 0, so the scan covers the allocated prefix.
//
// Returns: NullCounterID and false when no record matches.
func (reader *CountersReader) findCounter(
	match func(counterID int32, metadataOffset int, state int32) bool,
) (
	counterID int32,
	ok bool,
) {
	if reader == nil {
		return NullCounterID, false
	}

	for id := int32(0); id <= reader.maxCounterID; id++ {
		metadataOffset := counterMetadataOffset(id)
		state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
		if state == CounterRecordUnused {
			break
		}
		if match(id, metadataOffset, state) {
			return id, true
		}
	}

	return NullCounterID, false
}

func (reader *CountersReader) IsCounterActive(
	counterID int32,
	registrationID int64,
	typeID int32,
) bool {
	if reader == nil {
		return false
	}
	return reader.counterRecordMatches(counterID, registrationID, typeID)
}

// IsCounterAllocatedOfType reports whether counterID is allocated with typeID,
// without matching a registration id.
//
// Use for shared endpoint counters (send/receive channel status) whose registration
// id belongs to the channel endpoint, not the individual publication or subscription.
func (reader *CountersReader) IsCounterAllocatedOfType(counterID, typeID int32) bool {
	if reader == nil {
		return false
	}
	if counterID < 0 || counterID > reader.maxCounterID {
		return false
	}
	metadataOffset := counterMetadataOffset(counterID)
	state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
	return reader.counterAllocatedOfTypeAt(metadataOffset, state, typeID)
}

func (reader *CountersReader) counterRecordMatches(
	counterID int32,
	registrationID int64,
	typeID int32,
) bool {
	if counterID < 0 || counterID > reader.maxCounterID {
		return false
	}

	metadataOffset := counterMetadataOffset(counterID)
	state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
	return reader.counterRecordMatchesAt(counterID, metadataOffset, state, registrationID, typeID)
}

func (reader *CountersReader) counterRecordMatchesAt(
	counterID int32,
	metadataOffset int,
	state int32,
	registrationID int64,
	typeID int32,
) bool {
	if !reader.counterAllocatedOfTypeAt(metadataOffset, state, typeID) {
		return false
	}

	var recordRegistrationID int64
	if counterRegistrationIDInValuesBuffer(typeID) {
		recordRegistrationID = reader.values.GetInt64Volatile(
			CounterOffset(counterID) + CounterValueRegistrationIDOffset,
		)
	} else {
		recordRegistrationID = reader.metadata.GetInt64(metadataOffset + CounterMetadataKeyOffset)
	}
	return recordRegistrationID == registrationID
}

func (reader *CountersReader) counterAllocatedOfTypeAt(
	metadataOffset int,
	state int32,
	typeID int32,
) bool {
	if state != CounterRecordAllocated {
		return false
	}
	return reader.metadata.GetInt32(metadataOffset+CounterMetadataTypeIDOffset) == typeID
}

func counterRegistrationIDInValuesBuffer(typeID int32) bool {
	return typeID == DriverSendChannelStatusTypeID ||
		typeID == DriverReceiveChannelStatusTypeID
}

// CounterValuesBuffer returns the counters values buffer for bound slot reads. Returns a
// zero buffer when the reader is nil.
func (reader *CountersReader) CounterValuesBuffer() atomicbuffer.AtomicBuffer {
	if reader == nil {
		return atomicbuffer.AtomicBuffer{}
	}
	return reader.values
}

func CounterOffset(counterID int32) int {
	return int(counterID) * CounterValueLength
}

func counterMetadataOffset(counterID int32) int {
	return int(counterID) * CounterMetadataLength
}

func WriteCounterRecord(
	meta []byte,
	values []byte,
	counterID int32,
	state int32,
	typeID int32,
	registrationID int64,
	value int64,
) {
	if counterRegistrationIDInValuesBuffer(typeID) {
		panic(
			"cnc: WriteCounterRecord cannot write channel status counters " +
				"(type 6/7); use WriteChannelStatusCounterRecord",
		)
	}

	metaBuffer := atomicbuffer.Wrap(meta)
	valuesBuffer := atomicbuffer.Wrap(values)
	_ = writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		typeID,
		value,
		func(metaOffset, _ int) error {
			metaBuffer.PutInt64(metaOffset+CounterMetadataKeyOffset, registrationID)
			return nil
		},
	)
}

func WriteChannelStatusCounterRecord(
	meta []byte,
	values []byte,
	counterID int32,
	state int32,
	typeID int32,
	channel string,
	registrationID int64,
	value int64,
) error {
	if !counterRegistrationIDInValuesBuffer(typeID) {
		return fmt.Errorf(
			"cnc: WriteChannelStatusCounterRecord requires channel status type ID (6 or 7), "+
				"got %d; use WriteCounterRecord",
			typeID,
		)
	}

	metaBuffer := atomicbuffer.Wrap(meta)
	valuesBuffer := atomicbuffer.Wrap(values)
	return writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		typeID,
		value,
		func(metaOffset, valuesOffset int) error {
			if _, err := atomicbuffer.PutStringASCII(
				metaBuffer,
				metaOffset+CounterMetadataKeyOffset,
				channel,
			); err != nil {
				return err
			}
			valuesBuffer.PutInt64(
				valuesOffset+CounterValueRegistrationIDOffset,
				registrationID,
			)
			return nil
		},
	)
}

func writeCounterRecord(
	metaBuffer atomicbuffer.AtomicBuffer,
	valuesBuffer atomicbuffer.AtomicBuffer,
	counterID int32,
	state int32,
	typeID int32,
	value int64,
	writeKey func(metaOffset, valuesOffset int) error,
) error {
	metaOffset := counterMetadataOffset(counterID)
	valuesOffset := CounterOffset(counterID)

	metaBuffer.PutInt32(metaOffset+CounterMetadataTypeIDOffset, typeID)
	if err := writeKey(metaOffset, valuesOffset); err != nil {
		return err
	}
	valuesBuffer.PutInt64(valuesOffset, value)
	metaBuffer.PutInt32Release(metaOffset+CounterMetadataStateOffset, state)
	return nil
}
