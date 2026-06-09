package cnc

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	CounterValueLength    = 128
	CounterMetadataLength = 512

	CounterRecordUnused              int32 = 0
	CounterRecordAllocated           int32 = 1
	CounterRecordReclaimed           int32 = -1
	NullCounterID                    int32 = -1
	DriverPublisherLimitTypeID       int32 = 1
	DriverReceiveChannelStatusTypeID int32 = 7
	DriverSendChannelStatusTypeID    int32 = 6
	ClientHeartbeatTypeID            int32 = 11
	SystemCounterTypeID              int32 = 0

	SystemCounterControlProtocolVersionID int32 = 43

	offsetCounterMetadataState        = 0
	offsetCounterMetadataTypeID       = 4
	offsetCounterMetadataKey          = 16
	offsetCounterValuesRegistrationID = 8
)

type CountersView interface {
	GetCounterValue(counterID int32) (int64, error)

	FindHeartbeatCounter(
		registrationID int64,
		typeID int32,
	) (
		counterID int32,
		ok bool,
	)

	IsCounterActive(
		counterID int32,
		registrationID int64,
		typeID int32,
	) bool
}

type CountersReader struct {
	metadata     atomic.AtomicBuffer
	values       atomic.AtomicBuffer
	maxCounterID int32
}

var _ CountersView = (*CountersReader)(nil)

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
		metadata:     atomic.Wrap(meta[:requiredMetaLength]),
		values:       atomic.Wrap(values),
		maxCounterID: int32(counterCount - 1),
	}, nil
}

func (reader *CountersReader) GetCounterValue(counterID int32) (int64, error) {
	if reader == nil {
		return 0, errors.New("cnc: counters reader is nil")
	}
	if counterID < 0 || counterID > reader.maxCounterID {
		return 0, fmt.Errorf(
			"cnc: counter id %d out of range: maxCounterId=%d",
			counterID,
			reader.maxCounterID,
		)
	}
	return reader.values.GetInt64Volatile(counterOffset(counterID)), nil
}

func (reader *CountersReader) SetCounterValue(counterID int32, value int64) error {
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
	reader.values.PutInt64Release(counterOffset(counterID), value)
	return nil
}

func (reader *CountersReader) FindHeartbeatCounter(
	registrationID int64,
	typeID int32,
) (
	counterID int32,
	ok bool,
) {
	if reader == nil {
		return NullCounterID, false
	}

	for id := int32(0); id <= reader.maxCounterID; id++ {
		metadataOffset := counterMetadataOffset(id)
		state := reader.metadata.GetInt32Volatile(metadataOffset + offsetCounterMetadataState)
		if state == CounterRecordUnused {
			break
		}
		if !reader.counterRecordMatchesState(id, state, registrationID, typeID) {
			continue
		}
		return id, true
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

func (reader *CountersReader) counterRecordMatches(
	counterID int32,
	registrationID int64,
	typeID int32,
) bool {
	if counterID < 0 || counterID > reader.maxCounterID {
		return false
	}

	metadataOffset := counterMetadataOffset(counterID)
	state := reader.metadata.GetInt32Volatile(metadataOffset + offsetCounterMetadataState)
	return reader.counterRecordMatchesAt(counterID, metadataOffset, state, registrationID, typeID)
}

func (reader *CountersReader) counterRecordMatchesState(
	counterID int32,
	state int32,
	registrationID int64,
	typeID int32,
) bool {
	if counterID < 0 || counterID > reader.maxCounterID {
		return false
	}
	return reader.counterRecordMatchesAt(
		counterID,
		counterMetadataOffset(counterID),
		state,
		registrationID,
		typeID,
	)
}

func (reader *CountersReader) counterRecordMatchesAt(
	counterID int32,
	metadataOffset int,
	state int32,
	registrationID int64,
	typeID int32,
) bool {
	if state != CounterRecordAllocated {
		return false
	}
	if reader.metadata.GetInt32(metadataOffset+offsetCounterMetadataTypeID) != typeID {
		return false
	}

	var recordRegistrationID int64
	if counterRegistrationIDInValuesBuffer(typeID) {
		valuesOffset := counterOffset(counterID)
		recordRegistrationID = reader.values.GetInt64(
			valuesOffset + offsetCounterValuesRegistrationID,
		)
	} else {
		recordRegistrationID = reader.metadata.GetInt64(metadataOffset + offsetCounterMetadataKey)
	}
	return recordRegistrationID == registrationID
}

func counterRegistrationIDInValuesBuffer(typeID int32) bool {
	return typeID == DriverSendChannelStatusTypeID ||
		typeID == DriverReceiveChannelStatusTypeID
}

func (reader *CountersReader) CounterValuesBuffer() atomic.AtomicBuffer {
	if reader == nil {
		return atomic.AtomicBuffer{}
	}
	return reader.values
}

func counterOffset(counterID int32) int {
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

	metaBuffer := atomic.Wrap(meta)
	valuesBuffer := atomic.Wrap(values)
	_ = writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		typeID,
		value,
		func(metaOffset, _ int) error {
			metaBuffer.PutInt64(metaOffset+offsetCounterMetadataKey, registrationID)
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

	metaBuffer := atomic.Wrap(meta)
	valuesBuffer := atomic.Wrap(values)
	return writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		typeID,
		value,
		func(metaOffset, valuesOffset int) error {
			if _, err := atomic.PutStringASCII(
				metaBuffer,
				metaOffset+offsetCounterMetadataKey,
				channel,
			); err != nil {
				return err
			}
			valuesBuffer.PutInt64(
				valuesOffset+offsetCounterValuesRegistrationID,
				registrationID,
			)
			return nil
		},
	)
}

func writeCounterRecord(
	metaBuffer atomic.AtomicBuffer,
	valuesBuffer atomic.AtomicBuffer,
	counterID int32,
	state int32,
	typeID int32,
	value int64,
	writeKey func(metaOffset, valuesOffset int) error,
) error {
	metaOffset := counterMetadataOffset(counterID)
	valuesOffset := counterOffset(counterID)

	metaBuffer.PutInt32(metaOffset+offsetCounterMetadataTypeID, typeID)
	if err := writeKey(metaOffset, valuesOffset); err != nil {
		return err
	}
	valuesBuffer.PutInt64(valuesOffset, value)
	metaBuffer.PutInt32Release(metaOffset+offsetCounterMetadataState, state)
	return nil
}
