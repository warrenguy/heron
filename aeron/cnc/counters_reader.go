package cnc

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	CounterValueLength    = 128
	CounterMetadataLength = 512

	CounterRecordUnused    int32 = 0
	CounterRecordAllocated int32 = 1
	CounterRecordReclaimed int32 = -1
	NullCounterID          int32 = -1
	ClientHeartbeatTypeID  int32 = 11
	SystemCounterTypeID    int32 = 0

	SystemCounterControlProtocolVersionID int32 = 43

	offsetCounterMetadataState  = 0
	offsetCounterMetadataTypeID = 4
	offsetCounterMetadataKey    = 16
)

type CountersReader struct {
	metadata     atomic.AtomicBuffer
	values       atomic.AtomicBuffer
	maxCounterID int32
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
	return reader.counterRecordMatchesState(counterID, state, registrationID, typeID)
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
	if state != CounterRecordAllocated {
		return false
	}

	metadataOffset := counterMetadataOffset(counterID)
	if reader.metadata.GetInt32(metadataOffset+offsetCounterMetadataTypeID) != typeID {
		return false
	}
	if reader.metadata.GetInt64(metadataOffset+offsetCounterMetadataKey) != registrationID {
		return false
	}
	return true
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
	metaBuffer := atomic.Wrap(meta)
	valuesBuffer := atomic.Wrap(values)

	metaOffset := counterMetadataOffset(counterID)
	valuesOffset := counterOffset(counterID)

	metaBuffer.PutInt32(metaOffset+offsetCounterMetadataTypeID, typeID)
	metaBuffer.PutInt64(metaOffset+offsetCounterMetadataKey, registrationID)
	valuesBuffer.PutInt64(valuesOffset, value)
	metaBuffer.PutInt32Release(metaOffset+offsetCounterMetadataState, state)
}
