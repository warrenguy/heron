package cnc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func TestNewCountersReaderRejectsEmptyValues(t *testing.T) {
	_, err := cnc.NewCountersReader(make([]byte, cnc.CounterMetadataLength), nil)
	require.EqualError(t, err, "cnc: counters values buffer is empty")
}

func TestNewCountersReaderRejectsMisalignedValues(t *testing.T) {
	_, err := cnc.NewCountersReader(make([]byte, cnc.CounterMetadataLength), make([]byte, 64))
	require.EqualError(
		t,
		err,
		"cnc: counters values length 64 is not a multiple of 128",
	)
}

func TestNewCountersReaderRejectsInsufficientMetadata(t *testing.T) {
	values := make([]byte, cnc.CounterValueLength*2)
	meta := make([]byte, cnc.CounterMetadataLength)
	_, err := cnc.NewCountersReader(meta, values)
	require.EqualError(
		t,
		err,
		"cnc: counters metadata length 512 insufficient for values (need 1024)",
	)
}

func TestCountersReaderGetCounterValue(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 2)
	writeDenseCounterPrefix(meta, values, 1)
	cnc.WriteCounterRecord(
		meta,
		values,
		1,
		cnc.CounterRecordAllocated,
		cnc.ClientHeartbeatTypeID,
		42,
		1_700_000_000_123,
	)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	gotValue, err := reader.GetCounterValue(1)
	require.NoError(t, err)
	require.Equal(t, int64(1_700_000_000_123), gotValue)
}

func TestCountersReaderFindHeartbeat(t *testing.T) {
	tests := []struct {
		name             string
		counterCount     int
		densePrefixUntil int32
		records          []counterRecord
		lookupRegisterID int64
		lookupTypeID     int32
		wantID           int32
		wantOK           bool
	}{
		{
			name:             "allocated heartbeat counter",
			counterCount:     3,
			densePrefixUntil: 1,
			records: []counterRecord{
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 1_700_000_000_123},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           1,
			wantOK:           true,
		},
		{
			name:         "unused record stops scan",
			counterCount: 3,
			records: []counterRecord{
				{0, cnc.CounterRecordUnused, cnc.ClientHeartbeatTypeID, 42, 99},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           cnc.NullCounterID,
			wantOK:           false,
		},
		{
			name:         "type mismatch skipped",
			counterCount: 3,
			records: []counterRecord{
				{0, cnc.CounterRecordAllocated, cnc.SystemCounterTypeID, 42, 7},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           cnc.NullCounterID,
			wantOK:           false,
		},
		{
			name:         "type mismatch skipped then finds later match",
			counterCount: 3,
			records: []counterRecord{
				{0, cnc.CounterRecordAllocated, cnc.SystemCounterTypeID, 42, 7},
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 1},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           1,
			wantOK:           true,
		},
		{
			name:             "registration mismatch skipped",
			counterCount:     3,
			densePrefixUntil: 1,
			records: []counterRecord{
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 5},
			},
			lookupRegisterID: 99,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           cnc.NullCounterID,
			wantOK:           false,
		},
		{
			name:         "skips reclaimed",
			counterCount: 2,
			records: []counterRecord{
				{0, cnc.CounterRecordReclaimed, cnc.ClientHeartbeatTypeID, 42, 0},
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 1},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           1,
			wantOK:           true,
		},
		{
			name:         "returns first match",
			counterCount: 2,
			records: []counterRecord{
				{0, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 10},
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 20},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           0,
			wantOK:           true,
		},
		{
			name:         "stops at unused slot before later match",
			counterCount: 2,
			records: []counterRecord{
				{0, cnc.CounterRecordUnused, cnc.ClientHeartbeatTypeID, 42, 0},
				{1, cnc.CounterRecordAllocated, cnc.ClientHeartbeatTypeID, 42, 1},
			},
			lookupRegisterID: 42,
			lookupTypeID:     cnc.ClientHeartbeatTypeID,
			wantID:           cnc.NullCounterID,
			wantOK:           false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := newCountersReaderFromRecords(
				t,
				test.counterCount,
				test.densePrefixUntil,
				test.records,
			)

			gotID, ok := reader.FindHeartbeatCounter(
				test.lookupRegisterID,
				test.lookupTypeID,
			)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.wantID, gotID)
		})
	}
}

func TestCountersReaderGetCounterValueOutOfRange(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	_, err = reader.GetCounterValue(1)
	require.EqualError(t, err, "cnc: counter id 1 out of range: maxCounterId=0")

	_, err = reader.GetCounterValue(-1)
	require.EqualError(t, err, "cnc: counter id -1 out of range: maxCounterId=0")
}

func TestCountersReaderNilReceiver(t *testing.T) {
	var reader *cnc.CountersReader

	_, err := reader.GetCounterValue(0)
	require.EqualError(t, err, "cnc: counters reader is nil")

	counterID, ok := reader.FindHeartbeatCounter(1, cnc.ClientHeartbeatTypeID)
	require.False(t, ok)
	require.Equal(t, cnc.NullCounterID, counterID)
	require.Equal(t, atomic.AtomicBuffer{}, reader.CounterValuesBuffer())
}

func TestCountersReaderCounterValuesBufferAliasesAndVolatileRead(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	cnc.WriteCounterRecord(meta, values, 0, cnc.CounterRecordAllocated, cnc.SystemCounterTypeID, 0, 77)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	buffer := reader.CounterValuesBuffer()
	require.Equal(t, int64(77), buffer.GetInt64(0))

	buffer.PutInt64Release(0, 88)
	require.Equal(t, int64(88), buffer.GetInt64(0))

	gotValue, err := reader.GetCounterValue(0)
	require.NoError(t, err)
	require.Equal(t, int64(88), gotValue)
}

func newCountersReaderFromRecords(
	t *testing.T,
	counterCount int,
	densePrefixUntil int32,
	records []counterRecord,
) *cnc.CountersReader {
	t.Helper()

	meta, values := syntheticCountersBuffers(t, counterCount)
	if densePrefixUntil > 0 {
		writeDenseCounterPrefix(meta, values, densePrefixUntil)
	}
	for _, record := range records {
		cnc.WriteCounterRecord(
			meta,
			values,
			record.counterID,
			record.state,
			record.typeID,
			record.registrationID,
			record.value,
		)
	}

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	return reader
}

type counterRecord struct {
	counterID      int32
	state          int32
	typeID         int32
	registrationID int64
	value          int64
}

func syntheticCountersBuffers(tb testing.TB, counterCount int) ([]byte, []byte) {
	tb.Helper()
	require.Positive(tb, counterCount)

	values := make([]byte, counterCount*cnc.CounterValueLength)
	meta := make([]byte, counterCount*cnc.CounterMetadataLength)
	return meta, values
}

func writeDenseCounterPrefix(meta, values []byte, counterID int32) {
	for id := int32(0); id < counterID; id++ {
		cnc.WriteCounterRecord(meta, values, id, cnc.CounterRecordReclaimed, 0, 0, 0)
	}
}
