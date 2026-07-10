package cnc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
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

const writeCounterRecordChannelStatusPanic = "cnc: WriteCounterRecord cannot write channel " +
	"status counters (type 6/7); use WriteChannelStatusCounterRecord"

func TestWriteChannelStatusCounterRecordRejectsNonChannelStatusTypeIDs(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)

	err := cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		cnc.ClientHeartbeatTypeID,
		"aeron:ipc",
		42,
		1,
	)
	require.EqualError(
		t,
		err,
		"cnc: WriteChannelStatusCounterRecord requires channel status type ID (6 or 7), "+
			"got 11; use WriteCounterRecord",
	)
}

func TestWriteCounterRecordRejectsChannelStatusTypeIDs(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)

	require.PanicsWithValue(
		t,
		writeCounterRecordChannelStatusPanic,
		func() {
			cnc.WriteCounterRecord(
				meta,
				values,
				0,
				cnc.CounterRecordAllocated,
				cnc.DriverSendChannelStatusTypeID,
				9001,
				1,
			)
		},
	)
	require.PanicsWithValue(
		t,
		writeCounterRecordChannelStatusPanic,
		func() {
			cnc.WriteCounterRecord(
				meta,
				values,
				0,
				cnc.CounterRecordAllocated,
				cnc.DriverReceiveChannelStatusTypeID,
				9001,
				1,
			)
		},
	)
}

func TestCountersReaderCounterValue(t *testing.T) {
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

	gotValue, err := reader.CounterValue(1)
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

func TestCountersReaderCounterValueOutOfRange(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	_, err = reader.CounterValue(1)
	require.EqualError(t, err, "cnc: counter id 1 out of range: maxCounterId=0")

	_, err = reader.CounterValue(-1)
	require.EqualError(t, err, "cnc: counter id -1 out of range: maxCounterId=0")

	err = reader.SetCounterValue(1, 1)
	require.EqualError(t, err, "cnc: counter id 1 out of range: maxCounterId=0")
}

func TestCountersReaderIsCounterActive(t *testing.T) {
	tests := []struct {
		name             string
		counterID        int32
		registrationID   int64
		typeID           int32
		state            int32
		recordTypeID     int32
		recordRegisterID int64
		wantActive       bool
	}{
		{
			name:             "allocated matching heartbeat",
			counterID:        0,
			registrationID:   42,
			typeID:           cnc.ClientHeartbeatTypeID,
			state:            cnc.CounterRecordAllocated,
			recordTypeID:     cnc.ClientHeartbeatTypeID,
			recordRegisterID: 42,
			wantActive:       true,
		},
		{
			name:             "reclaimed counter inactive",
			counterID:        0,
			registrationID:   42,
			typeID:           cnc.ClientHeartbeatTypeID,
			state:            cnc.CounterRecordReclaimed,
			recordTypeID:     cnc.ClientHeartbeatTypeID,
			recordRegisterID: 42,
			wantActive:       false,
		},
		{
			name:             "type mismatch inactive",
			counterID:        0,
			registrationID:   42,
			typeID:           cnc.ClientHeartbeatTypeID,
			state:            cnc.CounterRecordAllocated,
			recordTypeID:     cnc.SystemCounterTypeID,
			recordRegisterID: 42,
			wantActive:       false,
		},
		{
			name:             "registration mismatch inactive",
			counterID:        0,
			registrationID:   42,
			typeID:           cnc.ClientHeartbeatTypeID,
			state:            cnc.CounterRecordAllocated,
			recordTypeID:     cnc.ClientHeartbeatTypeID,
			recordRegisterID: 99,
			wantActive:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meta, values := syntheticCountersBuffers(t, 1)
			cnc.WriteCounterRecord(
				meta,
				values,
				test.counterID,
				test.state,
				test.recordTypeID,
				test.recordRegisterID,
				0,
			)
			reader, err := cnc.NewCountersReader(meta, values)
			require.NoError(t, err)

			require.Equal(
				t,
				test.wantActive,
				reader.IsCounterActive(test.counterID, test.registrationID, test.typeID),
			)
		})
	}
}

func TestCountersReaderIsCounterActiveChannelStatus(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	channel := "aeron:ipc"
	registrationID := int64(9001)

	err := cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		cnc.DriverSendChannelStatusTypeID,
		channel,
		registrationID,
		1,
	)
	require.NoError(t, err)

	metaBuffer := atomicbuffer.Wrap(meta)
	gotChannel, _, err := atomicbuffer.GetStringASCII(
		metaBuffer,
		cnc.CounterMetadataKeyOffset,
	)
	require.NoError(t, err)
	require.Equal(t, channel, gotChannel)

	metaBuffer.PutInt64(cnc.CounterMetadataKeyOffset, 999)
	require.Equal(
		t,
		registrationID,
		atomicbuffer.Wrap(values).GetInt64(cnc.CounterValueRegistrationIDOffset),
	)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	require.True(
		t,
		reader.IsCounterActive(0, registrationID, cnc.DriverSendChannelStatusTypeID),
	)
	require.False(
		t,
		reader.IsCounterActive(0, registrationID+1, cnc.DriverSendChannelStatusTypeID),
	)
	require.False(
		t,
		reader.IsCounterActive(0, registrationID, cnc.ClientHeartbeatTypeID),
	)
}

func TestCountersReaderIsCounterActiveRegistrationIDSource(t *testing.T) {
	t.Run("publisher limit uses metadata key", func(t *testing.T) {
		meta, values := syntheticCountersBuffers(t, 1)
		cnc.WriteCounterRecord(
			meta,
			values,
			0,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			42,
			100,
		)

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(t, err)
		require.True(t, reader.IsCounterActive(0, 42, cnc.DriverPublisherLimitTypeID))
		require.False(t, reader.IsCounterActive(0, 43, cnc.DriverPublisherLimitTypeID))
	})

	t.Run("heartbeat uses metadata key", func(t *testing.T) {
		meta, values := syntheticCountersBuffers(t, 1)
		cnc.WriteCounterRecord(
			meta,
			values,
			0,
			cnc.CounterRecordAllocated,
			cnc.ClientHeartbeatTypeID,
			42,
			100,
		)

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(t, err)
		require.True(t, reader.IsCounterActive(0, 42, cnc.ClientHeartbeatTypeID))
		require.False(t, reader.IsCounterActive(0, 43, cnc.ClientHeartbeatTypeID))
	})

	t.Run("receive channel status uses values buffer", func(t *testing.T) {
		meta, values := syntheticCountersBuffers(t, 1)
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			0,
			cnc.CounterRecordAllocated,
			cnc.DriverReceiveChannelStatusTypeID,
			"aeron:udp?endpoint=localhost:40123",
			77,
			2,
		)
		require.NoError(t, err)

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(t, err)
		require.True(t, reader.IsCounterActive(0, 77, cnc.DriverReceiveChannelStatusTypeID))
		require.False(t, reader.IsCounterActive(0, 78, cnc.DriverReceiveChannelStatusTypeID))
	})
}

func TestCountersReaderIsCounterActiveOutOfRangeAndUnused(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	require.False(t, reader.IsCounterActive(0, 1, cnc.ClientHeartbeatTypeID))
	require.False(t, reader.IsCounterActive(1, 1, cnc.ClientHeartbeatTypeID))
	require.False(t, reader.IsCounterActive(-1, 1, cnc.ClientHeartbeatTypeID))
}

func TestCountersReaderNilReceiver(t *testing.T) {
	var reader *cnc.CountersReader

	_, err := reader.CounterValue(0)
	require.EqualError(t, err, "cnc: counters reader is nil")

	err = reader.SetCounterValue(0, 1)
	require.EqualError(t, err, "cnc: counters reader is nil")

	counterID, ok := reader.FindHeartbeatCounter(1, cnc.ClientHeartbeatTypeID)
	require.False(t, ok)
	require.Equal(t, cnc.NullCounterID, counterID)
	require.False(t, reader.IsCounterActive(0, 1, cnc.ClientHeartbeatTypeID))
	require.Equal(t, atomicbuffer.AtomicBuffer{}, reader.CounterValuesBuffer())
}

func TestNewCounterSlot_outOfBoundsPanics(t *testing.T) {
	_, values := syntheticCountersBuffers(t, 1)
	buffer := atomicbuffer.Wrap(values)

	cases := []struct {
		name      string
		counterID int32
		wantPanic string
	}{
		{
			name:      "counterIDPastBuffer",
			counterID: 1,
			wantPanic: "cnc: counter slot offset out of bounds: counterID=1 offset=128 capacity=128",
		},
		{
			name:      "negativeCounterID",
			counterID: -1,
			wantPanic: "cnc: counter slot offset out of bounds: counterID=-1 offset=-128 capacity=128",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.wantPanic, func() {
				cnc.NewCounterSlot(tc.counterID, buffer)
			})
		})
	}
}

func TestCountersReaderCounterValuesBufferAliasesAndVolatileRead(t *testing.T) {
	meta, values := syntheticCountersBuffers(t, 1)
	cnc.WriteCounterRecord(meta, values, 0, cnc.CounterRecordAllocated, cnc.SystemCounterTypeID, 0, 77)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	buffer := reader.CounterValuesBuffer()
	slot := cnc.NewCounterSlot(0, buffer)
	require.Equal(t, int64(77), buffer.GetInt64(0))
	require.Equal(t, int64(77), slot.Get())

	buffer.PutInt64Release(0, 88)
	require.Equal(t, int64(88), buffer.GetInt64(0))
	require.Equal(t, int64(88), slot.Get())

	gotValue, err := reader.CounterValue(0)
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

func writeAllocatedCounterForTest(
	meta []byte,
	values []byte,
	counterID int32,
	typeID int32,
	registrationID int64,
	value int64,
) error {
	switch typeID {
	case cnc.DriverSendChannelStatusTypeID, cnc.DriverReceiveChannelStatusTypeID:
		return cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			counterID,
			cnc.CounterRecordAllocated,
			typeID,
			"aeron:ipc",
			registrationID,
			value,
		)
	default:
		cnc.WriteCounterRecord(
			meta,
			values,
			counterID,
			cnc.CounterRecordAllocated,
			typeID,
			registrationID,
			value,
		)
		return nil
	}
}

func counterMetadataRegistrationKeyOffset(counterID int32) int {
	return int(counterID)*cnc.CounterMetadataLength + cnc.CounterMetadataKeyOffset
}

func syntheticCountersBuffers(tb testing.TB, counterCount int) ([]byte, []byte) {
	tb.Helper()
	require.Positive(tb, counterCount)

	values := make([]byte, counterCount*cnc.CounterValueLength)
	meta := make([]byte, counterCount*cnc.CounterMetadataLength)
	return meta, values
}

func writeDenseCounterPrefix(meta, values []byte, counterID int32) {
	for id := range counterID {
		cnc.WriteCounterRecord(meta, values, id, cnc.CounterRecordReclaimed, 0, 0, 0)
	}
}
