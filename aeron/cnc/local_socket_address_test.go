package cnc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

// localSocketAddressChannelStatusCounterID is the link of every record that
// activeLocalSocketAddressRecord builds.
const localSocketAddressChannelStatusCounterID int32 = 7

func TestFindLocalSocketAddress(t *testing.T) {
	const (
		channelStatusCounterID = localSocketAddressChannelStatusCounterID
		bindAddress            = "127.0.0.1:20111"
	)

	t.Run("nil reader", func(t *testing.T) {
		var reader *cnc.CountersReader
		got, ok := reader.FindLocalSocketAddress(cnc.ChannelEndpointStatusActive, channelStatusCounterID)
		require.False(t, ok)
		require.Empty(t, got)
	})

	inactive := activeLocalSocketAddressRecord(bindAddress)
	inactive.value = 0

	for _, tc := range []struct {
		name                   string
		record                 localSocketAddressRecord
		channelStatus          int64
		channelStatusCounterID int32
	}{
		{
			name:                   "inactive channel status",
			record:                 activeLocalSocketAddressRecord(bindAddress),
			channelStatus:          0,
			channelStatusCounterID: channelStatusCounterID,
		},
		{
			name:                   "wrong channel status counter id",
			record:                 activeLocalSocketAddressRecord(bindAddress),
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID + 1,
		},
		{
			name:                   "inactive local socket value",
			record:                 inactive,
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID,
		},
		{
			name:                   "zero length breaks without match",
			record:                 activeLocalSocketAddressRecord(""),
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := localSocketAddressRecordsReader(t, tc.record)
			got, ok := reader.FindLocalSocketAddress(tc.channelStatus, tc.channelStatusCounterID)
			require.False(t, ok)
			require.Empty(t, got)
		})
	}

	for _, tc := range []struct {
		name        string
		bindAddress string
	}{
		{name: "active match", bindAddress: bindAddress},
		{name: "non-loopback bind address", bindAddress: "192.168.0.5:48211"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := localSocketAddressRecordsReader(t, activeLocalSocketAddressRecord(tc.bindAddress))
			got, ok := reader.FindLocalSocketAddress(cnc.ChannelEndpointStatusActive, channelStatusCounterID)
			require.True(t, ok)
			require.Equal(t, tc.bindAddress, got)
		})
	}
}

func TestWriteLocalSocketAddressCounterRecord_boundsAddress(t *testing.T) {
	write := func(bindAddress string) ([]byte, error) {
		meta, values := syntheticCountersBuffers(t, 1)
		err := cnc.WriteLocalSocketAddressCounterRecord(
			meta, values, 0, cnc.CounterRecordAllocated, 9001, 1, bindAddress, 0,
		)
		return meta, err
	}
	_, err := write(strings.Repeat("a", 104))
	require.NoError(t, err)

	meta, err := write(strings.Repeat("a", 105))
	require.EqualError(t, err, "cnc: local socket address string exceeds the counter key")
	require.Zero(t, atomicbuffer.Wrap(meta).GetInt32(cnc.CounterMetadataTypeIDOffset))
}

// localSocketAddressRecord is one record that localSocketAddressRecordsReader
// writes.
type localSocketAddressRecord struct {
	state                  int32
	typeID                 int32
	channelStatusCounterID int32
	address                string
	value                  int64
}

// activeLocalSocketAddressRecord returns an allocated LOCAL_SOCKADDR record
// that links to localSocketAddressChannelStatusCounterID and holds
// ChannelEndpointStatusActive.
func activeLocalSocketAddressRecord(address string) localSocketAddressRecord {
	return localSocketAddressRecord{
		state:                  cnc.CounterRecordAllocated,
		typeID:                 cnc.DriverLocalSocketAddressStatusTypeID,
		channelStatusCounterID: localSocketAddressChannelStatusCounterID,
		address:                address,
		value:                  cnc.ChannelEndpointStatusActive,
	}
}

// localSocketAddressRecordsReader writes records at counter ids 0 onward. A
// record of another type keeps only its state, type id and value.
func localSocketAddressRecordsReader(
	tb countersTB,
	records ...localSocketAddressRecord,
) *cnc.CountersReader {
	tb.Helper()

	meta, values := syntheticCountersBuffers(tb, len(records))
	for index, record := range records {
		counterID := int32(index)
		if record.typeID != cnc.DriverLocalSocketAddressStatusTypeID {
			cnc.WriteCounterRecord(meta, values, counterID, record.state, record.typeID, 9001, record.value)
			continue
		}
		require.NoError(tb, cnc.WriteLocalSocketAddressCounterRecord(
			meta,
			values,
			counterID,
			record.state,
			9001,
			record.channelStatusCounterID,
			record.address,
			record.value,
		))
	}

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}

func TestFindLocalSocketAddresses(t *testing.T) {
	const channelStatusCounterID = localSocketAddressChannelStatusCounterID
	linked := activeLocalSocketAddressRecord

	t.Run("nil reader", func(t *testing.T) {
		var reader *cnc.CountersReader
		require.Nil(t, reader.FindLocalSocketAddresses(
			cnc.ChannelEndpointStatusActive,
			channelStatusCounterID,
		))
	})

	t.Run("inactive channel status", func(t *testing.T) {
		reader := localSocketAddressRecordsReader(t, linked("127.0.0.1:20111"))
		require.Nil(t, reader.FindLocalSocketAddresses(0, channelStatusCounterID))
	})

	unlinked := linked("127.0.0.1:20112")
	unlinked.channelStatusCounterID = channelStatusCounterID + 1

	inactive := linked("127.0.0.1:20113")
	inactive.value = 0

	reclaimed := linked("127.0.0.1:20114")
	reclaimed.state = cnc.CounterRecordReclaimed

	unused := linked("127.0.0.1:20115")
	unused.state = cnc.CounterRecordUnused

	for _, tc := range []struct {
		name    string
		records []localSocketAddressRecord
		want    []string
	}{
		{
			name:    "two linked active records",
			records: []localSocketAddressRecord{linked("127.0.0.1:20111"), linked("[::1]:20116")},
			want:    []string{"127.0.0.1:20111", "[::1]:20116"},
		},
		{
			name:    "unlinked record skipped",
			records: []localSocketAddressRecord{unlinked, linked("127.0.0.1:20111")},
			want:    []string{"127.0.0.1:20111"},
		},
		{
			name:    "inactive record skipped",
			records: []localSocketAddressRecord{inactive, linked("127.0.0.1:20111")},
			want:    []string{"127.0.0.1:20111"},
		},
		{
			name:    "zero length record skipped and scan continues",
			records: []localSocketAddressRecord{linked(""), linked("127.0.0.1:20111")},
			want:    []string{"127.0.0.1:20111"},
		},
		{
			name:    "reclaimed record skipped",
			records: []localSocketAddressRecord{reclaimed, linked("127.0.0.1:20111")},
			want:    []string{"127.0.0.1:20111"},
		},
		{
			name: "linked record after first unused record not seen",
			records: []localSocketAddressRecord{
				linked("127.0.0.1:20111"),
				unused,
				linked("127.0.0.1:20117"),
			},
			want: []string{"127.0.0.1:20111"},
		},
		{
			name:    "no linked record",
			records: []localSocketAddressRecord{unlinked},
			want:    nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := localSocketAddressRecordsReader(t, tc.records...)
			require.Equal(t, tc.want, reader.FindLocalSocketAddresses(
				cnc.ChannelEndpointStatusActive,
				channelStatusCounterID,
			))
		})
	}
}
