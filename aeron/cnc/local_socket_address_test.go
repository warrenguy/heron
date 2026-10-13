package cnc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestFindLocalSocketAddress(t *testing.T) {
	const (
		channelStatusCounterID int32 = 7
		bindAddress                  = "127.0.0.1:20111"
	)

	t.Run("nil reader", func(t *testing.T) {
		var reader *cnc.CountersReader
		got, ok := reader.FindLocalSocketAddress(cnc.ChannelEndpointStatusActive, channelStatusCounterID)
		require.False(t, ok)
		require.Empty(t, got)
	})

	for _, tc := range []struct {
		name                   string
		setup                  func(t *testing.T) *cnc.CountersReader
		channelStatus          int64
		channelStatusCounterID int32
	}{
		{
			name: "inactive channel status",
			setup: func(t *testing.T) *cnc.CountersReader {
				return localSocketAddressTestReader(t, channelStatusCounterID, bindAddress)
			},
			channelStatus:          0,
			channelStatusCounterID: channelStatusCounterID,
		},
		{
			name: "wrong channel status counter id",
			setup: func(t *testing.T) *cnc.CountersReader {
				return localSocketAddressTestReader(t, channelStatusCounterID, bindAddress)
			},
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID + 1,
		},
		{
			name: "inactive local socket value",
			setup: func(t *testing.T) *cnc.CountersReader {
				return localSocketAddressTestReaderWithValue(t, channelStatusCounterID, bindAddress, 0)
			},
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID,
		},
		{
			name: "zero length breaks without match",
			setup: func(t *testing.T) *cnc.CountersReader {
				return localSocketAddressTestReader(t, channelStatusCounterID, "")
			},
			channelStatus:          cnc.ChannelEndpointStatusActive,
			channelStatusCounterID: channelStatusCounterID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.setup(t)
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
			reader := localSocketAddressTestReader(t, channelStatusCounterID, tc.bindAddress)
			got, ok := reader.FindLocalSocketAddress(cnc.ChannelEndpointStatusActive, channelStatusCounterID)
			require.True(t, ok)
			require.Equal(t, tc.bindAddress, got)
		})
	}
}

func localSocketAddressTestReader(
	t *testing.T,
	channelStatusCounterID int32,
	bindAddress string,
) *cnc.CountersReader {
	t.Helper()
	return localSocketAddressTestReaderWithValue(
		t,
		channelStatusCounterID,
		bindAddress,
		cnc.ChannelEndpointStatusActive,
	)
}

func localSocketAddressTestReaderWithValue(
	t *testing.T,
	channelStatusCounterID int32,
	bindAddress string,
	value int64,
) *cnc.CountersReader {
	t.Helper()

	meta, values := syntheticCountersBuffers(t, 1)
	require.NoError(t, cnc.WriteLocalSocketAddressCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		9001,
		channelStatusCounterID,
		bindAddress,
		value,
	))

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	return reader
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
