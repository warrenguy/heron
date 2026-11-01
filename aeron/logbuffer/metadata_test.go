package logbuffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestMetadataOffsets(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"tail counters", logbuffer.OffsetTermTailCounters, 0},
		{"active term count", logbuffer.OffsetActiveTermCount, 24},
		{"end of stream", logbuffer.OffsetEndOfStreamPosition, 128},
		{"active transport count", logbuffer.OffsetActiveTransportCount, 140},
		{"correlation id", logbuffer.OffsetCorrelationID, 256},
		{"term length", logbuffer.OffsetTermLength, 276},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.got, tc.name)
	}
}

func TestPackRawTail(t *testing.T) {
	raw := logbuffer.PackRawTail(7, 1024)
	require.Equal(t, int32(7), logbuffer.TermID(raw))
	require.Equal(t, int32(1024), logbuffer.TermOffset(raw, logbuffer.TestTermLength))
}

func TestTermOffset_clamp(t *testing.T) {
	termLength := int32(logbuffer.TestTermLength)
	cases := []struct {
		name   string
		offset int32
		want   int32
	}{
		{"below term length", 1024, 1024},
		{"equal term length", termLength, termLength},
		{"above term length", termLength + 1, termLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := logbuffer.PackRawTail(7, tc.offset)
			require.Equal(t, tc.want, logbuffer.TermOffset(raw, int(termLength)))
		})
	}
}

func TestRawTailVolatile(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(buf)

	require.NoError(t, logbuffer.PutRawTail(meta, 1, logbuffer.PackRawTail(9, 512)))
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 1)

	rawTail, err := logbuffer.RawTailVolatile(meta)
	require.NoError(t, err)
	require.Equal(t, logbuffer.PackRawTail(9, 512), rawTail)
}

func TestParseMetadataView_roundtrip(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	cfg := logbuffer.DefaultSyntheticMetadataConfig()
	logbuffer.WriteSyntheticMetadata(buf, cfg)

	metaStart := logbuffer.LogMetaDataOffset(len(buf))
	metaBuf := buf[metaStart : metaStart+logbuffer.MetaDataLength]
	view, err := logbuffer.ParseMetadataView(atomicbuffer.Wrap(metaBuf))
	require.NoError(t, err)
	require.Equal(t, cfg.InitialTermID, view.InitialTermID)
	require.Equal(t, int32(logbuffer.TestTermLength), view.TermLength)
	require.Equal(t, int32(logbuffer.TestPageSize), view.PageSize)
	require.Equal(t, cfg.MTULength, view.MTULength)
	require.Equal(t, cfg.SessionID, view.SessionID)
	require.Equal(t, cfg.StreamID, view.StreamID)
	require.Equal(t, cfg.CorrelationID, view.CorrelationID)
	require.Equal(t, cfg.IsConnected, view.IsConnected)
	require.Equal(t, cfg.LogType, view.Type)
}

func TestMetadataVolatileAccessors(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(buf)
	meta.PutInt32(logbuffer.OffsetMTULength, logbuffer.TestMTULength)
	meta.PutInt32Release(logbuffer.OffsetIsConnected, 1)
	meta.PutInt32Release(logbuffer.OffsetActiveTransportCount, 3)

	require.Equal(t, int32(logbuffer.TestMTULength), logbuffer.MTULength(meta))
	require.Equal(t, int32(1), logbuffer.IsConnectedVolatile(meta))
	require.Equal(t, int32(3), logbuffer.ActiveTransportCountVolatile(meta))
	require.Equal(t, logbuffer.DataHeaderLength, logbuffer.DefaultFrameHeaderBuffer(meta).Capacity())
}

func TestDefaultFrameHeaderBuffer(t *testing.T) {
	cases := []struct {
		name         string
		headerLength int32
		wantCapacity int
	}{
		{"zero defaults to DataHeaderLength", 0, logbuffer.DataHeaderLength},
		{"negative defaults to DataHeaderLength", -1, logbuffer.DataHeaderLength},
		{"below DataHeaderLength clamps to DataHeaderLength", 16, logbuffer.DataHeaderLength},
		{"DataHeaderLength", logbuffer.DataHeaderLength, logbuffer.DataHeaderLength},
		{"at max", logbuffer.DefaultFrameHeaderMaxLength, logbuffer.DefaultFrameHeaderMaxLength},
		{
			"above max clamps to DefaultFrameHeaderMaxLength",
			logbuffer.DefaultFrameHeaderMaxLength + 1,
			logbuffer.DefaultFrameHeaderMaxLength,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, logbuffer.MetaDataLength)
			meta := atomicbuffer.Wrap(buf)
			meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, tc.headerLength)

			got := logbuffer.DefaultFrameHeaderBuffer(meta)
			require.Equal(t, tc.wantCapacity, got.Capacity())
		})
	}
}

func TestParseMetadataView_bufferTooShort(t *testing.T) {
	_, err := logbuffer.ParseMetadataView(atomicbuffer.Wrap(make([]byte, logbuffer.MetaDataLength-1)))
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: buffer too short: %d < %d",
			logbuffer.MetaDataLength-1,
			logbuffer.MetaDataLength,
		),
	)
}

func TestParseMetadataView_invalidTermLength(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(buf)
	meta.PutInt32(logbuffer.OffsetTermLength, 1000)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.TestPageSize)

	_, err := logbuffer.ParseMetadataView(meta)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: term length below minimum: 1000 < %d",
			logbuffer.TestTermLength,
		),
	)
}

func TestParseMetadataView_invalidPageSize(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(buf)
	meta.PutInt32(logbuffer.OffsetTermLength, logbuffer.TestTermLength)
	meta.PutInt32(logbuffer.OffsetPageSize, 1000)

	_, err := logbuffer.ParseMetadataView(meta)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: page size below minimum: 1000 < %d",
			logbuffer.TestPageSize,
		),
	)
}

func TestParseMetadataView_invalidMTU(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomicbuffer.Wrap(buf)
	meta.PutInt32(logbuffer.OffsetTermLength, logbuffer.TestTermLength)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.TestPageSize)

	_, err := logbuffer.ParseMetadataView(meta)
	require.EqualError(t, err, "logbuffer: mtu length out of range: 0")
}
