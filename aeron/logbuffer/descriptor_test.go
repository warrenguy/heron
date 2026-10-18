package logbuffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDescriptorConstants(t *testing.T) {
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"PartitionCount", logbuffer.PartitionCount, 3},
		{"TermMinLength", logbuffer.TermMinLength, 65536},
		{"TermMaxLength", logbuffer.TermMaxLength, 1073741824},
		{"PageMinSize", logbuffer.PageMinSize, 4096},
		{"PageMaxSize", logbuffer.PageMaxSize, 1073741824},
		{"MaxUDPPayloadLength", logbuffer.MaxUDPPayloadLength, 65504},
		{"MetaDataLength", logbuffer.MetaDataLength, 4096},
		{"FrameAlignment", logbuffer.FrameAlignment, 32},
		{"PaddingSize", logbuffer.PaddingSize, 64},
		{"DefaultFrameHeaderMaxLength", logbuffer.DefaultFrameHeaderMaxLength, 128},
		{"DataHeaderLength", logbuffer.DataHeaderLength, 32},
		{"LogMetaDataSectionIndex", logbuffer.LogMetaDataSectionIndex, 3},
		{"OffsetActiveTermCount", logbuffer.OffsetActiveTermCount, 24},
		{"OffsetEndOfStreamPosition", logbuffer.OffsetEndOfStreamPosition, 128},
		{"OffsetCorrelationID", logbuffer.OffsetCorrelationID, 256},
		{"OffsetDefaultFrameHeader", logbuffer.OffsetDefaultFrameHeader, 320},
		{"OffsetFrameLength", logbuffer.OffsetFrameLength, 0},
		{"OffsetTermOffset", logbuffer.OffsetTermOffset, 8},
		{"OffsetFramePayload", logbuffer.OffsetFramePayload, 32},
		{"HdrTypeData", logbuffer.HdrTypeData, 1},
		{"HdrTypePad", logbuffer.HdrTypePad, 0},
		{"BeginFlag", logbuffer.BeginFlag, 0x80},
		{"EndFlag", logbuffer.EndFlag, 0x40},
		{"Unfragmented", logbuffer.Unfragmented, 0xC0},
		{"EOSFlag", logbuffer.EOSFlag, 0x20},
		{"RevokedFlag", logbuffer.RevokedFlag, 0x10},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.got, tc.name)
	}
}

func TestLayoutInvariants(t *testing.T) {
	require.Equal(t, logbuffer.MetaDataLength, logbuffer.PageMinSize)
	require.True(t, atomicbuffer.IsPowerOfTwo(logbuffer.TermMinLength))
	require.True(t, atomicbuffer.IsPowerOfTwo(logbuffer.FrameAlignment))
	require.Equal(t, 0, logbuffer.OffsetTermTailCounters)
	require.Equal(
		t,
		logbuffer.OffsetActiveTermCount,
		logbuffer.OffsetTermTailCounters+logbuffer.PartitionCount*atomicbuffer.SizeInt64,
	)
}

func TestOffsetTermTailCounter_golden(t *testing.T) {
	tests := []struct {
		name           string
		partitionIndex int
		want           int
	}{
		{"partition0", 0, 0},
		{"partition1", 1, atomicbuffer.SizeInt64},
		{"partition2", 2, 2 * atomicbuffer.SizeInt64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, logbuffer.OffsetTermTailCounter(test.partitionIndex))
		})
	}
}

func TestComputeLogLength(t *testing.T) {
	length, err := logbuffer.ComputeLogLength(logbuffer.TestTermLength, logbuffer.TestPageSize)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TestComputedLength, length)

	aligned, err := logbuffer.ComputeLogLength(logbuffer.TestTermLength, 8192)
	require.NoError(t, err)
	require.Equal(t, 204800, aligned)
}

func TestComputeLogLength_invalidInputs(t *testing.T) {
	cases := []struct {
		name       string
		termLength int
		pageSize   int
		wantErr    string
	}{
		{
			name:       "invalid term length",
			termLength: 1000,
			pageSize:   logbuffer.TestPageSize,
			wantErr: fmt.Sprintf(
				"logbuffer: term length below minimum: 1000 < %d",
				logbuffer.TestTermLength,
			),
		},
		{
			name:       "invalid page size",
			termLength: logbuffer.TestTermLength,
			pageSize:   1000,
			wantErr: fmt.Sprintf(
				"logbuffer: page size below minimum: 1000 < %d",
				logbuffer.TestPageSize,
			),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := logbuffer.ComputeLogLength(tc.termLength, tc.pageSize)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestComputeMaxMessageLength(t *testing.T) {
	require.Equal(t, 8192, logbuffer.ComputeMaxMessageLength(logbuffer.TestTermLength))
	require.Equal(t, logbuffer.MaxMessageLength, logbuffer.ComputeMaxMessageLength(1<<27))
}

func TestComputeFragmentedFrameLength(t *testing.T) {
	const maxPayload = 1376
	require.Equal(
		t,
		logbuffer.AlignFrameLength(maxPayload+logbuffer.DataHeaderLength)+
			logbuffer.AlignFrameLength(1+logbuffer.DataHeaderLength),
		logbuffer.ComputeFragmentedFrameLength(maxPayload+1, maxPayload),
	)
	require.Equal(
		t,
		3*logbuffer.AlignFrameLength(maxPayload+logbuffer.DataHeaderLength),
		logbuffer.ComputeFragmentedFrameLength(3*maxPayload, maxPayload),
	)
}

func TestCheckTermLength(t *testing.T) {
	require.NoError(t, logbuffer.CheckTermLength(logbuffer.TestTermLength))

	cases := []struct {
		name       string
		termLength int
		wantErr    string
	}{
		{
			"below minimum",
			32768,
			fmt.Sprintf(
				"logbuffer: term length below minimum: 32768 < %d",
				logbuffer.TestTermLength,
			),
		},
		{
			"above maximum",
			logbuffer.TermMaxLength + 1,
			"logbuffer: term length above maximum: 1073741825 > 1073741824",
		},
		{"not power of two", 65537, "logbuffer: term length not a power of two: 65537"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := logbuffer.CheckTermLength(tc.termLength)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestCheckPageSize(t *testing.T) {
	require.NoError(t, logbuffer.CheckPageSize(logbuffer.TestPageSize))

	cases := []struct {
		name     string
		pageSize int
		wantErr  string
	}{
		{
			"below minimum",
			2048,
			fmt.Sprintf(
				"logbuffer: page size below minimum: 2048 < %d",
				logbuffer.TestPageSize,
			),
		},
		{
			"above maximum",
			logbuffer.PageMaxSize + 1,
			"logbuffer: page size above maximum: 1073741825 > 1073741824",
		},
		{"not power of two", 4097, "logbuffer: page size not a power of two: 4097"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := logbuffer.CheckPageSize(tc.pageSize)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestCheckMTULength(t *testing.T) {
	require.NoError(t, logbuffer.CheckMTULength(logbuffer.TestMTULength))
	require.NoError(t, logbuffer.CheckMTULength(logbuffer.MaxUDPPayloadLength))

	cases := []struct {
		name      string
		mtuLength int
		wantErr   string
	}{
		{"header length", 32, "logbuffer: mtu length out of range: 32"},
		{"above maximum", 65536, "logbuffer: mtu length out of range: 65536"},
		{"not frame aligned", 1400, "logbuffer: mtu length not frame aligned: 1400"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := logbuffer.CheckMTULength(tc.mtuLength)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestTermAndMetaOffsets(t *testing.T) {
	termLength := logbuffer.TestTermLength
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"term0", logbuffer.SectionTermOffset(0, termLength), 0},
		{"term1", logbuffer.SectionTermOffset(1, termLength), termLength},
		{"term2", logbuffer.SectionTermOffset(2, termLength), 2 * termLength},
		{"meta, 4 KiB page", logbuffer.LogMetaDataOffset(logbuffer.TestComputedLength), 3 * termLength},
		{"meta, 64 KiB page", logbuffer.LogMetaDataOffset(262144), 258048},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.got, tc.name)
	}
}
