package aeron_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

var compactTwoFragments = []aeron.FragmentedFrameForTest{
	{Flags: logbuffer.BeginFlag, Payload: []byte("compact")},
	{Flags: logbuffer.EndFlag, Payload: []byte("end")},
}

type completedFragmentFixture struct {
	termBuffer         atomic.AtomicBuffer
	beginFrameOffset   int
	endFrameOffset     int
	completedHeaderBuf [logbuffer.DataHeaderLength]byte
	lastFragment       *aeron.Header
	firstFrameLength   int32
	assembledLen       int
}

func newCompletedFragmentFixture(
	image *aeron.Image,
	termBuffer atomic.AtomicBuffer,
	frameOffsets []int,
	fragments []aeron.FragmentedFrameForTest,
) completedFragmentFixture {
	var completedHeaderBuf [logbuffer.DataHeaderLength]byte
	copy(
		completedHeaderBuf[:],
		termBuffer.Bytes()[frameOffsets[0]:frameOffsets[0]+logbuffer.DataHeaderLength],
	)

	lastFragment := aeron.ImageHeaderForTest(image)
	lastFragment.SetBuffer(termBuffer)
	lastFragment.SetOffset(frameOffsets[len(frameOffsets)-1])

	return completedFragmentFixture{
		termBuffer:         termBuffer,
		beginFrameOffset:   frameOffsets[0],
		endFrameOffset:     frameOffsets[len(frameOffsets)-1],
		completedHeaderBuf: completedHeaderBuf,
		lastFragment:       lastFragment,
		firstFrameLength:   logbuffer.FrameLengthVolatile(termBuffer, frameOffsets[0]),
		assembledLen:       len(reassembledPayload(fragments)),
	}
}

func completedFragmentFixtureForFragments(
	tb testing.TB,
	fragments []aeron.FragmentedFrameForTest,
) completedFragmentFixture {
	tb.Helper()

	sub := testSubscription(tb)
	image := testImage(tb, sub)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	frameOffsets := aeron.WriteCommittedDataFramesForTest(termBuffer, 0, fragments)
	return newCompletedFragmentFixture(image, termBuffer, frameOffsets, fragments)
}

func (fixture completedFragmentFixture) maxPayloadLength() int {
	return int(fixture.firstFrameLength) - logbuffer.DataHeaderLength
}

func (fixture completedFragmentFixture) expectedFragmentedSpan() int32 {
	return int32(logbuffer.ComputeFragmentedFrameLength(
		fixture.assembledLen,
		fixture.maxPayloadLength(),
	))
}

func assertConfigureCompletedFragmentOracle(
	tb require.TestingT,
	fixture completedFragmentFixture,
	checkGoldenBytes bool,
) {
	if helper, ok := tb.(interface{ Helper() }); ok {
		helper.Helper()
	}

	expectedSpan := fixture.expectedFragmentedSpan()

	beginFlags := logbuffer.FrameFlags(fixture.termBuffer, fixture.beginFrameOffset)
	endFlags := logbuffer.FrameFlags(fixture.termBuffer, fixture.endFrameOffset)

	var completedHeader aeron.Header
	completedHeader.ConfigureCompletedFragment(
		fixture.completedHeaderBuf[:],
		fixture.assembledLen,
		fixture.lastFragment,
	)

	require.Equal(tb, expectedSpan, aeron.HeaderFragmentedFrameLenForTest(&completedHeader))

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	expectedNextTermOffset := int32(
		logbuffer.AlignFrameLength(int(completedHeader.TermOffset()) + int(expectedSpan)),
	)
	require.Equal(tb, expectedNextTermOffset, completedHeader.NextTermOffset())
	require.Equal(
		tb,
		logbuffer.ComputePosition(
			completedHeader.TermID(),
			expectedNextTermOffset,
			shift,
			aeron.TestPublicationInitialTermID,
		),
		completedHeader.Position(),
	)

	// FrameLength uses release-patched length field read via FrameLengthVolatile.
	require.Equal(
		tb,
		int32(logbuffer.DataHeaderLength+fixture.assembledLen),
		completedHeader.FrameLength(),
	)
	require.Equal(tb, byte(beginFlags|endFlags), completedHeader.Flags())

	require.Equal(
		tb,
		logbuffer.FrameSessionID(fixture.termBuffer, fixture.beginFrameOffset),
		completedHeader.SessionID(),
	)
	require.Equal(
		tb,
		logbuffer.FrameStreamID(fixture.termBuffer, fixture.beginFrameOffset),
		completedHeader.StreamID(),
	)
	require.Equal(
		tb,
		logbuffer.FrameTermID(fixture.termBuffer, fixture.beginFrameOffset),
		completedHeader.TermID(),
	)
	require.Equal(
		tb,
		logbuffer.FrameTermOffset(fixture.termBuffer, fixture.beginFrameOffset),
		completedHeader.TermOffset(),
	)
	require.Equal(tb, int32(logbuffer.HdrTypeData), completedHeader.Type())
	require.Equal(
		tb,
		logbuffer.FrameReservedValue(fixture.termBuffer, fixture.beginFrameOffset),
		completedHeader.ReservedValue(),
	)
	require.Equal(tb, fixture.lastFragment.ImageFields(), completedHeader.ImageFields())

	if checkGoldenBytes {
		require.Equal(
			tb,
			uint32(logbuffer.DataHeaderLength+fixture.assembledLen),
			binary.LittleEndian.Uint32(fixture.completedHeaderBuf[logbuffer.OffsetFrameLength:]),
		)
		require.Equal(tb, byte(beginFlags|endFlags), fixture.completedHeaderBuf[logbuffer.OffsetFlags])
	}
}

func TestHeader_ConfigureCompletedFragment_matchesFragmentedSpan(t *testing.T) {
	cases := []struct {
		name             string
		fragments        []aeron.FragmentedFrameForTest
		checkGoldenBytes bool
	}{
		{
			// Two on-wire fragments with a compact (unpadded) committed first frame;
			// assembled payload 7+3 bytes yields ComputeFragmentedFrameLength fragmentCount==2.
			name:      "compact_first_frame_two_logical_fragments",
			fragments: compactTwoFragments,
		},
		{
			name: "two_fragments",
			fragments: []aeron.FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte("hello-world-!!")},
				{Flags: logbuffer.EndFlag, Payload: []byte("tail")},
			},
			checkGoldenBytes: true,
		},
		{
			name: "three_fragments",
			fragments: []aeron.FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: make([]byte, 100)},
				{Flags: 0, Payload: make([]byte, 50)},
				{Flags: logbuffer.EndFlag, Payload: make([]byte, 25)},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := completedFragmentFixtureForFragments(t, tc.fragments)
			assertConfigureCompletedFragmentOracle(t, fixture, tc.checkGoldenBytes)
		})
	}
}

func TestHeader_ConfigureCompletedFragment_committedFirstFrameLength(t *testing.T) {
	fixture := completedFragmentFixtureForFragments(t, compactTwoFragments)

	alignedFirstFrameLength := int32(
		logbuffer.AlignFragmentChunkLength(len(compactTwoFragments[0].Payload)),
	)
	require.NotEqual(t, alignedFirstFrameLength, fixture.firstFrameLength)
	require.Equal(
		t,
		int32(len(compactTwoFragments[0].Payload)+logbuffer.DataHeaderLength),
		fixture.firstFrameLength,
	)

	wrongSpan := int32(logbuffer.ComputeFragmentedFrameLength(
		fixture.assembledLen,
		int(alignedFirstFrameLength)-logbuffer.DataHeaderLength,
	))
	require.NotEqual(t, fixture.expectedFragmentedSpan(), wrongSpan)

	assertConfigureCompletedFragmentOracle(t, fixture, false)
}

func TestHeader_ConfigureCompletedFragment_invalidInputsPanic(t *testing.T) {
	fixture := completedFragmentFixtureForFragments(t, compactTwoFragments)

	cases := []struct {
		name string
		call func(header *aeron.Header, fixture completedFragmentFixture)
	}{
		{
			name: "nil_lastFragment",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				header.ConfigureCompletedFragment(
					fixture.completedHeaderBuf[:],
					fixture.assembledLen,
					nil,
				)
			},
		},
		{
			name: "lastFragment_missing_end_flag",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				var middleFragment aeron.Header
				middleFragment.SetBuffer(fixture.termBuffer)
				middleFragment.SetOffset(fixture.beginFrameOffset)
				header.ConfigureCompletedFragment(
					fixture.completedHeaderBuf[:],
					fixture.assembledLen,
					&middleFragment,
				)
			},
		},
		{
			name: "short_captured_header",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				header.ConfigureCompletedFragment(
					fixture.completedHeaderBuf[:logbuffer.DataHeaderLength-1],
					fixture.assembledLen,
					fixture.lastFragment,
				)
			},
		},
		{
			name: "firstFrameLength_shorter_than_header",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				shortHeader := fixture.completedHeaderBuf
				logbuffer.PutFrameLengthOrdered(
					atomic.Wrap(shortHeader[:]),
					0,
					int32(logbuffer.DataHeaderLength-1),
				)
				header.ConfigureCompletedFragment(
					shortHeader[:],
					fixture.assembledLen,
					fixture.lastFragment,
				)
			},
		},
		{
			name: "firstFrameLength_equal_to_header",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				equalHeader := fixture.completedHeaderBuf
				logbuffer.PutFrameLengthOrdered(
					atomic.Wrap(equalHeader[:]),
					0,
					int32(logbuffer.DataHeaderLength),
				)
				header.ConfigureCompletedFragment(
					equalHeader[:],
					fixture.assembledLen,
					fixture.lastFragment,
				)
			},
		},
		{
			name: "non_positive_assembledPayloadLength",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				header.ConfigureCompletedFragment(
					fixture.completedHeaderBuf[:],
					0,
					fixture.lastFragment,
				)
			},
		},
		{
			name: "assembledPayloadLength_exceeds_MaxMessageLength",
			call: func(header *aeron.Header, fixture completedFragmentFixture) {
				header.ConfigureCompletedFragment(
					fixture.completedHeaderBuf[:],
					logbuffer.MaxMessageLength+1,
					fixture.lastFragment,
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var header aeron.Header
			require.Panics(t, func() {
				tc.call(&header, fixture)
			})
		})
	}
}

func TestHeader_fragmentedFrameLen_overridesOccupancy(t *testing.T) {
	const frameOffset = 0
	image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, frameOffset)

	header := aeron.ImageHeaderForTest(image)
	header.SetBuffer(aeron.ImageTermBuffer0ForTest(image))
	header.SetOffset(frameOffset)

	const fragmentedSpan int32 = 256
	aeron.HeaderSetFragmentedFrameLenForTest(header, fragmentedSpan)

	alignedSpan := logbuffer.AlignFrameLength(int(frameOffset) + int(fragmentedSpan))
	require.Equal(t, int32(alignedSpan), header.NextTermOffset())
	require.Equal(t, int64(alignedSpan), header.Position())
	require.Equal(t, frameLength, header.FrameLength())
}
