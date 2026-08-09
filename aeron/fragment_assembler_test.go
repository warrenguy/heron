//go:build testhooks

package aeron

import (
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

type fragmentDelegateCapture struct {
	payloads    [][]byte
	bufferPtrs  []uintptr
	payloadPtrs []uintptr
	headers     []*Header
}

func sliceDataPtr(b []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}

var noopFragmentHandler FragmentHandler = func(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	_ int,
	_ *Header,
) {
}

func deliverFragments(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomicbuffer.AtomicBuffer,
	offsets []int,
) {
	t.Helper()
	for _, frameOffset := range offsets {
		deliverFragment(t, assembler, termBuffer, frameOffset)
	}
}

func deliverFragmentsInOrder(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomicbuffer.AtomicBuffer,
	offsets []int,
	order []int,
) {
	t.Helper()
	for _, index := range order {
		deliverFragment(t, assembler, termBuffer, offsets[index])
	}
}

func setupFragmentAssembler(
	t *testing.T,
	termSize int,
	specs []FragmentedFrameForTest,
) (*fragmentDelegateCapture, *FragmentAssembler, atomicbuffer.AtomicBuffer, []int) {
	t.Helper()

	termBuffer := atomicbuffer.Wrap(make([]byte, termSize))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)
	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	return capture, assembler, termBuffer, offsets
}

func newAssemblerWithPendingBeginFragment(t *testing.T) *FragmentAssembler {
	t.Helper()

	_, assembler, termBuffer, offsets := setupFragmentAssembler(t, 512, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("pending")},
	})
	deliverFragment(t, assembler, termBuffer, offsets[0])
	return assembler
}

func (capture *fragmentDelegateCapture) handler() FragmentHandler {
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *Header,
	) {
		bytes := buffer.Bytes()
		payload := slices.Clone(bytes[offset : offset+length])
		capture.payloads = append(capture.payloads, payload)
		capture.bufferPtrs = append(capture.bufferPtrs, sliceDataPtr(bytes))
		if length > 0 {
			capture.payloadPtrs = append(
				capture.payloadPtrs,
				sliceDataPtr(bytes[offset:offset+length]),
			)
		}
		capture.headers = append(capture.headers, header)
	}
}

func TestFragmentAssembler_unfragmentedDelegatesWithoutCopy(t *testing.T) {
	termBytes := make([]byte, 256)
	termBuffer := atomicbuffer.Wrap(termBytes)
	payload := []byte("whole-message")
	WriteCommittedDataFrameWithFlagsForTest(
		termBuffer,
		0,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		0,
		logbuffer.Unfragmented,
		payload,
	)

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())

	deliverFragment(t, assembler, termBuffer, 0)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, sliceDataPtr(termBytes), capture.bufferPtrs[0])
	require.Equal(
		t,
		sliceDataPtr(termBytes[logbuffer.OffsetFramePayload:]),
		capture.payloadPtrs[0],
	)

	beginPayload := []byte("begin-part")
	endPayload := []byte("end-part")
	capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 512, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: beginPayload},
		{Flags: logbuffer.EndFlag, Payload: endPayload},
	})
	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, append(beginPayload, endPayload...), capture.payloads[0])
	require.NotEqual(t, sliceDataPtr(termBuffer.Bytes()), capture.bufferPtrs[0])
}

func TestFragmentAssembler_reassemblesFragments(t *testing.T) {
	cases := []struct {
		name     string
		termSize int
		specs    []FragmentedFrameForTest
		want     []byte
	}{
		{
			name:     "begin_end",
			termSize: 512,
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte("hello-")},
				{Flags: logbuffer.EndFlag, Payload: []byte("world")},
			},
			want: []byte("hello-world"),
		},
		{
			name:     "begin_middle_end",
			termSize: 1024,
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte("a")},
				{Flags: 0, Payload: []byte("b")},
				{Flags: logbuffer.EndFlag, Payload: []byte("c")},
			},
			want: []byte("abc"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, tc.termSize, tc.specs)
			deliverFragments(t, assembler, termBuffer, offsets)

			require.Len(t, capture.payloads, 1)
			require.Equal(t, tc.want, capture.payloads[0])
		})
	}
}

// TestFragmentAssembler_beginEndSingleFragment: BeginFlag|EndFlag is Unfragmented
// (0xC0); delegates without entering the reassembly builder.
func TestFragmentAssembler_beginEndSingleFragment(t *testing.T) {
	payload := []byte("single-fragment")
	capture, assembler, termBuffer, offsets := setupFragmentAssembler(
		t,
		256,
		[]FragmentedFrameForTest{{
			Flags:   logbuffer.BeginFlag | logbuffer.EndFlag,
			Payload: payload,
		}},
	)
	deliverFragment(t, assembler, termBuffer, offsets[0])

	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
}

func TestFragmentAssembler_malformedAssemblyDropped(t *testing.T) {
	cases := []struct {
		name  string
		specs []FragmentedFrameForTest
	}{
		{
			name: "headerOnlyBeginWithPayloadEnd",
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte{}},
				{Flags: logbuffer.EndFlag, Payload: []byte("end")},
			},
		},
		{
			name: "allEmptyFragments",
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte{}},
				{Flags: logbuffer.EndFlag, Payload: []byte{}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 256, tc.specs)
			deliverFragments(t, assembler, termBuffer, offsets)

			require.Empty(t, capture.payloads)
			require.NotContains(t, assembler.core.builders, TestPublicationSessionID)
		})
	}
}

func TestFragmentAssembler_orphanFragmentResets(t *testing.T) {
	cases := []struct {
		name    string
		flags   uint8
		payload []byte
	}{
		{name: "middle", flags: 0, payload: []byte("orphan-middle")},
		{name: "end", flags: logbuffer.EndFlag, payload: []byte("orphan-end")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(
				t,
				256,
				[]FragmentedFrameForTest{{
					Flags:   tc.flags,
					Payload: tc.payload,
				}},
			)
			deliverFragment(t, assembler, termBuffer, offsets[0])

			require.Empty(t, capture.payloads)
		})
	}
}

func TestFragmentAssembler_termOffsetGapResets(t *testing.T) {
	termBuffer := atomicbuffer.Wrap(make([]byte, 1024))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("begin")},
	})

	const gapOffset = 256
	WriteCommittedDataFramesForTest(termBuffer, gapOffset, []FragmentedFrameForTest{
		{Flags: logbuffer.EndFlag, Payload: []byte("end")},
	})

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, offsets[0])
	deliverFragment(t, assembler, termBuffer, gapOffset)

	require.Empty(t, capture.payloads)
	// Offset gap resets builder but retains map slot for a later BEGIN.
	require.Contains(t, assembler.core.builders, TestPublicationSessionID)
}

func TestFragmentAssembler_newBeginResetsInProgress(t *testing.T) {
	termBuffer := atomicbuffer.Wrap(make([]byte, 1024))
	firstBegin := WriteCommittedDataFramesForTest(termBuffer, 0, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("stale-")},
	})
	secondBegin := WriteCommittedDataFramesForTest(termBuffer, 512, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("fresh-")},
		{Flags: logbuffer.EndFlag, Payload: []byte("done")},
	})

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, firstBegin[0])
	deliverFragment(t, assembler, termBuffer, secondBegin[0])
	deliverFragment(t, assembler, termBuffer, secondBegin[1])

	require.Len(t, capture.payloads, 1)
	require.Equal(t, []byte("fresh-done"), capture.payloads[0])
}

func TestFragmentAssembler_multiSessionIsolated(t *testing.T) {
	termBuffer := atomicbuffer.Wrap(make([]byte, 2048))

	offsetsA := WriteCommittedDataFramesForStreamForTest(
		termBuffer,
		0,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		[]FragmentedFrameForTest{
			{Flags: logbuffer.BeginFlag, Payload: []byte("A-")},
			{Flags: logbuffer.EndFlag, Payload: []byte("A")},
		},
	)
	endFrameLen := logbuffer.FrameLengthVolatile(termBuffer, offsetsA[1])
	sessionBOffset := offsetsA[1] + logbuffer.AlignFrameLength(int(endFrameLen))

	const sessionBID = int32(1002)
	offsetsB := WriteCommittedDataFramesForStreamForTest(
		termBuffer,
		sessionBOffset,
		sessionBID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		[]FragmentedFrameForTest{
			{Flags: logbuffer.BeginFlag, Payload: []byte("B-")},
			{Flags: logbuffer.EndFlag, Payload: []byte("B")},
		},
	)

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, offsetsA[0])
	deliverFragment(t, assembler, termBuffer, offsetsB[0])
	deliverFragment(t, assembler, termBuffer, offsetsA[1])
	deliverFragment(t, assembler, termBuffer, offsetsB[1])

	require.Len(t, capture.payloads, 2)
	require.Equal(t, []byte("A-A"), capture.payloads[0])
	require.Equal(t, []byte("B-B"), capture.payloads[1])
	require.Len(t, capture.headers, 2)
	require.NotSame(t, capture.headers[0], capture.headers[1])
}

func TestFragmentAssembler_multiFragmentCapacityGrowth(t *testing.T) {
	const chunkSize = 2048
	const chunkCount = 6
	totalLen := chunkSize * chunkCount

	payload := make([]byte, totalLen)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	specs := make([]FragmentedFrameForTest, chunkCount)
	for i := range chunkCount {
		var flags uint8
		switch i {
		case 0:
			flags = logbuffer.BeginFlag
		case chunkCount - 1:
			flags = logbuffer.EndFlag
		}
		specs[i] = FragmentedFrameForTest{
			Flags:   flags,
			Payload: payload[i*chunkSize : (i+1)*chunkSize],
		}
	}

	termBuffer := atomicbuffer.Wrap(make([]byte, CommittedDataFramesTermCapacityForTest(specs)))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssemblerWithOptions(capture.handler(), 1024, false)
	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])

	require.NotContains(t, assembler.core.builders, TestPublicationSessionID)
}

func TestFragmentAssembler_oversizePayloadResets(t *testing.T) {
	builder := &fragmentBufferBuilder{limit: logbuffer.MaxMessageLength}
	require.False(t, builder.ensureCapacity(1, 0))

	termBuffer := atomicbuffer.Wrap(make([]byte, 512))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("begin")},
		{Flags: logbuffer.EndFlag, Payload: []byte("x")},
	})

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, offsets[0])

	primed := assembler.core.builders[TestPublicationSessionID]
	require.NotNil(t, primed)
	primed.limit = logbuffer.MaxMessageLength

	deliverFragment(t, assembler, termBuffer, offsets[1])

	require.Empty(t, capture.payloads)
	require.True(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
}

func TestFragmentAssembler_ensureCapacity_clampsOnOvershoot(t *testing.T) {
	builder := &fragmentBufferBuilder{
		payload: make([]byte, 9_000_000),
		limit:   9_000_000,
	}
	require.True(t, builder.ensureCapacity(1_000_000, 9_000_000))
	require.GreaterOrEqual(t, cap(builder.payload), 10_000_000)
	require.LessOrEqual(t, cap(builder.payload), logbuffer.MaxMessageLength)
}

func TestFragmentAssembler_middleWrongTermOffsetResets(t *testing.T) {
	termBuffer := atomicbuffer.Wrap(make([]byte, 1024))
	beginOffset := 0
	beginLen := WriteCommittedDataFrameWithFlagsForTest(
		termBuffer,
		beginOffset,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		0,
		logbuffer.BeginFlag,
		[]byte("begin"),
	)

	middleOffset := logbuffer.AlignFrameLength(int(beginLen))
	middleLen := WriteCommittedDataFrameWithFlagsForTest(
		termBuffer,
		middleOffset,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		999,
		0,
		[]byte("middle"),
	)

	endOffset := logbuffer.AlignFrameLength(middleOffset + int(middleLen))
	WriteCommittedDataFrameWithFlagsForTest(
		termBuffer,
		endOffset,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		int32(endOffset),
		logbuffer.EndFlag,
		[]byte("end"),
	)

	capture := &fragmentDelegateCapture{}
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, beginOffset)
	deliverFragment(t, assembler, termBuffer, middleOffset)
	deliverFragment(t, assembler, termBuffer, endOffset)

	require.Empty(t, capture.payloads)
}

func TestFragmentAssembler_outOfOrderFragmentsDropped(t *testing.T) {
	cases := []struct {
		name  string
		specs []FragmentedFrameForTest
		order []int
	}{
		{
			name: "end_before_begin",
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte("begin-")},
				{Flags: logbuffer.EndFlag, Payload: []byte("end")},
			},
			order: []int{1, 0},
		},
		{
			name: "middle_before_begin",
			specs: []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: []byte("a")},
				{Flags: 0, Payload: []byte("b")},
				{Flags: logbuffer.EndFlag, Payload: []byte("c")},
			},
			order: []int{1, 0, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 1024, tc.specs)
			deliverFragmentsInOrder(t, assembler, termBuffer, offsets, tc.order)

			require.Empty(t, capture.payloads)
		})
	}
}

func TestFragmentAssembler_FreeSessionBuffer(t *testing.T) {
	assembler := newAssemblerWithPendingBeginFragment(t)

	require.True(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
	require.False(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
}

func TestFragmentAssembler_Clear(t *testing.T) {
	assembler := newAssemblerWithPendingBeginFragment(t)
	assembler.Clear()

	require.False(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
}

func TestFragmentAssembler_initialBufferLength_clamped(t *testing.T) {
	cases := []struct {
		name  string
		input int
		want  int
	}{
		{name: "negative", input: -1, want: 0},
		{name: "over_max", input: logbuffer.MaxMessageLength + 1, want: logbuffer.MaxMessageLength},
		{name: "zero", input: 0, want: 0},
		{name: "within_range", input: 8192, want: 8192},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assembler := NewFragmentAssemblerWithOptions(nil, tc.input, false)
			require.Equal(t, tc.want, assembler.core.initialBufferLength)
		})
	}
}

func TestFragmentAssembler_DirectAssemblers_stored(t *testing.T) {
	assembler := NewFragmentAssemblerWithOptions(noopFragmentHandler, 0, true)

	require.True(t, assembler.DirectAssemblers())
}
