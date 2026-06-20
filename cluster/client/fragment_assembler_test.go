package client

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

type fragmentDelegateCapture struct {
	payloads    [][]byte
	bufferPtrs  []uintptr
	payloadPtrs []uintptr
	headers     []*aeron.Header
}

func newFragmentDelegateCapture() *fragmentDelegateCapture {
	return &fragmentDelegateCapture{}
}

func sliceDataPtr(b []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}

var noopFragmentHandler aeron.FragmentHandler = func(
	_ atomic.AtomicBuffer,
	_ int,
	_ int,
	_ *aeron.Header,
) {
}

func deliverFragments(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomic.AtomicBuffer,
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
	termBuffer atomic.AtomicBuffer,
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
	specs []fragmentedFrameSpec,
) (*fragmentDelegateCapture, *FragmentAssembler, atomic.AtomicBuffer, []int) {
	t.Helper()

	termBuffer := atomic.Wrap(make([]byte, termSize))
	offsets := writeCommittedFragmentFrames(t, termBuffer, 0, specs)
	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	return capture, assembler, termBuffer, offsets
}

func newAssemblerWithPendingBeginFragment(t *testing.T) *FragmentAssembler {
	t.Helper()

	termBuffer := atomic.Wrap(make([]byte, 512))
	offsets := writeCommittedFragmentFrames(t, termBuffer, 0, []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: []byte("pending")},
	})

	assembler := NewFragmentAssembler(noopFragmentHandler)
	deliverFragment(t, assembler, termBuffer, offsets[0])
	return assembler
}

func (capture *fragmentDelegateCapture) handler() aeron.FragmentHandler {
	return func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		bytes := buffer.Bytes()
		payload := append([]byte(nil), bytes[offset:offset+length]...)
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
	termBuffer := atomic.Wrap(termBytes)
	payload := []byte("whole-message")
	writeCommittedFragmentFrames(t, termBuffer, 0, []fragmentedFrameSpec{{
		flags:   logbuffer.Unfragmented,
		payload: payload,
	}})

	capture := newFragmentDelegateCapture()
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
	termBytes = make([]byte, 512)
	termBuffer = atomic.Wrap(termBytes)
	offsets := writeCommittedFragmentFrames(t, termBuffer, 0, []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: beginPayload},
		{flags: logbuffer.EndFlag, payload: endPayload},
	})

	capture = newFragmentDelegateCapture()
	assembler = NewFragmentAssembler(capture.handler())
	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, append(beginPayload, endPayload...), capture.payloads[0])
	require.NotEqual(t, sliceDataPtr(termBytes), capture.bufferPtrs[0])
}

func TestFragmentAssembler_reassemblesFragments(t *testing.T) {
	cases := []struct {
		name     string
		termSize int
		specs    []fragmentedFrameSpec
		want     []byte
	}{
		{
			name:     "begin_end",
			termSize: 512,
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte("hello-")},
				{flags: logbuffer.EndFlag, payload: []byte("world")},
			},
			want: []byte("hello-world"),
		},
		{
			name:     "begin_middle_end",
			termSize: 1024,
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte("a")},
				{flags: 0, payload: []byte("b")},
				{flags: logbuffer.EndFlag, payload: []byte("c")},
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
	capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 256, []fragmentedFrameSpec{{
		flags:   logbuffer.BeginFlag | logbuffer.EndFlag,
		payload: payload,
	}})
	deliverFragment(t, assembler, termBuffer, offsets[0])

	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
}

func TestFragmentAssembler_malformedAssemblyDropped(t *testing.T) {
	cases := []struct {
		name  string
		specs []fragmentedFrameSpec
	}{
		{
			name: "headerOnlyBeginWithPayloadEnd",
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte{}},
				{flags: logbuffer.EndFlag, payload: []byte("end")},
			},
		},
		{
			name: "allEmptyFragments",
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte{}},
				{flags: logbuffer.EndFlag, payload: []byte{}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 256, tc.specs)
			deliverFragments(t, assembler, termBuffer, offsets)

			require.Empty(t, capture.payloads)
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
			capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 256, []fragmentedFrameSpec{{
				flags:   tc.flags,
				payload: tc.payload,
			}})
			deliverFragment(t, assembler, termBuffer, offsets[0])

			require.Empty(t, capture.payloads)
		})
	}
}

func TestFragmentAssembler_termOffsetGapResets(t *testing.T) {
	termBuffer := atomic.Wrap(make([]byte, 1024))
	beginOffset := 0
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		beginOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		0,
		fragmentedFrameSpec{flags: logbuffer.BeginFlag, payload: []byte("begin")},
	)

	gapOffset := 256
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		gapOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(gapOffset),
		fragmentedFrameSpec{flags: logbuffer.EndFlag, payload: []byte("end")},
	)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, beginOffset)
	deliverFragment(t, assembler, termBuffer, gapOffset)

	require.Empty(t, capture.payloads)
}

func TestFragmentAssembler_newBeginResetsInProgress(t *testing.T) {
	termBuffer := atomic.Wrap(make([]byte, 1024))
	firstBegin := writeCommittedFragmentFrames(t, termBuffer, 0, []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: []byte("stale-")},
	})
	secondBegin := writeCommittedFragmentFrames(t, termBuffer, 512, []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: []byte("fresh-")},
		{flags: logbuffer.EndFlag, payload: []byte("done")},
	})

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, firstBegin[0])
	deliverFragment(t, assembler, termBuffer, secondBegin[0])
	deliverFragment(t, assembler, termBuffer, secondBegin[1])

	require.Len(t, capture.payloads, 1)
	require.Equal(t, []byte("fresh-done"), capture.payloads[0])
}

func TestFragmentAssembler_multiSessionIsolated(t *testing.T) {
	termBuffer := atomic.Wrap(make([]byte, 2048))

	sessionABegin := 0
	sessionABeginLen := writeCommittedFragmentFrame(
		t,
		termBuffer,
		sessionABegin,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		0,
		fragmentedFrameSpec{flags: logbuffer.BeginFlag, payload: []byte("A-")},
	)
	sessionAEnd := logbuffer.AlignFrameLength(int(sessionABeginLen))
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		sessionAEnd,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(sessionAEnd),
		fragmentedFrameSpec{flags: logbuffer.EndFlag, payload: []byte("A")},
	)

	sessionBOffset := sessionAEnd + logbuffer.AlignFrameLength(logbuffer.DataHeaderLength+1)
	const sessionBID = int32(1002)
	sessionBBeginLen := writeCommittedFragmentFrame(
		t,
		termBuffer,
		sessionBOffset,
		sessionBID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(sessionBOffset),
		fragmentedFrameSpec{flags: logbuffer.BeginFlag, payload: []byte("B-")},
	)
	sessionBEnd := sessionBOffset + logbuffer.AlignFrameLength(int(sessionBBeginLen))
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		sessionBEnd,
		sessionBID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(sessionBEnd),
		fragmentedFrameSpec{flags: logbuffer.EndFlag, payload: []byte("B")},
	)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, sessionABegin)
	deliverFragment(t, assembler, termBuffer, sessionBOffset)
	deliverFragment(t, assembler, termBuffer, sessionAEnd)
	deliverFragment(t, assembler, termBuffer, sessionBEnd)

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

	specs := make([]fragmentedFrameSpec, chunkCount)
	for i := range chunkCount {
		var flags uint8
		switch i {
		case 0:
			flags = logbuffer.BeginFlag
		case chunkCount - 1:
			flags = logbuffer.EndFlag
		}
		specs[i] = fragmentedFrameSpec{
			flags:   flags,
			payload: payload[i*chunkSize : (i+1)*chunkSize],
		}
	}

	termBuffer := atomic.Wrap(make([]byte, fragmentedFrameTermCapacity(specs)))
	offsets := writeCommittedFragmentFrames(t, termBuffer, 0, specs)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssemblerWithOptions(capture.handler(), 1024, false)
	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])

	require.NotContains(t, assembler.builders, fragmentFrameSessionID)
}

func TestFragmentAssembler_oversizePayloadResets(t *testing.T) {
	builder := &fragmentBufferBuilder{limit: logbuffer.MaxMessageLength}
	require.False(t, builder.ensureCapacity(1, 0))

	termBuffer := atomic.Wrap(make([]byte, 512))
	beginOffset := 0
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		beginOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		0,
		fragmentedFrameSpec{flags: logbuffer.BeginFlag, payload: []byte("begin")},
	)
	endOffset := logbuffer.AlignFrameLength(logbuffer.DataHeaderLength + len("begin"))
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		endOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(endOffset),
		fragmentedFrameSpec{flags: logbuffer.EndFlag, payload: []byte("x")},
	)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, beginOffset)

	primed := assembler.builders[fragmentFrameSessionID]
	require.NotNil(t, primed)
	primed.limit = logbuffer.MaxMessageLength

	deliverFragment(t, assembler, termBuffer, endOffset)

	require.Empty(t, capture.payloads)
	require.True(t, assembler.FreeSessionBuffer(fragmentFrameSessionID))
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
	termBuffer := atomic.Wrap(make([]byte, 1024))
	beginOffset := 0
	beginLen := writeCommittedFragmentFrame(
		t,
		termBuffer,
		beginOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		0,
		fragmentedFrameSpec{flags: logbuffer.BeginFlag, payload: []byte("begin")},
	)

	middleOffset := logbuffer.AlignFrameLength(int(beginLen))
	middleLen := writeCommittedFragmentFrame(
		t,
		termBuffer,
		middleOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		999,
		fragmentedFrameSpec{flags: 0, payload: []byte("middle")},
	)

	endOffset := logbuffer.AlignFrameLength(middleOffset + int(middleLen))
	writeCommittedFragmentFrame(
		t,
		termBuffer,
		endOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		fragmentFrameTermID,
		int32(endOffset),
		fragmentedFrameSpec{flags: logbuffer.EndFlag, payload: []byte("end")},
	)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	deliverFragment(t, assembler, termBuffer, beginOffset)
	deliverFragment(t, assembler, termBuffer, middleOffset)
	deliverFragment(t, assembler, termBuffer, endOffset)

	require.Empty(t, capture.payloads)
}

func TestFragmentAssembler_outOfOrderFragmentsDropped(t *testing.T) {
	cases := []struct {
		name  string
		specs []fragmentedFrameSpec
		order []int
	}{
		{
			name: "end_before_begin",
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte("begin-")},
				{flags: logbuffer.EndFlag, payload: []byte("end")},
			},
			order: []int{1, 0},
		},
		{
			name: "middle_before_begin",
			specs: []fragmentedFrameSpec{
				{flags: logbuffer.BeginFlag, payload: []byte("a")},
				{flags: 0, payload: []byte("b")},
				{flags: logbuffer.EndFlag, payload: []byte("c")},
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

	require.True(t, assembler.FreeSessionBuffer(fragmentFrameSessionID))
	require.False(t, assembler.FreeSessionBuffer(fragmentFrameSessionID))
}

func TestFragmentAssembler_Clear(t *testing.T) {
	assembler := newAssemblerWithPendingBeginFragment(t)
	assembler.Clear()

	require.False(t, assembler.FreeSessionBuffer(fragmentFrameSessionID))
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
			require.Equal(t, tc.want, assembler.initialBufferLength)
		})
	}
}

func TestFragmentAssembler_DirectAssemblers_stored(t *testing.T) {
	assembler := NewFragmentAssemblerWithOptions(noopFragmentHandler, 0, true)

	require.True(t, assembler.DirectAssemblers())
}
