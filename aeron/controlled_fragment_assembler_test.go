//go:build testhooks

package aeron

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// completedHeaderSnapshot holds completed-header fields read at delegate time.
// Captures scalars so Abort redelivery can compare two deliveries without
// aliasing the mutable completedHeader pointer.
type completedHeaderSnapshot struct {
	frameLength    int32
	nextTermOffset int32
	position       int64
	fragmentedSpan int32
	flags          byte
	sessionID      int32
	streamID       int32
	termID         int32
	termOffset     int32
}

type controlledFragmentDelegateCapture struct {
	payloads    [][]byte
	bufferPtrs  []uintptr
	payloadPtrs []uintptr
	headers     []completedHeaderSnapshot
	actions     []ControlledAction
	actionIdx   int
}

func (capture *controlledFragmentDelegateCapture) handler() ControlledFragmentHandler {
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *Header,
	) ControlledAction {
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
		capture.headers = append(capture.headers, completedHeaderSnapshot{
			frameLength:    header.FrameLength(),
			nextTermOffset: header.NextTermOffset(),
			position:       header.Position(),
			fragmentedSpan: HeaderFragmentedFrameLenForTest(header),
			flags:          header.Flags(),
			sessionID:      header.SessionID(),
			streamID:       header.StreamID(),
			termID:         header.TermID(),
			termOffset:     header.TermOffset(),
		})

		action := ControlledContinue
		if capture.actionIdx < len(capture.actions) {
			action = capture.actions[capture.actionIdx]
			capture.actionIdx++
		}
		return action
	}
}

func expectedCompletedHeaderSnapshot(
	termBuffer atomicbuffer.AtomicBuffer,
	beginOffset, endOffset int,
	assembledLen int,
) completedHeaderSnapshot {
	firstFrameLength := logbuffer.FrameLengthVolatile(termBuffer, beginOffset)
	span := int32(logbuffer.ComputeFragmentedFrameLength(
		assembledLen,
		int(firstFrameLength)-logbuffer.DataHeaderLength,
	))
	beginTermOffset := logbuffer.FrameTermOffset(termBuffer, beginOffset)
	nextTermOffset := int32(logbuffer.AlignFrameLength(int(beginTermOffset) + int(span)))
	// Synthetic term buffers omit Image flyweight metadata; ImageFields are zero.
	position := logbuffer.ComputePosition(
		logbuffer.FrameTermID(termBuffer, beginOffset),
		int(nextTermOffset),
		0,
		0,
	)
	return completedHeaderSnapshot{
		frameLength:    int32(logbuffer.DataHeaderLength + assembledLen),
		nextTermOffset: nextTermOffset,
		position:       position,
		fragmentedSpan: span,
		flags: logbuffer.FrameFlags(termBuffer, beginOffset) |
			logbuffer.FrameFlags(termBuffer, endOffset),
		sessionID:  logbuffer.FrameSessionID(termBuffer, beginOffset),
		streamID:   logbuffer.FrameStreamID(termBuffer, beginOffset),
		termID:     logbuffer.FrameTermID(termBuffer, beginOffset),
		termOffset: beginTermOffset,
	}
}

var noopControlledFragmentHandler ControlledFragmentHandler = func(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	_ int,
	_ *Header,
) ControlledAction {
	return ControlledContinue
}

func deliverControlledFragment(
	t fragmentTestHelper,
	assembler *ControlledFragmentAssembler,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
) ControlledAction {
	t.Helper()

	payloadOffset, payloadLength, header := fragmentDeliveryArgs(termBuffer, frameOffset)
	return assembler.Handler()(termBuffer, payloadOffset, payloadLength, header)
}

func deliverControlledFragments(
	t fragmentTestHelper,
	assembler *ControlledFragmentAssembler,
	termBuffer atomicbuffer.AtomicBuffer,
	offsets []int,
) {
	t.Helper()
	for _, frameOffset := range offsets {
		deliverControlledFragment(t, assembler, termBuffer, frameOffset)
	}
}

func setupControlledFragmentAssembler(
	t *testing.T,
	termSize int,
	specs []FragmentedFrameForTest,
) (
	*controlledFragmentDelegateCapture,
	*ControlledFragmentAssembler,
	atomicbuffer.AtomicBuffer,
	[]int,
) {
	t.Helper()

	termBuffer := atomicbuffer.Wrap(make([]byte, termSize))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)
	capture := &controlledFragmentDelegateCapture{}
	assembler := NewControlledFragmentAssembler(capture.handler())
	return capture, assembler, termBuffer, offsets
}

func newControlledAssemblerWithPendingBeginFragment(t *testing.T) *ControlledFragmentAssembler {
	t.Helper()

	_, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
		t,
		512,
		[]FragmentedFrameForTest{
			{Flags: logbuffer.BeginFlag, Payload: []byte("pending")},
		},
	)
	deliverControlledFragment(t, assembler, termBuffer, offsets[0])
	return assembler
}

func TestControlledFragmentAssembler_unfragmentedDelegatesWithoutCopy(t *testing.T) {
	payload := []byte("whole-message")
	capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
		t,
		256,
		[]FragmentedFrameForTest{
			{Flags: logbuffer.Unfragmented, Payload: payload},
		},
	)

	action := deliverControlledFragment(t, assembler, termBuffer, offsets[0])
	termBytes := termBuffer.Bytes()

	require.Equal(t, ControlledContinue, action)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, sliceDataPtr(termBytes), capture.bufferPtrs[0])
	require.Equal(
		t,
		sliceDataPtr(termBytes[logbuffer.OffsetFramePayload:]),
		capture.payloadPtrs[0],
	)
}

func TestControlledFragmentAssembler_unfragmentedActionPassthrough(t *testing.T) {
	actions := []ControlledAction{
		ControlledAbort,
		ControlledBreak,
		ControlledCommit,
		ControlledContinue,
	}

	for _, want := range actions {
		t.Run(fmt.Sprintf("action_%d", want), func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
				t,
				256,
				[]FragmentedFrameForTest{
					{Flags: logbuffer.Unfragmented, Payload: []byte("msg")},
				},
			)
			capture.actions = []ControlledAction{want}

			got := deliverControlledFragment(t, assembler, termBuffer, offsets[0])
			require.Equal(t, want, got)
			require.Len(t, capture.payloads, 1)
		})
	}
}

func TestControlledFragmentAssembler_reassemblesFragments(t *testing.T) {
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
			capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
				t, tc.termSize, tc.specs,
			)

			for _, frameOffset := range offsets[:len(offsets)-1] {
				require.Equal(
					t,
					ControlledContinue,
					deliverControlledFragment(t, assembler, termBuffer, frameOffset),
				)
				require.Empty(t, capture.payloads)
			}

			require.Equal(
				t,
				ControlledContinue,
				deliverControlledFragment(t, assembler, termBuffer, offsets[len(offsets)-1]),
			)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, tc.want, capture.payloads[0])
			require.NotContains(t, assembler.builders, TestPublicationSessionID)
		})
	}
}

func TestControlledFragmentAssembler_endActionMatrix(t *testing.T) {
	cases := []struct {
		name       string
		action     ControlledAction
		wantKept   bool
		wantSecond []byte
	}{
		{
			name:       "continue_drops_builder",
			action:     ControlledContinue,
			wantKept:   false,
			wantSecond: nil,
		},
		{
			name:       "break_drops_builder",
			action:     ControlledBreak,
			wantKept:   false,
			wantSecond: nil,
		},
		{
			name:       "commit_drops_builder",
			action:     ControlledCommit,
			wantKept:   false,
			wantSecond: nil,
		},
		{
			name:       "abort_rewinds_builder",
			action:     ControlledAbort,
			wantKept:   true,
			wantSecond: []byte("hello-world"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
				t,
				512,
				[]FragmentedFrameForTest{
					{Flags: logbuffer.BeginFlag, Payload: []byte("hello-")},
					{Flags: logbuffer.EndFlag, Payload: []byte("world")},
				},
			)
			capture.actions = []ControlledAction{tc.action, ControlledContinue}

			require.Equal(
				t,
				ControlledContinue,
				deliverControlledFragment(t, assembler, termBuffer, offsets[0]),
			)
			got := deliverControlledFragment(t, assembler, termBuffer, offsets[1])
			require.Equal(t, tc.action, got)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, []byte("hello-world"), capture.payloads[0])

			_, kept := assembler.builders[TestPublicationSessionID]
			require.Equal(t, tc.wantKept, kept)

			// Redeliver END only.
			got = deliverControlledFragment(t, assembler, termBuffer, offsets[1])
			require.Equal(t, ControlledContinue, got)
			if tc.wantSecond == nil {
				require.Len(t, capture.payloads, 1)
				return
			}

			wantHeader := expectedCompletedHeaderSnapshot(
				termBuffer,
				offsets[0],
				offsets[1],
				len(tc.wantSecond),
			)
			require.Len(t, capture.payloads, 2)
			require.Equal(t, tc.wantSecond, capture.payloads[1])
			require.Len(t, capture.headers, 2)
			require.Equal(t, wantHeader, capture.headers[0])
			require.Equal(t, wantHeader, capture.headers[1])
			require.NotContains(t, assembler.builders, TestPublicationSessionID)
		})
	}
}

func TestControlledFragmentAssembler_malformedAssemblyDropped(t *testing.T) {
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
			capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
				t, 256, tc.specs,
			)
			for _, frameOffset := range offsets {
				require.Equal(
					t,
					ControlledContinue,
					deliverControlledFragment(t, assembler, termBuffer, frameOffset),
				)
			}

			require.Empty(t, capture.payloads)
			require.NotContains(t, assembler.builders, TestPublicationSessionID)
		})
	}
}

func TestControlledFragmentAssembler_orphanFragmentNoDelegate(t *testing.T) {
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
			capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
				t,
				256,
				[]FragmentedFrameForTest{{
					Flags:   tc.flags,
					Payload: tc.payload,
				}},
			)
			action := deliverControlledFragment(t, assembler, termBuffer, offsets[0])

			require.Equal(t, ControlledContinue, action)
			require.Empty(t, capture.payloads)
		})
	}
}

func TestControlledFragmentAssembler_FreeSessionBuffer(t *testing.T) {
	assembler := newControlledAssemblerWithPendingBeginFragment(t)

	require.True(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
	require.False(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
}

func TestControlledFragmentAssembler_Clear(t *testing.T) {
	assembler := newControlledAssemblerWithPendingBeginFragment(t)
	assembler.Clear()

	require.False(t, assembler.FreeSessionBuffer(TestPublicationSessionID))
}

func TestControlledFragmentAssembler_initialBufferLength_clamped(t *testing.T) {
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
			assembler := NewControlledFragmentAssemblerWithOptions(nil, tc.input, false)
			require.Equal(t, tc.want, assembler.initialBufferLength)
		})
	}
}

func TestControlledFragmentAssembler_DirectAssemblers_stored(t *testing.T) {
	assembler := NewControlledFragmentAssemblerWithOptions(noopControlledFragmentHandler, 0, true)

	require.True(t, assembler.DirectAssemblers())
}

func TestControlledFragmentAssembler_assembledBufferIsAssemblerOwned(t *testing.T) {
	beginPayload := []byte("begin-part")
	endPayload := []byte("end-part")
	capture, assembler, termBuffer, offsets := setupControlledFragmentAssembler(
		t,
		512,
		[]FragmentedFrameForTest{
			{Flags: logbuffer.BeginFlag, Payload: beginPayload},
			{Flags: logbuffer.EndFlag, Payload: endPayload},
		},
	)
	deliverControlledFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.payloads, 1)
	require.Equal(t, slices.Concat(beginPayload, endPayload), capture.payloads[0])
	require.NotEqual(t, sliceDataPtr(termBuffer.Bytes()), capture.bufferPtrs[0])
}
