//go:build testhooks

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

var fragmentHandlerBytesAssemblerAdapters = []struct {
	name               string
	adapt              func(FragmentHandlerBytes) FragmentHandler
	unfragmentedAssert func(t *testing.T, termBytes, gotPayload []byte, gotBufferPtr uintptr)
	reassembledAssert  func(t *testing.T, termBytes, gotPayload []byte, gotBufferPtr uintptr)
}{
	{
		name:  "view",
		adapt: AdaptFragmentHandlerBytes,
		unfragmentedAssert: func(t *testing.T, termBytes, _ []byte, gotBufferPtr uintptr) {
			require.Equal(
				t,
				sliceDataPtr(termBytes[logbuffer.OffsetFramePayload:]),
				gotBufferPtr,
			)
		},
		reassembledAssert: func(t *testing.T, termBytes, _ []byte, gotBufferPtr uintptr) {
			require.NotEqual(t, sliceDataPtr(termBytes), gotBufferPtr)
		},
	},
	{
		name:  "retain",
		adapt: AdaptFragmentHandlerBytesRetain,
		unfragmentedAssert: func(t *testing.T, termBytes, gotPayload []byte, gotBufferPtr uintptr) {
			require.NotEqual(
				t,
				sliceDataPtr(termBytes[logbuffer.OffsetFramePayload:]),
				gotBufferPtr,
			)
			termBytes[logbuffer.OffsetFramePayload] = 'Z'
			require.Equal(t, byte('w'), gotPayload[0])
		},
		reassembledAssert: func(t *testing.T, termBytes, gotPayload []byte, gotBufferPtr uintptr) {
			require.NotEqual(t, sliceDataPtr(termBytes), gotBufferPtr)
			termBytes[logbuffer.OffsetFramePayload] = 'Z'
			require.Equal(t, byte('b'), gotPayload[0])
		},
	},
}

func TestAdaptFragmentHandlerBytes_fragmentAssemblerUnfragmented(t *testing.T) {
	for _, adapter := range fragmentHandlerBytesAssemblerAdapters {
		t.Run(adapter.name, func(t *testing.T) {
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

			var gotPayload []byte
			var gotBufferPtr uintptr
			assembler := NewFragmentAssembler(adapter.adapt(func(p []byte, _ *Header) {
				gotPayload = p
				gotBufferPtr = sliceDataPtr(p)
			}))

			deliverFragment(t, assembler, termBuffer, 0)

			require.Equal(t, payload, gotPayload)
			adapter.unfragmentedAssert(t, termBytes, gotPayload, gotBufferPtr)
		})
	}
}

func TestAdaptFragmentHandlerBytes_fragmentAssemblerReassembled(t *testing.T) {
	beginPayload := []byte("begin-part")
	endPayload := []byte("end-part")
	wantPayload := append(beginPayload, endPayload...)

	for _, adapter := range fragmentHandlerBytesAssemblerAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			termBytes := make([]byte, 512)
			termBuffer := atomicbuffer.Wrap(termBytes)
			offsets := WriteCommittedDataFramesForTest(termBuffer, 0, []FragmentedFrameForTest{
				{Flags: logbuffer.BeginFlag, Payload: beginPayload},
				{Flags: logbuffer.EndFlag, Payload: endPayload},
			})

			var gotPayload []byte
			var gotBufferPtr uintptr
			assembler := NewFragmentAssembler(adapter.adapt(func(p []byte, _ *Header) {
				gotPayload = p
				gotBufferPtr = sliceDataPtr(p)
			}))
			deliverFragments(t, assembler, termBuffer, offsets)

			require.Equal(t, wantPayload, gotPayload)
			adapter.reassembledAssert(t, termBytes, gotPayload, gotBufferPtr)
		})
	}
}
