//go:build testhooks

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestFragmentAssembler_completedHeaderPosition(t *testing.T) {
	beginPayload := []byte("compact")
	endPayload := []byte("end")
	specs := []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: beginPayload},
		{Flags: logbuffer.EndFlag, Payload: endPayload},
	}
	termBuffer := atomicbuffer.Wrap(make([]byte, CommittedDataFramesTermCapacityForTest(specs)))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)
	want := expectedCompletedHeaderSnapshot(
		termBuffer,
		offsets[0],
		offsets[1],
		len(beginPayload)+len(endPayload),
	)

	// The completed header is valid only during the callback, so snapshot it there.
	var got []completedHeaderSnapshot
	assembler := NewFragmentAssembler(func(
		_ atomicbuffer.AtomicBuffer,
		_ int,
		_ int,
		header *Header,
	) {
		got = append(got, snapshotHeader(header))
	})
	deliverFragments(t, assembler, termBuffer, offsets)

	require.Equal(t, []completedHeaderSnapshot{want}, got)
}
