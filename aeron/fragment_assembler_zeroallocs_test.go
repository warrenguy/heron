//go:build testhooks

package aeron

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestFragmentAssembler_Handler_ZeroAllocs(t *testing.T) {
	const chunkSize = 2048
	payload := patternedPayloadForTest(3*chunkSize, 0)
	specs := fragmentedSpecsFromChunks(slices.Collect(slices.Chunk(payload, chunkSize)))
	termBuffer := atomicbuffer.Wrap(make([]byte, CommittedDataFramesTermCapacityForTest(specs)))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)

	type delivery struct {
		payloadOffset int
		payloadLength int
		header        *Header
	}
	deliveries := make([]delivery, len(offsets))
	for i, frameOffset := range offsets {
		payloadOffset, payloadLength, header := fragmentDeliveryArgs(termBuffer, frameOffset)
		deliveries[i] = delivery{payloadOffset, payloadLength, header}
	}

	var delivered, deliveredLength int
	assembler := NewFragmentAssembler(func(
		_ atomicbuffer.AtomicBuffer,
		_ int,
		length int,
		_ *Header,
	) {
		delivered++
		deliveredLength = length
	})
	handler := assembler.Handler()
	deliverMessage := func() {
		for _, frame := range deliveries {
			handler(termBuffer, frame.payloadOffset, frame.payloadLength, frame.header)
		}
	}

	const runs = 1000
	deliverMessage()
	testutil.AssertZeroAllocs(t, "FragmentAssembler reassembly", runs, deliverMessage)

	// AllocsPerRun adds one warm-up call of its own.
	require.Equal(t, runs+2, delivered)
	require.Equal(t, len(payload), deliveredLength)
}
