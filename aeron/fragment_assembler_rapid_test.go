//go:build rapid

package aeron

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	fragmentAssemblerRapidMaxPayload      = 4096
	fragmentAssemblerRapidMaxChunks       = 64
	fragmentAssemblerRapidOrphanMaxChunks = 4
)

func TestFragmentAssembler_rapid_reassembly(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		chunkCount := drawFragmentAssemblerChunkCount(rt, payload, 1, fragmentAssemblerRapidMaxChunks)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))
		deliverFragmentSpecsAndAssertPayload(rt, specs, payload)
	})
}

func TestFragmentAssembler_rapid_unfragmentedRoundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, 1))
		deliverFragmentSpecsAndAssertPayload(rt, specs, payload)
	})
}

func TestFragmentAssembler_rapid_orphanFragmentsNoDelegate(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		deliverEndOrphan := rapid.Bool().Draw(rt, "deliver_end_orphan")

		minChunks := 3
		if deliverEndOrphan {
			minChunks = 2
		}
		payload := drawFragmentAssemblerPayloadMin(rt, minChunks)
		chunkCount := drawFragmentAssemblerChunkCount(
			rt,
			payload,
			minChunks,
			fragmentAssemblerRapidOrphanMaxChunks,
		)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))

		assembler, termBuffer, offsets, capture := setupFragmentAssemblerRapid(rt, specs)

		offset := offsets[1]
		if deliverEndOrphan {
			offset = offsets[len(offsets)-1]
		}
		deliverFragment(rt, assembler, termBuffer, offset)

		require.Empty(rt, capture.payloads)
	})
}

func TestFragmentAssembler_rapid_termOffsetGapNoDelegate(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayloadMin(rt, 2)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, 2))

		assembler, termBuffer, offsets, capture := setupFragmentAssemblerRapid(rt, specs)
		deliverFragment(rt, assembler, termBuffer, offsets[0])

		gapOffset := offsets[1] + logbuffer.AlignFrameLength(logbuffer.DataHeaderLength+1)
		WriteCommittedDataFrameWithFlagsForTest(
			termBuffer,
			gapOffset,
			TestPublicationSessionID,
			TestPublicationStreamID,
			TestPublicationInitialTermID,
			int32(gapOffset),
			specs[1].Flags,
			specs[1].Payload,
		)
		deliverFragment(rt, assembler, termBuffer, gapOffset)

		require.Empty(rt, capture.payloads)
	})
}

func drawFragmentAssemblerPayload(rt *rapid.T) []byte {
	return drawFragmentAssemblerPayloadMin(rt, 1)
}

func drawFragmentAssemblerPayloadMin(rt *rapid.T, minLen int) []byte {
	return rapid.SliceOfN(
		rapid.Byte(),
		minLen,
		fragmentAssemblerRapidMaxPayload,
	).Draw(rt, "payload")
}

func drawFragmentAssemblerChunkCount(rt *rapid.T, payload []byte, minChunks, maxChunksCap int) int {
	maxChunks := min(len(payload), maxChunksCap)
	return rapid.IntRange(minChunks, maxChunks).Draw(rt, "chunk_count")
}

func setupFragmentAssemblerRapid(
	rt *rapid.T,
	specs []FragmentedFrameForTest,
) (*FragmentAssembler, atomic.AtomicBuffer, []int, *fragmentDelegateCapture) {
	termBuffer := atomic.Wrap(make([]byte, CommittedDataFramesTermCapacityForTest(specs)))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)

	capture := &fragmentDelegateCapture{}
	return NewFragmentAssembler(capture.handler()), termBuffer, offsets, capture
}

func deliverFragmentSpecsAndAssertPayload(
	rt *rapid.T,
	specs []FragmentedFrameForTest,
	want []byte,
) {
	assembler, termBuffer, offsets, capture := setupFragmentAssemblerRapid(rt, specs)
	deliverFragments(rt, assembler, termBuffer, offsets)

	require.Len(rt, capture.payloads, 1)
	require.Equal(rt, want, capture.payloads[0])
}

func splitPayloadIntoChunks(payload []byte, chunkCount int) [][]byte {
	if chunkCount <= 1 {
		return [][]byte{slices.Clone(payload)}
	}

	chunks := make([][]byte, chunkCount)
	start := 0
	for i := range chunkCount - 1 {
		after := chunkCount - i - 1
		maxSize := len(payload) - start - after
		size := 1 + int(uint32(start+i*17)%uint32(maxSize))
		chunks[i] = slices.Clone(payload[start : start+size])
		start += size
	}
	chunks[chunkCount-1] = slices.Clone(payload[start:])
	return chunks
}

func fragmentedSpecsFromChunks(chunks [][]byte) []FragmentedFrameForTest {
	specs := make([]FragmentedFrameForTest, len(chunks))
	if len(chunks) == 1 {
		specs[0] = FragmentedFrameForTest{Flags: logbuffer.Unfragmented, Payload: chunks[0]}
		return specs
	}

	for i, chunk := range chunks {
		var flags uint8
		switch {
		case i == 0:
			flags = logbuffer.BeginFlag
		case i == len(chunks)-1:
			flags = logbuffer.EndFlag
		}
		specs[i] = FragmentedFrameForTest{Flags: flags, Payload: chunk}
	}
	return specs
}
