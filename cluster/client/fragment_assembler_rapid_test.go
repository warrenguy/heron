//go:build rapid

package client

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	fragmentAssemblerRapidMaxPayload = 4096
	fragmentAssemblerRapidMaxChunks  = 64
)

func TestFragmentAssembler_rapid_reassembly(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		maxChunks := min(len(payload), fragmentAssemblerRapidMaxChunks)
		chunkCount := rapid.IntRange(1, maxChunks).Draw(rt, "chunk_count")
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))
		deliverFragmentSpecsAndAssertPayload(rt, specs, payload)
	})
}

func TestFragmentAssembler_rapid_unfragmentedRoundtrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		deliverFragmentSpecsAndAssertPayload(rt, []fragmentedFrameSpec{{
			flags:   logbuffer.Unfragmented,
			payload: payload,
		}}, payload)
	})
}

func TestFragmentAssembler_rapid_orphanFragmentsNoDelegate(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		deliverEndOrphan := rapid.Bool().Draw(rt, "deliver_end_orphan")

		var (
			payload    []byte
			chunkCount int
		)
		maxChunksCap := 4
		if deliverEndOrphan {
			payload = drawFragmentAssemblerPayloadMin(rt, 2)
			maxChunks := min(len(payload), maxChunksCap)
			chunkCount = rapid.IntRange(2, maxChunks).Draw(rt, "chunk_count")
		} else {
			payload = drawFragmentAssemblerPayloadMin(rt, 3)
			maxChunks := min(len(payload), maxChunksCap)
			chunkCount = rapid.IntRange(3, maxChunks).Draw(rt, "chunk_count")
		}
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))

		assembler, termBuffer, offsets, capture := setupFragmentAssemblerRapid(rt, specs)

		var offset int
		if deliverEndOrphan {
			offset = offsets[len(offsets)-1]
		} else {
			offset = offsets[1]
		}
		deliverFragment(rt, assembler, termBuffer, offset)

		require.Empty(rt, capture.payloads)
	})
}

func TestFragmentAssembler_rapid_termOffsetGapNoDelegate(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayloadMin(rt, 2)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, 2))

		assembler, termBuffer, offsets, capture := setupFragmentAssemblerRapid(rt, specs)
		deliverFragment(rt, assembler, termBuffer, offsets[0])

		gapOffset := offsets[1] + logbuffer.AlignFrameLength(logbuffer.DataHeaderLength+1)
		writeCommittedFragmentFrames(rt, termBuffer, gapOffset, []fragmentedFrameSpec{specs[1]})
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

func setupFragmentAssemblerRapid(
	rt *rapid.T,
	specs []fragmentedFrameSpec,
) (*FragmentAssembler, atomic.AtomicBuffer, []int, *fragmentDelegateCapture) {
	termBuffer := atomic.Wrap(make([]byte, fragmentedFrameTermCapacity(specs)))
	offsets := writeCommittedFragmentFrames(rt, termBuffer, 0, specs)

	capture := newFragmentDelegateCapture()
	assembler := NewFragmentAssembler(capture.handler())
	return assembler, termBuffer, offsets, capture
}

func deliverFragmentSpecsAndAssertPayload(
	rt *rapid.T,
	specs []fragmentedFrameSpec,
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
	remaining := len(payload)
	start := 0
	for i := range chunkCount - 1 {
		after := chunkCount - i - 1
		maxSize := remaining - after
		size := 1 + int(uint32(start+i*17)%uint32(maxSize))
		chunks[i] = slices.Clone(payload[start : start+size])
		start += size
		remaining -= size
	}
	chunks[chunkCount-1] = slices.Clone(payload[start:])
	return chunks
}

func fragmentedSpecsFromChunks(chunks [][]byte) []fragmentedFrameSpec {
	specs := make([]fragmentedFrameSpec, len(chunks))
	if len(chunks) == 1 {
		specs[0] = fragmentedFrameSpec{flags: logbuffer.Unfragmented, payload: chunks[0]}
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
		specs[i] = fragmentedFrameSpec{flags: flags, payload: chunk}
	}
	return specs
}
