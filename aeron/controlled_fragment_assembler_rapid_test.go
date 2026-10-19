//go:build rapid

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestControlledFragmentAssembler_rapid_fragmentAssemblerParity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		chunkCount := drawFragmentAssemblerChunkCount(rt, payload, 1, fragmentAssemblerRapidMaxChunks)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))

		assembler, termBuffer, offsets, capture := setupControlledFragmentAssemblerRapid(rt, specs)

		fragmentCapture := &fragmentDelegateCapture{}
		fragmentAssembler := NewFragmentAssembler(fragmentCapture.handler())
		deliverFragments(rt, fragmentAssembler, termBuffer, offsets)
		deliverControlledFragments(rt, assembler, termBuffer, offsets)

		require.Equal(rt, fragmentCapture.payloads, capture.payloads)
		require.Equal(rt, payload, capture.payloads[0])
	})
}

func TestControlledFragmentAssembler_rapid_reassembly(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayload(rt)
		chunkCount := drawFragmentAssemblerChunkCount(rt, payload, 1, fragmentAssemblerRapidMaxChunks)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))

		assembler := deliverControlledFragmentSpecsAndAssertPayload(rt, specs, payload)
		if chunkCount == 1 {
			require.NotContains(rt, assembler.builders, TestPublicationSessionID)
			return
		}
		requireFragmentBuilderReset(rt, assembler.builders, TestPublicationSessionID)
	})
}

func TestControlledFragmentAssembler_rapid_builderReuseAcrossMessages(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		messageCount := rapid.IntRange(2, 4).Draw(rt, "message_count")
		payloads := make([][]byte, messageCount)
		var specs []FragmentedFrameForTest
		messageStarts := make([]int, messageCount+1)
		for i := range messageCount {
			// Draw past the initial capacity so later messages can grow a reused builder.
			payloads[i] = drawFragmentAssemblerPayloadRange(rt, 2, 3*fragmentAssemblerRapidMaxPayload)
			chunkCount := drawFragmentAssemblerChunkCount(
				rt,
				payloads[i],
				2,
				fragmentAssemblerRapidMaxChunks,
			)
			specs = append(
				specs,
				fragmentedSpecsFromChunks(splitPayloadIntoChunks(payloads[i], chunkCount))...,
			)
			messageStarts[i+1] = len(specs)
		}

		assembler, termBuffer, offsets, capture := setupControlledFragmentAssemblerRapid(rt, specs)

		var reuse fragmentBuilderReuse
		for i, payload := range payloads {
			messageOffsets := offsets[messageStarts[i]:messageStarts[i+1]]
			deliverControlledFragments(rt, assembler, termBuffer, messageOffsets)

			require.Len(rt, capture.payloads, i+1)
			require.Equal(rt, payload, capture.payloads[i])
			require.Equal(
				rt,
				expectedCompletedHeaderSnapshot(
					termBuffer,
					messageOffsets[0],
					messageOffsets[len(messageOffsets)-1],
					len(payload),
				),
				capture.headers[i],
			)

			reuse.requireReused(rt, assembler.builders, TestPublicationSessionID, len(payload))
		}
	})
}

func TestControlledFragmentAssembler_rapid_abortRewindRedelivery(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		payload := drawFragmentAssemblerPayloadMin(rt, 2)
		chunkCount := drawFragmentAssemblerChunkCount(rt, payload, 2, fragmentAssemblerRapidMaxChunks)
		specs := fragmentedSpecsFromChunks(splitPayloadIntoChunks(payload, chunkCount))

		assembler, termBuffer, offsets, capture := setupControlledFragmentAssemblerRapid(rt, specs)
		// Handler closes over capture; set Abort-then-Continue before END delivery.
		capture.actions = []ControlledAction{ControlledAbort, ControlledContinue}

		// Deliver through middle (or BEGIN only for 2-chunk).
		for _, offset := range offsets[:len(offsets)-1] {
			action := deliverControlledFragment(rt, assembler, termBuffer, offset)
			require.Equal(rt, ControlledContinue, action)
		}
		require.Empty(rt, capture.payloads)

		beginOffset := offsets[0]
		endOffset := offsets[len(offsets)-1]
		wantHeader := expectedCompletedHeaderSnapshot(
			termBuffer,
			beginOffset,
			endOffset,
			len(payload),
		)

		require.Equal(
			rt,
			ControlledAbort,
			deliverControlledFragment(rt, assembler, termBuffer, endOffset),
		)
		require.Len(rt, capture.payloads, 1)
		require.Equal(rt, payload, capture.payloads[0])
		require.Len(rt, capture.headers, 1)
		require.Equal(rt, wantHeader, capture.headers[0])
		require.Contains(rt, assembler.builders, TestPublicationSessionID)

		require.Equal(
			rt,
			ControlledContinue,
			deliverControlledFragment(rt, assembler, termBuffer, endOffset),
		)
		require.Len(rt, capture.payloads, 2)
		require.Equal(rt, payload, capture.payloads[1])
		require.Len(rt, capture.headers, 2)
		require.Equal(rt, wantHeader, capture.headers[1])
		require.Equal(rt, capture.headers[0], capture.headers[1])
		requireFragmentBuilderReset(rt, assembler.builders, TestPublicationSessionID)
	})
}

func TestControlledFragmentAssembler_rapid_orphanFragmentsNoDelegate(t *testing.T) {
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

		assembler, termBuffer, offsets, capture := setupControlledFragmentAssemblerRapid(rt, specs)

		offset := offsets[1]
		if deliverEndOrphan {
			offset = offsets[len(offsets)-1]
		}
		action := deliverControlledFragment(rt, assembler, termBuffer, offset)

		require.Equal(rt, ControlledContinue, action)
		require.Empty(rt, capture.payloads)
	})
}

func setupControlledFragmentAssemblerRapid(
	rt *rapid.T,
	specs []FragmentedFrameForTest,
) (
	*ControlledFragmentAssembler,
	atomicbuffer.AtomicBuffer,
	[]int,
	*controlledFragmentDelegateCapture,
) {
	termBuffer := atomicbuffer.Wrap(make([]byte, CommittedDataFramesTermCapacityForTest(specs)))
	offsets := WriteCommittedDataFramesForTest(termBuffer, 0, specs)

	capture := &controlledFragmentDelegateCapture{}
	return NewControlledFragmentAssembler(capture.handler()), termBuffer, offsets, capture
}

func deliverControlledFragmentSpecsAndAssertPayload(
	rt *rapid.T,
	specs []FragmentedFrameForTest,
	want []byte,
) *ControlledFragmentAssembler {
	assembler, termBuffer, offsets, capture := setupControlledFragmentAssemblerRapid(rt, specs)
	deliverControlledFragments(rt, assembler, termBuffer, offsets)

	require.Len(rt, capture.payloads, 1)
	require.Equal(rt, want, capture.payloads[0])
	return assembler
}
