package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

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
