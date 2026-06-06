package cnc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestLayoutOffsetChain_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawRapidBufferLengths(t, TestRapidMaxFieldWide)
		computedLength := meta.ComputedLength()
		buf := make([]byte, computedLength)
		WriteMetadataFieldsToBuffer(buf, meta)

		offsets := []int{
			ToDriverOffset(),
			ToClientsOffset(meta),
			CountersMetaDataOffset(meta),
			CountersValuesOffset(meta),
			ErrorLogOffset(meta),
			computedLength,
		}

		regions := sliceCncRegions(buf, meta)
		regionLens := []int{
			len(regions.toDriver),
			len(regions.toClients),
			len(regions.countersMetaData),
			len(regions.countersValues),
			len(regions.errorLog),
		}
		totalSpan := computedLength - MetaDataLength
		regionSum := 0
		for _, length := range regionLens {
			regionSum += length
		}
		require.Equal(t, totalSpan, regionSum, "region sum vs buffer span")
		for i := range regionLens {
			regionEnd := offsets[i] + regionLens[i]
			if i < len(regionLens)-1 {
				require.Equal(t, offsets[i+1], regionEnd, "region %d end vs next start", i)
			}
		}

		require.NoError(t, validateLayoutEnd(meta))
		require.NoError(t, validateMappedBufferLengths(meta, regions))
	})
}

func TestParseMetadata_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawRapidBufferLengths(t, TestRapidMaxFieldWide)
		buf := make([]byte, MetaDataLength)
		WriteMetadataFieldsToBuffer(buf, meta)

		got, err := ParseMetadata(buf)
		require.NoError(t, err, "ParseMetadata")
		AssertParsedReadyMetadataEqual(t, got, meta)
	})
}

func TestComputeCncFileLength_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawRapidBufferLengths(t, TestRapidMaxFieldWide)
		computedLength := meta.ComputedLength()
		total := computedLength - MetaDataLength
		shift := rapid.IntRange(0, 12).Draw(t, "shift")
		align := 1 << shift

		got, err := ComputeCncFileLength(total, align)
		require.NoError(t, err, "ComputeCncFileLength")
		require.Equal(t, 0, got%align, "ComputeCncFileLength(%d,%d)=%d not aligned", total, align, got)
		require.GreaterOrEqual(
			t, got, computedLength,
			"ComputeCncFileLength(%d,%d)=%d < need %d", total, align, got, computedLength,
		)
		require.Less(
			t, got-computedLength, align,
			"ComputeCncFileLength(%d,%d)=%d overshot", total, align, got,
		)
		require.False(
			t,
			meta.IsFileLengthSufficient(computedLength-1),
			"file length %d should be insufficient for computed %d",
			computedLength-1,
			computedLength,
		)
		require.True(
			t,
			meta.IsFileLengthSufficient(computedLength),
			"file length %d should be sufficient for computed %d",
			computedLength,
			computedLength,
		)
	})
}

func TestComputedLength_hugeMetadata_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawOverflowBufferLengths(t)

		header := make([]byte, MetaDataLength)
		WriteMetadataFieldsToBuffer(header, meta)
		_, err := ParseMetadata(header)
		require.NoError(t, err, "ParseMetadata")

		computedLength := meta.ComputedLength()
		require.NoError(t, validateLayoutEnd(meta))
		require.False(
			t,
			meta.IsFileLengthSufficient(TestShortCncFileLength),
			"file length %d should be insufficient for computed %d",
			TestShortCncFileLength,
			computedLength,
		)
	})
}
