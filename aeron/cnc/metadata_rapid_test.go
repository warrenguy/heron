package cnc

import (
	"testing"

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
		if regionSum != totalSpan {
			t.Fatalf("region sum %d != buffer span %d", regionSum, totalSpan)
		}
		for i := range regionLens {
			regionEnd := offsets[i] + regionLens[i]
			if i < len(regionLens)-1 && regionEnd != offsets[i+1] {
				t.Fatalf("region %d end %d != next start %d", i, regionEnd, offsets[i+1])
			}
		}

		if err := validateLayoutEnd(meta); err != nil {
			t.Fatalf("validateLayoutEnd: %v", err)
		}
		if err := validateMappedBufferLengths(meta, regions); err != nil {
			t.Fatalf("validateMappedBufferLengths: %v", err)
		}
	})
}

func TestParseMetadata_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawRapidBufferLengths(t, TestRapidMaxFieldWide)
		buf := make([]byte, MetaDataLength)
		WriteMetadataFieldsToBuffer(buf, meta)

		got, err := ParseMetadata(buf)
		if err != nil {
			t.Fatalf("ParseMetadata: %v", err)
		}
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
		if err != nil {
			t.Fatalf("ComputeCncFileLength: %v", err)
		}
		if got%align != 0 {
			t.Fatalf("ComputeCncFileLength(%d,%d)=%d not aligned", total, align, got)
		}
		if got < computedLength {
			t.Fatalf("ComputeCncFileLength(%d,%d)=%d < need %d", total, align, got, computedLength)
		}
		if got-computedLength >= align {
			t.Fatalf("ComputeCncFileLength(%d,%d)=%d overshot", total, align, got)
		}
		if meta.IsFileLengthSufficient(computedLength - 1) {
			t.Fatalf(
				"file length %d should be insufficient for computed %d",
				computedLength-1,
				computedLength,
			)
		}
		if !meta.IsFileLengthSufficient(computedLength) {
			t.Fatalf(
				"file length %d should be sufficient for computed %d",
				computedLength,
				computedLength,
			)
		}
	})
}

func TestComputedLength_hugeMetadata_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		meta := DrawOverflowBufferLengths(t)

		header := make([]byte, MetaDataLength)
		WriteMetadataFieldsToBuffer(header, meta)
		if _, err := ParseMetadata(header); err != nil {
			t.Fatalf("ParseMetadata: %v", err)
		}

		computedLength := meta.ComputedLength()
		if err := validateLayoutEnd(meta); err != nil {
			t.Fatalf("validateLayoutEnd: %v", err)
		}
		if meta.IsFileLengthSufficient(TestShortCncFileLength) {
			t.Fatalf(
				"file length %d should be insufficient for computed %d",
				TestShortCncFileLength,
				computedLength,
			)
		}
	})
}
