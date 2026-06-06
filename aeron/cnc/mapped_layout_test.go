package cnc

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func validateMappableLayout(meta Metadata, dataLen int) error {
	if err := validateLayoutEnd(meta); err != nil {
		return err
	}
	computedLength := meta.ComputedLength()
	if dataLen < MetaDataLength {
		return fmt.Errorf(
			"cnc: mapped length %d insufficient for metadata header (need %d)",
			dataLen,
			MetaDataLength,
		)
	}
	if dataLen < computedLength {
		return fmt.Errorf(
			"cnc: mapped length %d insufficient for metadata (need %d)",
			dataLen,
			computedLength,
		)
	}
	return nil
}

func writeReadyCncFileAt(t *testing.T, path string) Metadata {
	t.Helper()
	meta := ReadyMetadata()
	buf := make([]byte, meta.ComputedLength())
	WriteMetadataFieldsToBuffer(buf, meta)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
	return meta
}

func TestValidateMappedBufferLengths(t *testing.T) {
	meta := ReadyMetadata()
	data := make([]byte, meta.ComputedLength())
	regions := sliceCncRegions(data, meta)

	cases := []struct {
		name      string
		region    int
		wantError string
	}{
		{"to-driver length mismatch", cncRegionToDriver, "cnc: to-driver buffer length"},
		{"to-clients length mismatch", cncRegionToClients, "cnc: to-clients buffer length"},
		{
			name:      "counters metadata length mismatch",
			region:    cncRegionCountersMetaData,
			wantError: "cnc: counters metadata buffer length",
		},
		{
			name:      "counters values length mismatch",
			region:    cncRegionCountersValues,
			wantError: "cnc: counters values buffer length",
		},
		{"error log length mismatch", cncRegionErrorLog, "cnc: error log buffer length"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			mutated := regions
			switch test.region {
			case cncRegionToDriver:
				mutated.toDriver = mutated.toDriver[:len(mutated.toDriver)-1]
			case cncRegionToClients:
				mutated.toClients = mutated.toClients[:len(mutated.toClients)-1]
			case cncRegionCountersMetaData:
				mutated.countersMetaData = mutated.countersMetaData[:len(mutated.countersMetaData)-1]
			case cncRegionCountersValues:
				mutated.countersValues = mutated.countersValues[:len(mutated.countersValues)-1]
			case cncRegionErrorLog:
				mutated.errorLog = mutated.errorLog[:len(mutated.errorLog)-1]
			}

			err := validateMappedBufferLengths(meta, mutated)
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestValidateMappableLayout_insufficientHeaderSpan(t *testing.T) {
	meta := ReadyMetadata()
	err := validateMappableLayout(meta, MetaDataLength-1)
	require.ErrorContains(t, err, "insufficient for metadata header")
}

func TestValidateMappableLayout_hugeMetadata(t *testing.T) {
	meta := WrappedPositiveOverflowMetadata()
	computedLength := meta.ComputedLength()
	require.NoError(t, validateLayoutEnd(meta))
	require.NoError(t, validateMappableLayout(meta, computedLength))

	err := validateMappableLayout(meta, TestWrappedOverflowOnDiskLength)
	require.ErrorContains(t, err, "insufficient for metadata")
}

func TestValidateLayoutEnd_hugeMetadata(t *testing.T) {
	fixtures := []struct {
		name string
		meta Metadata
	}{
		{"overflow computed length", OverflowComputedLengthMetadata()},
		{"wrapped positive overflow", WrappedPositiveOverflowMetadata()},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			require.NoError(t, validateLayoutEnd(fixture.meta))
		})
	}
}

func TestValidateMetadataLayoutConsistency(t *testing.T) {
	headerMeta := ReadyMetadata()
	liveMeta := headerMeta
	liveMeta.ToDriverBufferLength = headerMeta.ToClientsBufferLength
	liveMeta.ToClientsBufferLength = headerMeta.ToDriverBufferLength

	err := validateMetadataLayoutConsistency(headerMeta, liveMeta)
	require.ErrorContains(t, err, "metadata layout drift")
	require.ErrorContains(t, err, "to-driver")
}
