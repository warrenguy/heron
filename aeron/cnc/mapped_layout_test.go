package cnc

import (
	"os"
	"strings"
	"testing"
)

func writeReadyCncFileAt(t *testing.T, path string) Metadata {
	t.Helper()
	meta := ReadyMetadata()
	buf := make([]byte, meta.ComputedLength())
	WriteMetadataFieldsToBuffer(buf, meta)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
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
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestValidateMappableLayout_insufficientHeaderSpan(t *testing.T) {
	meta := ReadyMetadata()
	err := validateMappableLayout(meta, MetaDataLength-1)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata header") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateMappableLayout_hugeMetadata(t *testing.T) {
	meta := WrappedPositiveOverflowMetadata()
	computedLength := meta.ComputedLength()
	if err := validateLayoutEnd(meta); err != nil {
		t.Fatalf("validateLayoutEnd: %v", err)
	}
	if err := validateMappableLayout(meta, computedLength); err != nil {
		t.Fatalf("full mapped length: %v", err)
	}

	err := validateMappableLayout(meta, TestWrappedOverflowOnDiskLength)
	if err == nil {
		t.Fatal("expected insufficient mapped length error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
	}
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
			if err := validateLayoutEnd(fixture.meta); err != nil {
				t.Fatalf("validateLayoutEnd: %v", err)
			}
		})
	}
}

func TestValidateMetadataLayoutConsistency(t *testing.T) {
	headerMeta := ReadyMetadata()
	liveMeta := headerMeta
	liveMeta.ToDriverBufferLength = headerMeta.ToClientsBufferLength
	liveMeta.ToClientsBufferLength = headerMeta.ToDriverBufferLength

	err := validateMetadataLayoutConsistency(headerMeta, liveMeta)
	if err == nil {
		t.Fatal("expected layout drift error")
	}
	if !strings.Contains(err.Error(), "metadata layout drift") {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "to-driver") {
		t.Fatalf("got %v", err)
	}
}
