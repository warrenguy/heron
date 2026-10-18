package logbuffer

import (
	"fmt"
	"os"

	"github.com/warrenguy/heron/atomicbuffer"
)

type layoutDimensions struct {
	termLength int32
	pageSize   int32
	computed   int
}

// layoutFromMetadata validates termLength and pageSize for a log of logLength
// bytes.
//
// Returns an error when:
//   - either value is out of range or not a power of two
//   - logLength differs from ComputeLogLength of the two values
func layoutFromMetadata(logLength int, termLength, pageSize int32) (layoutDimensions, error) {
	computed, err := ComputeLogLength(int(termLength), int(pageSize))
	if err != nil {
		return layoutDimensions{}, err
	}
	if logLength != computed {
		return layoutDimensions{}, fmt.Errorf(
			"logbuffer: log length %d != computed length %d (term length %d, page size %d)",
			logLength,
			computed,
			termLength,
			pageSize,
		)
	}
	return layoutDimensions{
		termLength: termLength,
		pageSize:   pageSize,
		computed:   computed,
	}, nil
}

// readLayoutFromBuffer reads the layout from the metadata at the tail of data.
//
// data must hold at least MetaDataLength bytes.
func readLayoutFromBuffer(data []byte) (layoutDimensions, error) {
	log := atomicbuffer.Wrap(data)
	metaStart := LogMetaDataOffset(len(data))
	termLength := log.GetInt32(metaStart + OffsetTermLength)
	pageSize := log.GetInt32(metaStart + OffsetPageSize)
	return layoutFromMetadata(len(data), termLength, pageSize)
}

// readLayoutFromFile reads the layout from the metadata at the tail of a file
// of fileSize bytes.
//
// fileSize must be at least MetaDataLength.
func readLayoutFromFile(file *os.File, fileSize int) (layoutDimensions, error) {
	var raw [OffsetPageSize + atomicbuffer.SizeInt32 - OffsetTermLength]byte
	offset := int64(LogMetaDataOffset(fileSize) + OffsetTermLength)
	if _, err := file.ReadAt(raw[:], offset); err != nil {
		return layoutDimensions{}, fmt.Errorf("logbuffer: read dimensions: %w", err)
	}
	fields := atomicbuffer.Wrap(raw[:])
	termLength := fields.GetInt32(0)
	pageSize := fields.GetInt32(OffsetPageSize - OffsetTermLength)
	return layoutFromMetadata(fileSize, termLength, pageSize)
}

// validateMetadataDimensions checks the layout read first against the
// metadata parsed from the mapped log. A mismatch means the log changed
// between the two reads.
func validateMetadataDimensions(headerTerm, headerPage, liveTerm, livePage int32) error {
	if headerTerm != liveTerm || headerPage != livePage {
		return fmt.Errorf(
			"logbuffer: metadata layout drift: header term %d page %d != live term %d page %d",
			headerTerm,
			headerPage,
			liveTerm,
			livePage,
		)
	}
	return nil
}
