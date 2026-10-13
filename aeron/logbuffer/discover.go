// Layout discovery scans candidate term lengths at MetaDataOffset rather than
// reading the file tail. Page-aligned driver files place padding after the
// metadata region, so a tail peek can read zero padding and mis-parse dimensions.
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

type dimensionReader func(candidate int) (termLength, pageSize int32, ok bool, err error)

// Returns the first ascending power-of-two candidate that validates.
//
// Assumes driver-created Aeron-shaped files; corrupt blobs may false-match at a
// smaller MetaDataOffset.
func discoverDimensions(span int, read dimensionReader) (layoutDimensions, error) {
	maxShift := PositionBitsToShift(TermMaxLength)
	for shift := PositionBitsToShift(TermMinLength); shift <= maxShift; shift++ {
		candidate := 1 << shift
		metaStart := MetaDataOffset(candidate)
		minSpan := metaStart + MetaDataLength
		if span < minSpan {
			continue
		}
		termLength, pageSize, ok, err := read(candidate)
		if err != nil {
			return layoutDimensions{}, err
		}
		if !ok {
			continue
		}
		if int(termLength) != candidate {
			continue
		}
		computed, err := ComputeLogLength(int(termLength), int(pageSize))
		if err != nil {
			continue // unreachable after validateDimensions and TermMaxLength cap
		}
		if computed > span {
			continue
		}
		return layoutDimensions{
			termLength: termLength,
			pageSize:   pageSize,
			computed:   computed,
		}, nil
	}
	return layoutDimensions{}, fmt.Errorf("logbuffer: could not discover log layout in span %d", span)
}

func discoverDimensionsFromBuffer(data []byte) (layoutDimensions, error) {
	meta := atomicbuffer.Wrap(data)
	return discoverDimensions(len(data), func(candidate int) (int32, int32, bool, error) {
		metaStart := MetaDataOffset(candidate)
		termLength := meta.GetInt32(metaStart + OffsetTermLength)
		pageSize := meta.GetInt32(metaStart + OffsetPageSize)
		if validateDimensions(termLength, pageSize) != nil {
			return 0, 0, false, nil
		}
		return termLength, pageSize, true, nil
	})
}

func discoverDimensionsFromFile(
	file *os.File,
	fileSize int,
) (layoutDimensions, error) {
	var dimsBuf [8]byte
	return discoverDimensions(fileSize, func(candidate int) (int32, int32, bool, error) {
		metaStart := MetaDataOffset(candidate)
		if _, err := file.ReadAt(dimsBuf[:], int64(metaStart+OffsetTermLength)); err != nil {
			return 0, 0, false, fmt.Errorf("logbuffer: read dimensions: %w", err)
		}
		dims := atomicbuffer.Wrap(dimsBuf[:])
		termLength := dims.GetInt32(0)
		pageSize := dims.GetInt32(atomicbuffer.SizeInt32)
		if validateDimensions(termLength, pageSize) != nil {
			return 0, 0, false, nil
		}
		return termLength, pageSize, true, nil
	})
}

// validateMetadataDimensions checks discovery output against metadata parsed from the
// mapped buffer. Discovery and parse are separate steps; drift catches edited or raced files.
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
