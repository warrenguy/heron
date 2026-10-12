package logbuffer

import (
	"fmt"
	"math"
	"os"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/mmap"
)

// MappedLogBuffer owns mmap until Close.
//
// Term and metadata AtomicBuffer views returned before Close must not be used after Close;
// fresh accessors return error once closed.
type MappedLogBuffer struct {
	mapping    *mmap.Mapping
	path       string
	data       []byte
	termLength int
	pageSize   int
	computed   int
	metadata   MetadataView
	closed     bool
	termViews  [PartitionCount]atomicbuffer.AtomicBuffer
	metaView   atomicbuffer.AtomicBuffer
}

func errSpanTooShort(label string, got, need int) error {
	return fmt.Errorf("logbuffer: %s too short: %d < %d", label, got, need)
}

func fileSizeFromStat(size int64) (int, error) {
	if size < 0 || size > int64(math.MaxInt) {
		return 0, fmt.Errorf("logbuffer: file size overflow: %d", size)
	}
	return int(size), nil
}

// MapLogBufferFromBytes copies data into an owned heap buffer and maps it as a log.
//
// Intended for in-memory fixtures and tests; production IPC uses MapLogFile.
func MapLogBufferFromBytes(data []byte) (*MappedLogBuffer, error) {
	return mapLogBuffer(data)
}

func MapLogFile(path string) (*MappedLogBuffer, error) {
	// The driver supplies the log file name over the CnC.
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("logbuffer: open %s: %w", path, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("logbuffer: fstat %s: %w", path, err)
	}
	fileSize, err := fileSizeFromStat(fileInfo.Size())
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if fileSize < MetaDataLength {
		_ = file.Close()
		return nil, errSpanTooShort("file", fileSize, MetaDataLength)
	}

	dims, err := discoverDimensionsFromFile(file, fileSize)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return mapLogFileAfterDiscover(file, path, dims, 0)
}

// preValidatedFileSize 0 forces Stat before mmap (TOCTOU-safe).
//
// Non-zero skips restat and is only valid when size was validated immediately before on this
// file descriptor; production call sites pass 0.
func mapLogFileAfterDiscover(
	file *os.File,
	path string,
	dims layoutDimensions,
	preValidatedFileSize int,
) (*MappedLogBuffer, error) {
	fileSize := preValidatedFileSize
	if fileSize == 0 {
		fileInfo, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("logbuffer: fstat %s: %w", path, err)
		}
		var statErr error
		fileSize, statErr = fileSizeFromStat(fileInfo.Size())
		if statErr != nil {
			_ = file.Close()
			return nil, statErr
		}
	}
	if fileSize < dims.computed {
		_ = file.Close()
		return nil, errSpanTooShort("file", fileSize, dims.computed)
	}

	mapping, err := mmap.MapOpenedFile(file, dims.computed)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	mapped, err := buildMappedLogBuffer(mapping, path, dims, nil)
	if err != nil {
		_ = mapping.Close()
		return nil, err
	}
	mmap.PreTouch(mapping.Data, int(dims.pageSize))
	return mapped, nil
}

// Skips PreTouch: heap copy, not mmap - no page warmup benefit.
func mapLogBuffer(data []byte) (*MappedLogBuffer, error) {
	if len(data) < MetaDataLength {
		return nil, errSpanTooShort("buffer", len(data), MetaDataLength)
	}

	dims, err := discoverDimensionsFromBuffer(data)
	if err != nil {
		return nil, err
	}
	if len(data) < dims.computed {
		return nil, errSpanTooShort("buffer", len(data), dims.computed)
	}

	// Copy so MappedLogBuffer owns backing past return; caller slice may be temporary.
	owned := append([]byte(nil), data[:dims.computed]...)
	return buildMappedLogBuffer(nil, "", dims, owned)
}

func buildMappedLogBuffer(
	mapping *mmap.Mapping,
	path string,
	dims layoutDimensions,
	backing []byte,
) (*MappedLogBuffer, error) {
	if backing == nil {
		if mapping == nil {
			return nil, fmt.Errorf("logbuffer: no backing data")
		}
		backing = mapping.Data
	}

	termLength := int(dims.termLength)
	terms, meta, err := sliceLogRegions(backing, termLength)
	if err != nil {
		return nil, err
	}

	metaBuffer := atomicbuffer.Wrap(meta)
	metadata, err := ParseMetadataView(metaBuffer)
	if err != nil {
		return nil, err
	}
	if err := validateMetadataDimensions(
		dims.termLength,
		dims.pageSize,
		metadata.TermLength,
		metadata.PageSize,
	); err != nil {
		return nil, err
	}

	mapped := &MappedLogBuffer{
		mapping:    mapping,
		path:       path,
		data:       backing,
		termLength: termLength,
		pageSize:   int(dims.pageSize),
		computed:   dims.computed,
		metadata:   metadata,
		metaView:   metaBuffer,
	}
	for i := range PartitionCount {
		mapped.termViews[i] = atomicbuffer.Wrap(terms[i])
	}
	return mapped, nil
}

func sliceLogRegions(
	data []byte,
	termLength int,
) (terms [PartitionCount][]byte, meta []byte, err error) {
	metaStart := MetaDataOffset(termLength)
	need := metaStart + MetaDataLength
	if len(data) < need {
		return terms, nil, errSpanTooShort("buffer", len(data), need)
	}
	for i := range PartitionCount {
		off := SectionTermOffset(i, termLength)
		terms[i] = data[off : off+termLength]
	}
	meta = data[metaStart : metaStart+MetaDataLength]
	return terms, meta, nil
}

func (mapped *MappedLogBuffer) Close() error {
	if mapped == nil || mapped.closed {
		return nil
	}
	mapped.closed = true
	var err error
	if mapped.mapping != nil {
		err = mapped.mapping.Close()
		mapped.mapping = nil
	}
	mapped.data = nil
	mapped.metadata = MetadataView{}
	mapped.metaView = atomicbuffer.AtomicBuffer{}
	mapped.termViews = [PartitionCount]atomicbuffer.AtomicBuffer{}
	return err
}

func (mapped *MappedLogBuffer) Path() string {
	if mapped == nil {
		return ""
	}
	return mapped.path
}

func (mapped *MappedLogBuffer) TermLength() int {
	if mapped == nil {
		return 0
	}
	return mapped.termLength
}

func (mapped *MappedLogBuffer) PageSize() int {
	if mapped == nil {
		return 0
	}
	return mapped.pageSize
}

func (mapped *MappedLogBuffer) ComputedLength() int {
	if mapped == nil {
		return 0
	}
	return mapped.computed
}

// Metadata returns the snapshot parsed at map time.
//
// After Close the view is zeroed; hot paths should re-read volatile fields from
// MetaDataBuffer instead.
func (mapped *MappedLogBuffer) Metadata() MetadataView {
	if mapped == nil || mapped.closed {
		return MetadataView{}
	}
	return mapped.metadata
}

func (mapped *MappedLogBuffer) closedErr() error {
	if mapped == nil || mapped.closed {
		return fmt.Errorf("logbuffer: closed mapped log buffer")
	}
	return nil
}

// Returned AtomicBuffer is a by-value slice header.
//
// Retaining it across Close is use-after-unmap on the MapLogFile path;
// heap-backed MapLogBuffer differs.
func (mapped *MappedLogBuffer) TermBuffer(index int) (atomicbuffer.AtomicBuffer, error) {
	if err := mapped.closedErr(); err != nil {
		return atomicbuffer.AtomicBuffer{}, err
	}
	if err := ValidateTermIndex(index, "term index"); err != nil {
		return atomicbuffer.AtomicBuffer{}, err
	}
	return mapped.termViews[index], nil
}

// Returned AtomicBuffer views are by-value slice headers.
//
// Retaining them across Close is use-after-unmap on the MapLogFile path;
// heap-backed MapLogBuffer differs.
func (mapped *MappedLogBuffer) TermBuffers() ([PartitionCount]atomicbuffer.AtomicBuffer, error) {
	if err := mapped.closedErr(); err != nil {
		return [PartitionCount]atomicbuffer.AtomicBuffer{}, err
	}
	return mapped.termViews, nil
}

// Returned AtomicBuffer is a by-value slice header.
//
// Retaining it across Close is use-after-unmap on the MapLogFile path;
// heap-backed MapLogBuffer differs.
func (mapped *MappedLogBuffer) MetaDataBuffer() (atomicbuffer.AtomicBuffer, error) {
	if err := mapped.closedErr(); err != nil {
		return atomicbuffer.AtomicBuffer{}, err
	}
	return mapped.metaView, nil
}

// Bytes returns the mapped backing slice.
//
// When the on-disk file is page-padded beyond ComputedLength(), len(Bytes()) equals
// the mapped length; use ComputedLength() for logical log layout bounds.
func (mapped *MappedLogBuffer) Bytes() []byte {
	if mapped == nil || mapped.closed {
		return nil
	}
	return mapped.data
}
