package cnc

import (
	"fmt"
	"os"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/mmap"
)

type MappedCncFile struct {
	mapping  *mmap.Mapping
	metadata Metadata
	data     []byte
}

func MapCncFile(path string) (*MappedCncFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("cnc: open %s: %w", path, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cnc: fstat %s: %w", path, err)
	}
	fileSize := int(fileInfo.Size())

	headerMeta, err := readMetadataHeaderFromFile(file, path, fileSize, nil)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return mapCncFileAfterHeader(file, path, headerMeta)
}

func mapCncFileAfterHeader(
	file *os.File,
	path string,
	headerMeta Metadata,
) (*MappedCncFile, error) {
	computedLength := headerMeta.ComputedLength()
	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cnc: fstat %s: %w", path, err)
	}
	fileSize := int(fileInfo.Size())
	if err := errIfFileLengthInsufficient(headerMeta, fileSize); err != nil {
		_ = file.Close()
		return nil, err
	}

	mapping, err := mmap.MapOpenedFile(file, computedLength)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	data := mapping.Data
	meta, err := ParseMetadata(data[:MetaDataLength])
	if err != nil {
		_ = mapping.Close()
		return nil, err
	}

	if err := validateMetadataLayoutConsistency(headerMeta, meta); err != nil {
		_ = mapping.Close()
		return nil, err
	}

	return buildMappedCncFile(mapping, meta), nil
}

func bufferLengthDescriptors(meta Metadata) []bufferLengthDesc {
	return []bufferLengthDesc{
		{"to-driver", meta.ToDriverBufferLength},
		{"to-clients", meta.ToClientsBufferLength},
		{"counters metadata", meta.CountersMetaDataBufferLength},
		{"counters values", meta.CountersValuesBufferLength},
		{"error log", meta.ErrorLogBufferLength},
	}
}

type bufferLengthDesc struct {
	name   string
	length int32
}

func validateMetadataLayoutConsistency(headerMeta, liveMeta Metadata) error {
	headerDescs := bufferLengthDescriptors(headerMeta)
	liveDescs := bufferLengthDescriptors(liveMeta)
	for i := range headerDescs {
		if headerDescs[i].length != liveDescs[i].length {
			return fmt.Errorf(
				"cnc: metadata layout drift: header %s length %d != live %d",
				headerDescs[i].name,
				headerDescs[i].length,
				liveDescs[i].length,
			)
		}
	}
	return nil
}

func buildMappedCncFile(mapping *mmap.Mapping, meta Metadata) *MappedCncFile {
	return &MappedCncFile{
		mapping:  mapping,
		metadata: meta,
		data:     mapping.Data,
	}
}

type cncRegions struct {
	toDriver         []byte
	toClients        []byte
	countersMetaData []byte
	countersValues   []byte
	errorLog         []byte
}

const (
	cncRegionToDriver = iota
	cncRegionToClients
	cncRegionCountersMetaData
	cncRegionCountersValues
	cncRegionErrorLog
)

func cncRegionBounds(meta Metadata, region int) (start, end int) {
	switch region {
	case cncRegionToDriver:
		return ToDriverOffset(), ToClientsOffset(meta)
	case cncRegionToClients:
		return ToClientsOffset(meta), CountersMetaDataOffset(meta)
	case cncRegionCountersMetaData:
		return CountersMetaDataOffset(meta), CountersValuesOffset(meta)
	case cncRegionCountersValues:
		return CountersValuesOffset(meta), ErrorLogOffset(meta)
	case cncRegionErrorLog:
		return ErrorLogOffset(meta), meta.ComputedLength()
	default:
		return 0, 0
	}
}

func sliceCncRegion(data []byte, meta Metadata, region int) []byte {
	start, end := cncRegionBounds(meta, region)
	return data[start:end]
}

func sliceCncRegions(data []byte, meta Metadata) cncRegions {
	return cncRegions{
		toDriver:         sliceCncRegion(data, meta, cncRegionToDriver),
		toClients:        sliceCncRegion(data, meta, cncRegionToClients),
		countersMetaData: sliceCncRegion(data, meta, cncRegionCountersMetaData),
		countersValues:   sliceCncRegion(data, meta, cncRegionCountersValues),
		errorLog:         sliceCncRegion(data, meta, cncRegionErrorLog),
	}
}

func validateMappedBufferLengths(meta Metadata, regions cncRegions) error {
	got := []int{
		len(regions.toDriver),
		len(regions.toClients),
		len(regions.countersMetaData),
		len(regions.countersValues),
		len(regions.errorLog),
	}
	for i, desc := range bufferLengthDescriptors(meta) {
		if got[i] != int(desc.length) {
			return fmt.Errorf(
				"cnc: %s buffer length %d != metadata %d",
				desc.name,
				got[i],
				desc.length,
			)
		}
	}
	return nil
}

func (f *MappedCncFile) mappedData() ([]byte, Metadata) {
	if f == nil || f.data == nil {
		return nil, Metadata{}
	}
	return f.data, f.metadata
}

func (f *MappedCncFile) Close() error {
	if f == nil || f.mapping == nil {
		return nil
	}
	err := f.mapping.Close()
	f.mapping = nil
	f.metadata = Metadata{}
	f.data = nil
	return err
}

func (f *MappedCncFile) Metadata() Metadata {
	if f == nil {
		return Metadata{}
	}
	return f.metadata
}

func (f *MappedCncFile) Bytes() []byte {
	data, _ := f.mappedData()
	return data
}

func (f *MappedCncFile) ToDriverBuffer() []byte {
	return f.sliceRegion(cncRegionToDriver)
}

func (f *MappedCncFile) ToClientsBuffer() []byte {
	return f.sliceRegion(cncRegionToClients)
}

func (f *MappedCncFile) CountersMetaDataBuffer() []byte {
	return f.sliceRegion(cncRegionCountersMetaData)
}

func (f *MappedCncFile) CountersValuesBuffer() []byte {
	return f.sliceRegion(cncRegionCountersValues)
}

func (f *MappedCncFile) ErrorLogBuffer() []byte {
	return f.sliceRegion(cncRegionErrorLog)
}

func (f *MappedCncFile) sliceRegion(region int) []byte {
	data, meta := f.mappedData()
	if data == nil {
		return nil
	}
	return sliceCncRegion(data, meta, region)
}

func (f *MappedCncFile) ToDriverAtomic() atomicbuffer.AtomicBuffer {
	return atomicbuffer.Wrap(f.ToDriverBuffer())
}

func (f *MappedCncFile) ToClientsAtomic() atomicbuffer.AtomicBuffer {
	return atomicbuffer.Wrap(f.ToClientsBuffer())
}
