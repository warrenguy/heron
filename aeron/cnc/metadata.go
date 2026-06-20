package cnc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	FileName = "cnc.dat"

	MetaDataLength = atomic.CacheLineLength * 2

	cncPollInterval = 50 * time.Millisecond

	OffsetCncVersion                   = 0
	OffsetToDriverBufferLength         = 4
	OffsetToClientsBufferLength        = 8
	OffsetCountersMetaDataBufferLength = 12
	OffsetCountersValuesBufferLength   = 16
	OffsetErrorLogBufferLength         = 20
	OffsetClientLivenessTimeout        = 24
	OffsetStartTimestamp               = 32
	OffsetPID                          = 40
	OffsetFilePageSize                 = 48
)

var (
	Version = ComposeSemanticVersion(0, 2, 0)

	ErrNotReady = errors.New("cnc: not ready")
)

type Metadata struct {
	CncVersion                   int32
	ToDriverBufferLength         int32
	ToClientsBufferLength        int32
	CountersMetaDataBufferLength int32
	CountersValuesBufferLength   int32
	ErrorLogBufferLength         int32
	ClientLivenessTimeoutNs      int64
	StartTimestampMs             int64
	PID                          int64
	FilePageSize                 int32
}

func ComposeSemanticVersion(major, minor, patch int) int32 {
	return int32((major << 16) | (minor << 8) | patch)
}

func MajorVersion(version int32) int {
	return int(version >> 16)
}

// ParseMetadata reads CNC metadata after the driver has published CncVersion (volatile,
// non-zero) with major version compatible with Version.
func ParseMetadata(buf []byte) (Metadata, error) {
	if len(buf) < MetaDataLength {
		return Metadata{}, fmt.Errorf("cnc: buffer too short: %d < %d", len(buf), MetaDataLength)
	}
	atomicBuffer := atomic.Wrap(buf)
	version := atomicBuffer.GetInt32Volatile(OffsetCncVersion)
	if version == 0 {
		return Metadata{}, ErrNotReady
	}
	if err := validateMajorVersion(version); err != nil {
		return Metadata{}, err
	}
	return parseMetadata(atomicBuffer, version)
}

func validateMajorVersion(version int32) error {
	if MajorVersion(version) != MajorVersion(Version) {
		return fmt.Errorf("cnc: incompatible major version: file=%d heron=%d", version, Version)
	}
	return nil
}

func parseMetadata(atomicBuffer atomic.AtomicBuffer, version int32) (Metadata, error) {
	meta := Metadata{
		CncVersion:                   version,
		ToDriverBufferLength:         atomicBuffer.GetInt32(OffsetToDriverBufferLength),
		ToClientsBufferLength:        atomicBuffer.GetInt32(OffsetToClientsBufferLength),
		CountersMetaDataBufferLength: atomicBuffer.GetInt32(OffsetCountersMetaDataBufferLength),
		CountersValuesBufferLength:   atomicBuffer.GetInt32(OffsetCountersValuesBufferLength),
		ErrorLogBufferLength:         atomicBuffer.GetInt32(OffsetErrorLogBufferLength),
		ClientLivenessTimeoutNs:      atomicBuffer.GetInt64(OffsetClientLivenessTimeout),
		StartTimestampMs:             atomicBuffer.GetInt64(OffsetStartTimestamp),
		PID:                          atomicBuffer.GetInt64(OffsetPID),
		FilePageSize:                 atomicBuffer.GetInt32(OffsetFilePageSize),
	}
	if err := meta.validate(); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func (m Metadata) validate() error {
	if m.ToDriverBufferLength <= 0 {
		return fmt.Errorf(
			"cnc: to-driver buffer length must be positive: %d",
			m.ToDriverBufferLength,
		)
	}
	if m.ToClientsBufferLength <= 0 {
		return fmt.Errorf(
			"cnc: to-clients buffer length must be positive: %d",
			m.ToClientsBufferLength,
		)
	}
	if m.CountersMetaDataBufferLength < 0 {
		return fmt.Errorf("cnc: negative buffer length %d", m.CountersMetaDataBufferLength)
	}
	if m.CountersValuesBufferLength < 0 {
		return fmt.Errorf("cnc: negative buffer length %d", m.CountersValuesBufferLength)
	}
	if m.ErrorLogBufferLength < 0 {
		return fmt.Errorf("cnc: negative buffer length %d", m.ErrorLogBufferLength)
	}
	if _, err := m.computedLengthInt64(); err != nil {
		return err
	}
	return nil
}

// Driver writes other header fields before publishing version last.
func CncVersionVolatile(buf []byte) int32 {
	if len(buf) < OffsetCncVersion+4 {
		return 0
	}
	return atomic.Wrap(buf).GetInt32Volatile(OffsetCncVersion)
}

// WaitCncReady polls until cnc.dat exists, version is published with a compatible major
// version, and file length matches metadata.
func WaitCncReady(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	metaBuf := make([]byte, MetaDataLength)
	for {
		if time.Now().After(deadline) {
			return errors.New("cnc: timeout waiting for ready")
		}
		ready, err := tryCncReady(path, metaBuf)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		time.Sleep(cncPollInterval)
	}
}

func isRetryableFileErr(err error) bool {
	return os.IsNotExist(err) || os.IsPermission(err)
}

func isShortMetadataRead(n int, err error) bool {
	return n < MetaDataLength && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF))
}

// ReadMetadataFile parses the CNC metadata header from a driver-ready cnc.dat and
// checks that the on-disk file length matches the header buffer layout.
func ReadMetadataFile(path string) (Metadata, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("cnc: stat %s: %w", path, err)
	}
	fileSize := int(info.Size())
	if fileSize <= MetaDataLength {
		return Metadata{}, fmt.Errorf("cnc: file too short: %d <= %d", fileSize, MetaDataLength)
	}
	metaBuf := make([]byte, MetaDataLength)
	return readMetadataHeader(path, metaBuf, fileSize)
}

func readMetadataHeaderFromFile(
	file io.Reader,
	path string,
	fileSize int,
	metaBuf []byte,
) (Metadata, error) {
	if len(metaBuf) < MetaDataLength {
		metaBuf = make([]byte, MetaDataLength)
	}
	n, err := io.ReadFull(file, metaBuf)
	if err != nil {
		if isShortMetadataRead(n, err) {
			return Metadata{}, ErrNotReady
		}
		return Metadata{}, fmt.Errorf("cnc: read %s: %w", path, err)
	}

	meta, err := ParseMetadata(metaBuf)
	if err != nil {
		return Metadata{}, err
	}

	if err := errIfFileLengthInsufficient(meta, fileSize); err != nil {
		return Metadata{}, err
	}
	if err := validateLayoutEnd(meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func errIfFileLengthInsufficient(meta Metadata, fileSize int) error {
	need := meta.ComputedLength()
	if need < 0 {
		return fmt.Errorf("cnc: computed length overflow")
	}
	if fileSize < need {
		return fmt.Errorf(
			"cnc: file length %d insufficient for metadata (need %d)",
			fileSize,
			need,
		)
	}
	return nil
}

func validateLayoutEnd(meta Metadata) error {
	layoutEnd := ErrorLogOffset(meta) + int(meta.ErrorLogBufferLength)
	computedLength := meta.ComputedLength()
	if layoutEnd != computedLength {
		return fmt.Errorf(
			"cnc: buffer layout end %d != computed length %d",
			layoutEnd,
			computedLength,
		)
	}
	return nil
}

func readMetadataHeader(path string, metaBuf []byte, fileSize int) (Metadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if isRetryableFileErr(err) {
			return Metadata{}, ErrNotReady
		}
		return Metadata{}, fmt.Errorf("cnc: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	if fileSize < 0 {
		fileInfo, err := file.Stat()
		if err != nil {
			return Metadata{}, fmt.Errorf("cnc: fstat %s: %w", path, err)
		}
		fileSize = int(fileInfo.Size())
	}
	return readMetadataHeaderFromFile(file, path, fileSize, metaBuf)
}

func tryCncReady(path string, metaBuf []byte) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("cnc: stat %s: %w", path, err)
	}
	if info.Size() <= int64(MetaDataLength) {
		return false, nil
	}

	_, err = readMetadataHeader(path, metaBuf, int(info.Size()))
	if err != nil {
		if errors.Is(err, ErrNotReady) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func maxIntAsInt64() int64 {
	return int64(^uint(0) >> 1)
}

func (m Metadata) computedLengthInt64() (int64, error) {
	sum := int64(m.ToDriverBufferLength) +
		int64(m.ToClientsBufferLength) +
		int64(m.CountersMetaDataBufferLength) +
		int64(m.CountersValuesBufferLength) +
		int64(m.ErrorLogBufferLength)
	total := int64(MetaDataLength) + sum
	if total < int64(MetaDataLength) {
		return 0, errors.New("cnc: computed length overflow")
	}
	if total > maxIntAsInt64() {
		return 0, errors.New("cnc: computed length overflow")
	}
	return total, nil
}

func (m Metadata) ComputedLength() int {
	total, err := m.computedLengthInt64()
	if err != nil {
		return -1
	}
	return int(total)
}

func (m Metadata) IsFileLengthSufficient(fileLength int) bool {
	if err := m.validate(); err != nil {
		return false
	}
	need := m.ComputedLength()
	if need < 0 {
		return false
	}
	return fileLength >= need
}

func ToDriverOffset() int { return MetaDataLength }

func ToClientsOffset(m Metadata) int {
	return ToDriverOffset() + int(m.ToDriverBufferLength)
}

func CountersMetaDataOffset(m Metadata) int {
	return ToClientsOffset(m) + int(m.ToClientsBufferLength)
}

func CountersValuesOffset(m Metadata) int {
	return CountersMetaDataOffset(m) + int(m.CountersMetaDataBufferLength)
}

func ErrorLogOffset(m Metadata) int {
	return CountersValuesOffset(m) + int(m.CountersValuesBufferLength)
}

func ComputeCncFileLength(totalBufferLengths, alignment int) (int, error) {
	if alignment <= 0 || !atomic.IsPowerOfTwo(alignment) {
		return 0, fmt.Errorf("cnc: alignment must be a positive power of two: %d", alignment)
	}
	return atomic.Align(MetaDataLength+totalBufferLengths, alignment), nil
}
