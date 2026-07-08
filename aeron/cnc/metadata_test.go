package cnc_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestParseMetadataSmoke(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength+1024)
	writeReadyMetadata(t, buf)

	meta, err := cnc.ParseMetadata(buf)
	require.NoError(t, err)
	cnc.AssertParsedReadyMetadataEqual(t, meta, cnc.ReadyMetadata())

	wantLen := testReadyFileLength()
	require.Equal(t, wantLen, meta.ComputedLength())
	require.True(t, meta.IsFileLengthSufficient(wantLen), "should be sufficient")
	require.False(t, meta.IsFileLengthSufficient(wantLen-1), "should be insufficient")

	offsetTests := []struct {
		name string
		got  int
		want int
	}{
		{"to-driver", cnc.ToDriverOffset(), cnc.MetaDataLength},
		{
			"to-clients",
			cnc.ToClientsOffset(meta),
			cnc.ToDriverOffset() + int(meta.ToDriverBufferLength),
		},
		{
			"counters-meta",
			cnc.CountersMetaDataOffset(meta),
			cnc.ToClientsOffset(meta) + int(meta.ToClientsBufferLength),
		},
		{
			"counters-values",
			cnc.CountersValuesOffset(meta),
			cnc.CountersMetaDataOffset(meta) + int(meta.CountersMetaDataBufferLength),
		},
		{
			"error-log",
			cnc.ErrorLogOffset(meta),
			cnc.CountersValuesOffset(meta) + int(meta.CountersValuesBufferLength),
		},
	}
	for _, test := range offsetTests {
		require.Equal(t, test.want, test.got, test.name)
	}
}

func TestParseMetadataBufferTooShort(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength-1)
	_, err := cnc.ParseMetadata(buf)
	require.ErrorContains(t, err, "buffer too short")
}

func TestParseMetadataNotReady(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	_, err := cnc.ParseMetadata(buf)
	require.ErrorIs(t, err, cnc.ErrNotReady)
}

func TestParseMetadataIncompatibleMajor(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	writeReadyMetadata(t, buf)
	atomicbuffer.Wrap(buf).PutInt32Release(
		cnc.OffsetCncVersion,
		cnc.ComposeSemanticVersion(99, 0, 0),
	)

	_, err := cnc.ParseMetadata(buf)
	require.ErrorContains(t, err, "incompatible major version")
}

func writeMetadataNegativeDriverLength(t *testing.T, buf []byte) {
	t.Helper()
	writeReadyMetadata(t, buf)
	atomicbuffer.Wrap(buf).PutInt32(cnc.OffsetToDriverBufferLength, -1)
}

func TestParseMetadataNegativeLength(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	writeMetadataNegativeDriverLength(t, buf)

	_, err := cnc.ParseMetadata(buf)
	require.ErrorContains(t, err, "must be positive")
}

func TestParseMetadata_rejectsZeroRingBuffers(t *testing.T) {
	cases := []struct {
		name   string
		offset int
		field  string
	}{
		{"zero to-driver", cnc.OffsetToDriverBufferLength, "to-driver"},
		{"zero to-clients", cnc.OffsetToClientsBufferLength, "to-clients"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			buf := make([]byte, cnc.MetaDataLength)
			writeReadyMetadata(t, buf)
			atomicbuffer.Wrap(buf).PutInt32(test.offset, 0)

			_, err := cnc.ParseMetadata(buf)
			require.ErrorContains(t, err, "must be positive")
		})
	}
}

func TestCncVersionVolatile(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	atomicBuffer := atomicbuffer.Wrap(buf)
	atomicBuffer.PutInt32Release(cnc.OffsetCncVersion, cnc.Version)
	require.Equal(t, cnc.Version, cnc.CncVersionVolatile(buf))
}

func TestCncVersionVolatile_bufferTooShort(t *testing.T) {
	require.Equal(t, int32(0), cnc.CncVersionVolatile([]byte{0, 0, 0}))
}

func TestComputeCncFileLength(t *testing.T) {
	ready := cnc.ReadyMetadata()
	total := ready.ComputedLength() - cnc.MetaDataLength
	want := atomicbuffer.Align(cnc.MetaDataLength+total, int(cnc.TestFilePageSize))

	got, err := cnc.ComputeCncFileLength(total, int(cnc.TestFilePageSize))
	require.NoError(t, err)
	require.Equal(t, want, got)

	for _, alignment := range []int{0, 3} {
		_, err = cnc.ComputeCncFileLength(total, alignment)
		require.Error(t, err)
	}
}

func TestIsFileLengthSufficientNegativeLength(t *testing.T) {
	meta := cnc.Metadata{
		ToDriverBufferLength: -1,
	}
	require.False(
		t,
		meta.IsFileLengthSufficient(cnc.TestLargeFileLength),
		"negative length should not be sufficient",
	)
}

func TestIsFileLengthSufficient_zeroRingBuffer(t *testing.T) {
	meta := cnc.Metadata{
		ToDriverBufferLength: 512,
	}
	require.False(
		t, meta.IsFileLengthSufficient(cnc.TestLargeFileLength),
		"zero to-clients length should not be sufficient",
	)
}

func TestReadMetadataFile_roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	_, err := cnc.ReadMetadataFile(path)
	require.NoError(t, err)
}

func TestReadMetadataFile_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	fileLength := writeWrappedPositiveOverflowCncFile(t, path)
	meta := cnc.WrappedPositiveOverflowMetadata()
	require.Greater(t, meta.ComputedLength(), fileLength)

	_, err := cnc.ReadMetadataFile(path)
	require.ErrorContains(t, err, "insufficient for metadata")
}

func TestReadMetadataFile_notReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, cnc.MetaDataLength+1)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
	_, err := cnc.ReadMetadataFile(path)
	require.ErrorIs(t, err, cnc.ErrNotReady)
}

func TestWaitCncReady_readyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	require.NoError(t, cnc.WaitCncReady(path, time.Second))
}

func TestWaitCncReady_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeWrappedPositiveOverflowCncFile(t, path)

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	require.ErrorContains(t, err, "insufficient for metadata")
}

func TestWaitCncReady_insufficientFileLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, testReadyFileLength())
	writeReadyMetadata(t, buf)

	shortFile := cnc.TestShortCncFileLength
	require.NoError(t, os.WriteFile(path, buf[:shortFile], 0o644))

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	require.ErrorContains(t, err, "insufficient for metadata")
}

func TestWaitCncReady_missingFile(t *testing.T) {
	err := cnc.WaitCncReady(t.TempDir()+"/missing.dat", 10*time.Millisecond)
	require.Error(t, err, "expected timeout")
}

func TestWaitCncReady_invalidMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, cnc.TestShortCncFileLength)
	writeMetadataNegativeDriverLength(t, buf)

	require.NoError(t, os.WriteFile(path, buf, 0o644))

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	require.ErrorContains(t, err, "must be positive")
}
