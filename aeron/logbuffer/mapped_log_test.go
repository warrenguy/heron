package logbuffer_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/mmap"
)

func openFDCount(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("open FD count requires linux /proc")
	}
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func TestMapLogFile_roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	logbuffer.WriteTestLogFile(t, path)

	mapped, err := logbuffer.MapLogFile(path)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	term2, err := mapped.TermBuffer(2)
	require.NoError(t, err)
	term2.PutInt32(0, 0xABCD)

	meta, err := mapped.MetaDataBuffer()
	require.NoError(t, err)
	meta.PutInt64(logbuffer.OffsetCorrelationID, 4242)
	require.NoError(t, mapped.Close())

	remapped, err := logbuffer.MapLogFile(path)
	require.NoError(t, err)
	defer func() { _ = remapped.Close() }()

	term2Again, err := remapped.TermBuffer(2)
	require.NoError(t, err)
	require.Equal(t, int32(0xABCD), term2Again.GetInt32(0))
	metaAgain, err := remapped.MetaDataBuffer()
	require.NoError(t, err)
	require.Equal(t, int64(4242), metaAgain.GetInt64(logbuffer.OffsetCorrelationID))
}

func TestMapLogBuffer_roundtrip(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())

	mapped, err := logbuffer.MapLogBuffer(buf)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	require.Equal(t, logbuffer.TestTermLength, mapped.TermLength())
	require.Equal(t, logbuffer.TestPageSize, mapped.PageSize())
	require.Equal(t, logbuffer.TestComputedLength, mapped.ComputedLength())
	require.Empty(t, mapped.Path())
}

func write8192PageLogBuffer(t *testing.T) ([]byte, int) {
	t.Helper()
	computed, err := logbuffer.ComputeLogLength(logbuffer.TestTermLength, 8192)
	require.NoError(t, err)
	buf := make([]byte, computed)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
	atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetPageSize, 8192)
	return buf, computed
}

func TestMapLogFile_fileTooShort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.log")
	buf, computed := write8192PageLogBuffer(t)
	require.NoError(t, os.WriteFile(path, buf[:computed-1], 0o644))

	_, err := logbuffer.MapLogFile(path)
	require.EqualError(
		t,
		err,
		fmt.Sprintf("logbuffer: could not discover log layout in span %d", computed-1),
	)
}

func TestMapLogFile_metadataTailTooShort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.log")
	shortSize := logbuffer.MetaDataLength - 1
	require.NoError(t, os.WriteFile(path, make([]byte, shortSize), 0o644))

	_, err := logbuffer.MapLogFile(path)
	require.EqualError(
		t,
		err,
		fmt.Sprintf("logbuffer: file too short: %d < %d", shortSize, logbuffer.MetaDataLength),
	)
}

func TestMapLogFile_invalidTermLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-meta.log")
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
	atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetTermLength, 1000)
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	_, err := logbuffer.MapLogFile(path)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: could not discover log layout in span %d",
			logbuffer.TestComputedLength,
		),
	)
}

func TestMapLogBuffer_bufferTooShort(t *testing.T) {
	_, err := logbuffer.MapLogBuffer(make([]byte, logbuffer.MetaDataLength-1))
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: buffer too short: %d < %d",
			logbuffer.MetaDataLength-1,
			logbuffer.MetaDataLength,
		),
	)
}

func TestMapLogBuffer_computedMismatch(t *testing.T) {
	buf, computed := write8192PageLogBuffer(t)

	_, err := logbuffer.MapLogBuffer(buf[:computed-1])
	require.EqualError(
		t,
		err,
		fmt.Sprintf("logbuffer: could not discover log layout in span %d", computed-1),
	)
}

func TestMapLogBuffer_invalidMetadata(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
	atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetPageSize, 1000)

	_, err := logbuffer.MapLogBuffer(buf)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: could not discover log layout in span %d",
			logbuffer.TestComputedLength,
		),
	)
}

func TestTermBuffer_invalidIndex(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	mapped, err := logbuffer.MapLogBuffer(buf)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	cases := []struct {
		name    string
		index   int
		wantErr string
	}{
		{"negative", -1, "logbuffer: invalid term index: -1"},
		{"metadata section index", logbuffer.LogMetaDataSectionIndex, "logbuffer: invalid term index: 3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapped.TermBuffer(tc.index)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestClose_idempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close.log")
	mapping, err := mmap.MapCreate(path, logbuffer.TestComputedLength)
	require.NoError(t, err)
	logbuffer.WriteSyntheticMetadata(mapping.Data, logbuffer.DefaultSyntheticMetadataConfig())
	require.NoError(t, mapping.Close())

	mapped, err := logbuffer.MapLogFile(path)
	require.NoError(t, err)
	require.NoError(t, mapped.Close())
	require.NoError(t, mapped.Close())
	require.Nil(t, mapped.Bytes())

	_, err = mapped.TermBuffer(0)
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")

	_, err = mapped.MetaDataBuffer()
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")
}

func TestMappedLogBuffer_retainedTermBufferInvalidAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	logbuffer.WriteTestLogFile(t, path)

	mapped, err := logbuffer.MapLogFile(path)
	require.NoError(t, err)

	term0, err := mapped.TermBuffer(0)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TestTermLength, term0.Capacity())

	require.NoError(t, mapped.Close())
	require.Nil(t, mapped.Bytes())

	_, err = mapped.TermBuffer(0)
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")
}

func TestMappedLogBuffer_retainedTermBufferHeapBackedAfterClose(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())

	mapped, err := logbuffer.MapLogBuffer(buf)
	require.NoError(t, err)

	term0, err := mapped.TermBuffer(0)
	require.NoError(t, err)
	term0.PutInt32(0, 0xBEEF)

	require.NoError(t, mapped.Close())

	// Heap-backed MapLogBuffer copies backing; retained AtomicBuffer still aliases
	// the copy until GC. Callers must not use retained views after Close.
	require.Equal(t, int32(0xBEEF), term0.GetInt32(0))
	_, err = mapped.TermBuffer(0)
	require.EqualError(t, err, "logbuffer: closed mapped log buffer")
}

func TestMapLogFile_paddedFilePartialMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "padded.log")
	paddedLength := logbuffer.TestComputedLength + logbuffer.TestPageSize
	buf := make([]byte, paddedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	mapped, err := logbuffer.MapLogFile(path)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	require.Equal(t, logbuffer.TestComputedLength, mapped.ComputedLength())
	require.Len(t, mapped.Bytes(), logbuffer.TestComputedLength)

	term0, err := mapped.TermBuffer(0)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TestTermLength, term0.Capacity())
}

func TestMapLogFile_errorPathsNoFDLeak(t *testing.T) {
	before := openFDCount(t)

	cases := []struct {
		name  string
		setup func(t *testing.T) string
	}{
		{
			name: "file too short for metadata",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "short-meta.log")
				require.NoError(t, os.WriteFile(path, make([]byte, 32), 0o644))
				return path
			},
		},
		{
			name: "metadata tail too short",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "tiny.log")
				shortSize := logbuffer.MetaDataLength - 1
				require.NoError(t, os.WriteFile(path, make([]byte, shortSize), 0o644))
				return path
			},
		},
		{
			name: "file too short for computed length",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "short-computed.log")
				buf, computed := write8192PageLogBuffer(t)
				require.NoError(t, os.WriteFile(path, buf[:computed-1], 0o644))
				return path
			},
		},
		{
			name: "invalid layout",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "bad-meta.log")
				buf := make([]byte, logbuffer.TestComputedLength)
				logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
				metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
				atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetTermLength, 1000)
				require.NoError(t, os.WriteFile(path, buf, 0o644))
				return path
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.setup(t)
			_, err := logbuffer.MapLogFile(path)
			require.Error(t, err)

			after := openFDCount(t)
			require.Equal(t, before, after)
		})
	}
}

func TestMapLogFile_missingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.log")
	_, err := logbuffer.MapLogFile(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMapLogBufferFromBytes_roundtrip(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())

	mapped, err := logbuffer.MapLogBufferFromBytes(buf)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	term0, err := mapped.TermBuffer(0)
	require.NoError(t, err)
	term0.PutInt32(0, 0xBEEF)

	remapped, err := logbuffer.MapLogBufferFromBytes(buf)
	require.NoError(t, err)
	defer func() { _ = remapped.Close() }()

	term0Again, err := remapped.TermBuffer(0)
	require.NoError(t, err)
	require.Equal(t, int32(0), term0Again.GetInt32(0))
}

func TestMapLogBufferFromBytes_bufferTooShort(t *testing.T) {
	_, err := logbuffer.MapLogBufferFromBytes(make([]byte, logbuffer.MetaDataLength-1))
	require.EqualError(t, err, "logbuffer: buffer too short: 4095 < 4096")
}
