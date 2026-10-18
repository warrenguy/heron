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

// syntheticLog builds a log of logLength bytes. Its tail holds metadata that
// declares termLength and pageSize. logLength 0 selects the computed length.
func syntheticLog(tb testing.TB, termLength, pageSize int32, logLength int) []byte {
	tb.Helper()
	if logLength == 0 {
		computed, err := logbuffer.ComputeLogLength(int(termLength), int(pageSize))
		require.NoError(tb, err)
		logLength = computed
	}
	cfg := logbuffer.DefaultSyntheticMetadataConfig()
	cfg.TermLength = termLength
	cfg.PageSize = pageSize
	buf := make([]byte, logLength)
	logbuffer.WriteSyntheticMetadata(buf, cfg)
	return buf
}

func TestMapLog_metadataAtTail(t *testing.T) {
	cases := []struct {
		name           string
		build          func(t *testing.T) []byte
		wantTermLength int
		wantPageSize   int
		wantErr        string
	}{
		{
			name:           "64 KiB terms, 4 KiB page",
			build:          func(t *testing.T) []byte { return syntheticLog(t, 65536, 4096, 0) },
			wantTermLength: 65536,
			wantPageSize:   4096,
		},
		{
			name:           "64 KiB terms, 8 KiB page",
			build:          func(t *testing.T) []byte { return syntheticLog(t, 65536, 8192, 0) },
			wantTermLength: 65536,
			wantPageSize:   8192,
		},
		{
			name:           "64 KiB terms, 64 KiB page",
			build:          func(t *testing.T) []byte { return syntheticLog(t, 65536, 65536, 0) },
			wantTermLength: 65536,
			wantPageSize:   65536,
		},
		{
			name:           "64 KiB terms, 1 MiB page",
			build:          func(t *testing.T) []byte { return syntheticLog(t, 65536, 1<<20, 0) },
			wantTermLength: 65536,
			wantPageSize:   1 << 20,
		},
		{
			name: "128 KiB terms with planted metadata in term 1",
			build: func(t *testing.T) []byte {
				buf := syntheticLog(t, 131072, 4096, 0)
				planted := atomicbuffer.Wrap(buf[196608 : 196608+logbuffer.MetaDataLength])
				planted.PutInt32(logbuffer.OffsetTermLength, 65536)
				planted.PutInt32(logbuffer.OffsetPageSize, 4096)
				planted.PutInt32(logbuffer.OffsetMTULength, logbuffer.TestMTULength)
				return buf
			},
			wantTermLength: 131072,
			wantPageSize:   4096,
		},
		{
			name: "metadata at three terms with zeros at the tail",
			build: func(t *testing.T) []byte {
				buf := make([]byte, 262144)
				cfg := logbuffer.DefaultSyntheticMetadataConfig()
				cfg.PageSize = 65536
				logbuffer.WriteSyntheticMetadata(buf[:196608+logbuffer.MetaDataLength], cfg)
				return buf
			},
			wantErr: "logbuffer: term length below minimum: 0 < 65536",
		},
		{
			name:  "tail declares a larger page",
			build: func(t *testing.T) []byte { return syntheticLog(t, 65536, 8192, 200704) },
			wantErr: "logbuffer: log length 200704 != computed length 204800 " +
				"(term length 65536, page size 8192)",
		},
		{
			name:  "file padded past the computed length",
			build: func(t *testing.T) []byte { return syntheticLog(t, 65536, 4096, 204800) },
			wantErr: "logbuffer: log length 204800 != computed length 200704 " +
				"(term length 65536, page size 4096)",
		},
		{
			name:  "tail declares a 1 GiB term",
			build: func(t *testing.T) []byte { return syntheticLog(t, 1<<30, 4096, 200704) },
			wantErr: "logbuffer: log length 200704 != computed length 3221229568 " +
				"(term length 1073741824, page size 4096)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := tc.build(t)
			check := func(t *testing.T, mapped *logbuffer.MappedLogBuffer, err error) {
				t.Helper()
				if tc.wantErr != "" {
					require.EqualError(t, err, tc.wantErr)
					require.Nil(t, mapped)
					return
				}
				require.NoError(t, err)
				defer func() { _ = mapped.Close() }()

				require.Equal(t, tc.wantTermLength, mapped.TermLength())
				require.Equal(t, tc.wantPageSize, mapped.PageSize())
				require.Equal(t, len(buf), mapped.ComputedLength())
				require.Len(t, mapped.Bytes(), len(buf))
				terms, err := mapped.TermBuffers()
				require.NoError(t, err)
				for i := range logbuffer.PartitionCount {
					require.Equal(t, tc.wantTermLength, terms[i].Capacity())
				}
				meta, err := mapped.MetaDataBuffer()
				require.NoError(t, err)
				require.Equal(
					t,
					logbuffer.DefaultSyntheticMetadataConfig().CorrelationID,
					meta.GetInt64(logbuffer.OffsetCorrelationID),
				)
			}

			t.Run("buffer", func(t *testing.T) {
				mapped, err := logbuffer.MapLogBuffer(buf)
				check(t, mapped, err)
			})
			t.Run("file", func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "tail.log")
				require.NoError(t, os.WriteFile(path, buf, 0o644))
				mapped, err := logbuffer.MapLogFile(path)
				check(t, mapped, err)
			})
		})
	}
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
	metaStart := logbuffer.LogMetaDataOffset(len(buf))
	atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetTermLength, 1000)
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	_, err := logbuffer.MapLogFile(path)
	require.EqualError(t, err, "logbuffer: term length below minimum: 1000 < 65536")
}

func TestMapLogFile_rejectsJavaDriverLargePageLog(t *testing.T) {
	const (
		termLength = 512 * 1024 * 1024
		pageSize   = 1024 * 1024 * 1024
		logLength  = 2684354560
	)
	path := filepath.Join(t.TempDir(), "large-page.log")
	file, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	require.NoError(t, file.Truncate(logLength))

	var dims [logbuffer.OffsetPageSize + atomicbuffer.SizeInt32 - logbuffer.OffsetTermLength]byte
	fields := atomicbuffer.Wrap(dims[:])
	fields.PutInt32(0, termLength)
	fields.PutInt32(logbuffer.OffsetPageSize-logbuffer.OffsetTermLength, pageSize)
	offset := logbuffer.LogMetaDataOffset(logLength) + logbuffer.OffsetTermLength
	_, err = file.WriteAt(dims[:], int64(offset))
	require.NoError(t, err)

	_, err = logbuffer.MapLogFile(path)
	require.EqualError(
		t,
		err,
		"logbuffer: log length 2684354560 != computed length 2147483648 "+
			"(term length 536870912, page size 1073741824)",
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

func TestMapLogBuffer_invalidMetadata(t *testing.T) {
	buf := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
	metaStart := logbuffer.LogMetaDataOffset(len(buf))
	atomicbuffer.Wrap(buf[metaStart:]).PutInt32(logbuffer.OffsetPageSize, 1000)

	_, err := logbuffer.MapLogBuffer(buf)
	require.EqualError(t, err, "logbuffer: page size below minimum: 1000 < 4096")
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
			name: "log length mismatch",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "length-mismatch.log")
				buf := syntheticLog(t, logbuffer.TestTermLength, 8192, logbuffer.TestComputedLength)
				require.NoError(t, os.WriteFile(path, buf, 0o644))
				return path
			},
		},
		{
			name: "invalid layout",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "bad-meta.log")
				buf := make([]byte, logbuffer.TestComputedLength)
				logbuffer.WriteSyntheticMetadata(buf, logbuffer.DefaultSyntheticMetadataConfig())
				metaStart := logbuffer.LogMetaDataOffset(len(buf))
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
