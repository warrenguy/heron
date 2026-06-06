package cnc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapCncFile_restatRejectsTruncatedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	meta := writeReadyCncFileAt(t, path)
	computedLength := meta.ComputedLength()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	fileInfo, err := file.Stat()
	require.NoError(t, err)
	headerMeta, err := readMetadataHeaderFromFile(file, path, int(fileInfo.Size()), nil)
	require.NoError(t, err)

	require.NoError(t, os.Truncate(path, int64(computedLength-1)))

	_, err = mapCncFileAfterHeader(file, path, headerMeta)
	require.ErrorContains(t, err, "cnc: file length")
}

func TestReadMetadataHeaderFromFile_staleFileSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	meta := writeReadyCncFileAt(t, path)
	computedLength := meta.ComputedLength()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	staleSize := computedLength - TestStaleFileSizeShrink
	_, err = readMetadataHeaderFromFile(file, path, staleSize, nil)
	require.ErrorContains(t, err, "insufficient for metadata")
}
