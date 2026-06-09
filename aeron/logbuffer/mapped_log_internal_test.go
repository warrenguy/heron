package logbuffer

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapLogFile_restatRejectsTruncatedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restat.log")
	WriteTestLogFile(t, path)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	fileInfo, err := file.Stat()
	require.NoError(t, err)
	dims, err := discoverDimensionsFromFile(file, int(fileInfo.Size()))
	require.NoError(t, err)

	require.NoError(t, os.Truncate(path, int64(dims.computed-1)))

	_, err = MapLogFileAfterDiscover(file, path, dims.termLength, dims.pageSize, dims.computed)
	require.EqualError(
		t,
		err,
		fmt.Sprintf("logbuffer: file too short: %d < %d", dims.computed-1, dims.computed),
	)
}
