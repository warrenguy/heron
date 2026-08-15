//go:build testhooks

package service

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServicePackage_importBan(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(thisFile)

	denied := []string{
		"github.com/warrenguy/heron/cluster/consensus",
		"github.com/warrenguy/heron/cluster/election",
		"github.com/warrenguy/heron/cluster/recordinglog",
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		require.NoError(t, err, path)
		for _, spec := range file.Imports {
			imp, err := strconv.Unquote(spec.Path.Value)
			require.NoError(t, err, path)
			require.NotContains(t, denied, imp, "%s imports %s", entry.Name(), imp)
		}
	}
}
