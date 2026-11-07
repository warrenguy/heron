package linearizability

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
)

// recordingTB stores how [RequireLinearizable] uses its testing.TB. FailNow
// stores the failure and does not exit the goroutine. [RequireLinearizable]
// does not run code after the require call.
type recordingTB struct {
	testing.TB
	dir              string
	artifactDirCalls int
	failed           bool
}

func (tb *recordingTB) Helper() {}

func (tb *recordingTB) Name() string {
	return "recordingTB"
}

func (tb *recordingTB) ArtifactDir() string {
	tb.artifactDirCalls++
	return tb.dir
}

func (tb *recordingTB) Errorf(string, ...any) {}

func (tb *recordingTB) FailNow() {
	tb.failed = true
}

// slowKeyOps returns operations whose search cannot finish in a test run. The
// get never matches. Each append order gives the search a distinct state.
func slowKeyOps(key string) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, 65)
	for i := range 64 {
		ops = append(ops, pendingOp(OpAppend, key, testToken(i), 0))
	}
	return append(ops, getOp(key, "x", 1, 2))
}

func TestCheck_stopsAtTheFirstIllegalKey(t *testing.T) {
	t.Parallel()

	ops := append(slowKeyOps("k1"), getOp("k0", "x", 0, 1))

	started := time.Now()
	require.Equal(t, porcupine.Illegal, Check(ops, testCheckTimeout))
	require.Less(t, time.Since(started), testCheckTimeout)
}

func TestCheckAndVisualize_writesOnFailure(t *testing.T) {
	t.Parallel()

	t.Run("illegal history writes the visualization", func(t *testing.T) {
		t.Parallel()

		ops := []porcupine.Operation{
			appendOp("k0", "1,", "1,", 0, 10),
			pendingOp(OpAppend, "k0", "2,", 20),
			getOp("k0", "", 30, 40),
		}
		ops[0].Metadata = int64(3)
		ops[2].Metadata = int64(3)

		dir := t.TempDir()
		err := checkAndVisualize(ops, testCheckTimeout, func() string { return dir })
		require.ErrorContains(t, err, string(porcupine.Illegal))

		path := filepath.Join(dir, visualizationFile)
		require.ErrorContains(t, err, path)
		info, statErr := os.Stat(path)
		require.NoError(t, statErr)
		require.Positive(t, info.Size())
	})

	t.Run("timed-out check writes the visualization", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		err := checkAndVisualize(slowKeyOps("k0"), 10*time.Millisecond, func() string { return dir })
		require.ErrorContains(t, err, string(porcupine.Unknown))
		require.FileExists(t, filepath.Join(dir, visualizationFile))
	})

	t.Run("ok history does not call dir", func(t *testing.T) {
		t.Parallel()

		ops := []porcupine.Operation{
			appendOp("k0", "1,", "1,", 0, 10),
			pendingOp(OpAppend, "k0", "2,", 20),
			getOp("k0", "1,", 30, 40),
		}

		err := checkAndVisualize(ops, testCheckTimeout, func() string {
			require.FailNow(t, "dir called on an ok history")
			return ""
		})
		require.NoError(t, err)
	})

	t.Run("unwritable directory names the result", func(t *testing.T) {
		t.Parallel()

		ops := []porcupine.Operation{getOp("k0", "x", 0, 10)}
		missing := filepath.Join(t.TempDir(), "missing")

		err := checkAndVisualize(ops, testCheckTimeout, func() string { return missing })
		require.ErrorContains(t, err, string(porcupine.Illegal))
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestRequireLinearizable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		getOutput  string
		wantFailed bool
	}{
		{name: "illegal history fails and writes the visualization", getOutput: "", wantFailed: true},
		{name: "ok history passes and does not call ArtifactDir", getOutput: "1,", wantFailed: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ops := []porcupine.Operation{
				putOp("k0", "1,", 0, 10),
				getOp("k0", test.getOutput, 20, 30),
			}

			tb := &recordingTB{dir: t.TempDir()}
			RequireLinearizable(tb, ops, testCheckTimeout)

			require.Equal(t, test.wantFailed, tb.failed)
			path := filepath.Join(tb.dir, visualizationFile)
			if test.wantFailed {
				require.Equal(t, 1, tb.artifactDirCalls)
				require.FileExists(t, path)
			} else {
				require.Zero(t, tb.artifactDirCalls)
				require.NoFileExists(t, path)
			}
		})
	}
}
