package conductor_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
)

func TestValidateLogFileName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	inside := filepath.Join(dir, "logs", "pub.log")
	outside := filepath.Join(t.TempDir(), "escape.log")

	tests := []struct {
		name           string
		logFileName    string
		mediaDriverDir string
		wantErr        error
	}{
		{
			name:        "empty",
			logFileName: "",
			wantErr:     conductor.ErrLogFileNameEmpty,
		},
		{
			name:        "nonASCII",
			logFileName: "/tmp/\xff.log",
			wantErr:     conductor.ErrLogFileNameNotASCII,
		},
		{
			name:           "insideDir",
			logFileName:    inside,
			mediaDriverDir: dir,
		},
		{
			name:           "relativeRejected",
			logFileName:    "relative.log",
			mediaDriverDir: dir,
			wantErr:        conductor.ErrLogFilePathNotAbsolute,
		},
		{
			name:           "outsideDir",
			logFileName:    outside,
			mediaDriverDir: dir,
			wantErr:        conductor.ErrLogFileOutsideMediaDriverDir,
		},
		{
			name:           "emptyMediaDriverDirSkipsConfinement",
			logFileName:    inside,
			mediaDriverDir: "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := conductor.ValidateLogFileNameForTest(
				testCase.logFileName,
				testCase.mediaDriverDir,
			)
			if testCase.wantErr != nil {
				require.ErrorIs(t, err, testCase.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateLogFileName_dottedPrefixBasenameAccepted(t *testing.T) {
	t.Parallel()

	driverDir := t.TempDir()
	dotPrefixLog := filepath.Join(driverDir, "..foo.log")
	require.NoError(t, os.WriteFile(dotPrefixLog, nil, 0o644))

	require.NoError(t, conductor.ValidateLogFileNameForTest(dotPrefixLog, driverDir))
}

func TestValidateLogFileName_relativeMediaDriverDir(t *testing.T) {
	base := t.TempDir()
	driverDir := filepath.Join(base, "driver")
	require.NoError(t, os.MkdirAll(driverDir, 0o755))

	logPath := filepath.Join(driverDir, "pub.log")
	require.NoError(t, os.WriteFile(logPath, nil, 0o644))

	t.Chdir(base)

	require.NoError(t, conductor.ValidateLogFileNameForTest(logPath, "driver"))
}

func TestValidateLogFileName_symlinkEscape(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	driverDir := filepath.Join(baseDir, "driver")
	outsideFile := filepath.Join(baseDir, "outside", "secret.log")
	require.NoError(t, os.MkdirAll(driverDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(outsideFile), 0o755))
	require.NoError(t, os.WriteFile(outsideFile, nil, 0o644))

	linkPath := filepath.Join(driverDir, "escape.log")
	require.NoError(t, os.Symlink(outsideFile, linkPath))

	require.ErrorIs(
		t,
		conductor.ValidateLogFileNameForTest(linkPath, driverDir),
		conductor.ErrLogFileOutsideMediaDriverDir,
	)
}
