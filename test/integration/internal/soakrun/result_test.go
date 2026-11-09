package soakrun

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseOverrides(t *testing.T) {
	t.Parallel()

	// Phase: empty texts use the defaults.
	empty, err := ParseOverrides("", "")
	require.NoError(t, err)
	require.False(t, empty.HasSeed)
	require.Equal(t, 30*time.Minute, empty.Duration)

	// Phase: seed 0 sets HasSeed.
	zero, err := ParseOverrides("0", "")
	require.NoError(t, err)
	require.True(t, zero.HasSeed)
	require.Zero(t, zero.Seed)

	// Phase: valid seed and duration.
	both, err := ParseOverrides("42", "5m")
	require.NoError(t, err)
	require.True(t, both.HasSeed)
	require.Equal(t, uint64(42), both.Seed)
	require.Equal(t, 5*time.Minute, both.Duration)

	// Phase: a bad seed or duration fails.
	invalid := []struct{ seed, duration string }{
		{seed: "-1"},
		{duration: "later"},
		{duration: "0s"},
		{duration: "-1s"},
	}

	for _, in := range invalid {
		_, err = ParseOverrides(in.seed, in.duration)
		require.Error(t, err, "seed %q duration %q", in.seed, in.duration)
	}
}

func TestWriteSeedFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed uint64
		want string
	}{
		{name: "nonzero", seed: 42, want: "42\n"},
		{name: "zero", seed: 0, want: "0\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, WriteSeedFile(dir, tc.seed))

			raw, err := os.ReadFile(filepath.Join(dir, seedFileName))
			require.NoError(t, err)
			require.Equal(t, tc.want, string(raw))
		})
	}
}

func TestUnplannedElections(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(0), UnplannedElections(1, 4, 3))
	require.Equal(t, int64(2), UnplannedElections(1, 6, 3))
	require.Equal(t, int64(0), UnplannedElections(1, 2, 3))
}
