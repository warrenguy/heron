package mmap_test

import (
	"bytes"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"luas.fast/mmap"
)

func threadMinorFaults(t *testing.T) int64 {
	t.Helper()
	var usage unix.Rusage
	require.NoError(t, unix.Getrusage(unix.RUSAGE_THREAD, &usage))
	return usage.Minflt
}

func writeFaults(t *testing.T, touch func([]byte)) int64 {
	t.Helper()
	const length = 4 << 20
	pageSize := os.Getpagesize()

	path := testMapPath(t, "touch.bin")
	content := bytes.Repeat([]byte{0x5A}, length)
	require.NoError(t, os.WriteFile(path, content, 0o600))

	m, err := mmap.MapExisting(path, length)
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if touch != nil {
		touch(m.Data)
	}
	before := threadMinorFaults(t)
	for offset := 0; offset < length; offset += pageSize {
		m.Data[offset] = 0xA5
	}
	faults := threadMinorFaults(t) - before

	for offset := 0; offset < length; offset += pageSize {
		m.Data[offset] = 0x5A
	}
	require.True(t, bytes.Equal(content, m.Data))
	return faults
}

func TestPreTouch_writeDoesNotFault(t *testing.T) {
	cases := []struct {
		name       string
		touch      func([]byte)
		wantFaults bool
	}{
		{name: "untouched", wantFaults: true},
		{name: "PreTouch", touch: func(data []byte) { mmap.PreTouch(data) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The thread count sometimes includes a stray fault. The fewest of
			// three attempts drops the stray fault.
			faults := min(writeFaults(t, tc.touch), writeFaults(t, tc.touch), writeFaults(t, tc.touch))

			if tc.wantFaults {
				require.Positive(t, faults)
			} else {
				require.Zero(t, faults)
			}
		})
	}
}
