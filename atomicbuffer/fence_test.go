package atomicbuffer_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	acquireFenceSymbol = "atomicbuffer_acquireFence"
	// dmbISHLD is the hex encoding of DMB ISHLD.
	dmbISHLD = "d50339bf"
)

func TestAcquireFenceARM64LoadBarrier(t *testing.T) {
	goTool, err := exec.LookPath("go")
	require.NoError(t, err)

	probe := filepath.Join(t.TempDir(), "fenceprobe")
	build := exec.Command(goTool, "build", "-o", probe, "./testdata/fenceprobe")
	build.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH=arm64",
		"CGO_ENABLED=0",
		"GOFLAGS=",
	)
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))

	objdump := exec.Command(goTool, "tool", "objdump", "-s", acquireFenceSymbol, probe)
	out, err = objdump.CombinedOutput()
	disassembly := string(out)
	require.NoError(t, err, disassembly)
	require.Contains(t, disassembly, acquireFenceSymbol)
	require.Contains(t, disassembly, dmbISHLD)
}
