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
	releaseFenceSymbol = "atomicbuffer_releaseFence"
	// dmbISHLD is the hex encoding of DMB ISHLD.
	dmbISHLD = "d50339bf"
	// dmbISH is the hex encoding of DMB ISH.
	dmbISH = "d5033bbf"
)

func TestFenceARM64Barrier(t *testing.T) {
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

	tests := []struct {
		name     string
		symbol   string
		encoding string
	}{
		{name: "AcquireFence", symbol: acquireFenceSymbol, encoding: dmbISHLD},
		{name: "ReleaseFence", symbol: releaseFenceSymbol, encoding: dmbISH},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objdump := exec.Command(goTool, "tool", "objdump", "-s", tt.symbol, probe)
			out, err := objdump.CombinedOutput()
			disassembly := string(out)
			require.NoError(t, err, disassembly)
			require.Contains(t, disassembly, tt.symbol)
			require.Contains(t, disassembly, tt.encoding)
		})
	}
}
