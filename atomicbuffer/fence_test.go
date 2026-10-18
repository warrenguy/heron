package atomicbuffer_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
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
	// retARM64 is the hex encoding of the arm64 RET.
	retARM64 = "d65f03c0"
	// retAMD64 is the hex encoding of the amd64 RET.
	retAMD64 = "c3"
)

// objdumpEncoding captures the hex encoding column of one go tool objdump
// instruction line. Alignment padding prints a ? mnemonic and does not match.
var objdumpEncoding = regexp.MustCompile(`(?m)^\s+\S+:\d+\t0x[0-9a-f]+\t+([0-9a-f]+)\t+[A-Z]`)

// buildProbe cross-builds testdata/fenceprobe for goarch and returns the go
// tool and the binary path.
func buildProbe(t *testing.T, goarch string, race bool) (goTool, probe string) {
	t.Helper()
	goTool, err := exec.LookPath("go")
	require.NoError(t, err)

	probe = filepath.Join(t.TempDir(), "fenceprobe")
	args := []string{"build", "-o", probe}
	if race {
		args = append(args, "-race")
	}
	build := exec.Command(goTool, append(args, "./testdata/fenceprobe")...)
	build.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH="+goarch,
		"GOFLAGS=",
	)
	if !race {
		build.Env = append(build.Env, "CGO_ENABLED=0")
	}
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))
	return goTool, probe
}

// symbolEncodings returns the instruction encodings of one assembly symbol in
// probe, in order.
func symbolEncodings(t *testing.T, goTool, probe, symbol string) []string {
	t.Helper()
	objdump := exec.Command(goTool, "tool", "objdump", "-s", "^"+symbol+`\.abi0$`, probe)
	out, err := objdump.CombinedOutput()
	require.NoError(t, err, string(out))

	var encodings []string
	for _, match := range objdumpEncoding.FindAllStringSubmatch(string(out), -1) {
		encodings = append(encodings, match[1])
	}
	return encodings
}

func TestFenceARM64Barrier(t *testing.T) {
	t.Parallel()
	goTool, probe := buildProbe(t, "arm64", false)

	tests := []struct {
		name   string
		symbol string
		want   []string
	}{
		{name: "AcquireFence", symbol: acquireFenceSymbol, want: []string{dmbISHLD, retARM64}},
		{name: "ReleaseFence", symbol: releaseFenceSymbol, want: []string{dmbISH, retARM64}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, symbolEncodings(t, goTool, probe, tt.symbol))
		})
	}
}

func TestReleaseAMD64Codegen(t *testing.T) {
	t.Parallel()
	goTool, probe := buildProbe(t, "amd64", false)

	tests := []struct {
		name   string
		symbol string
		want   []string
	}{
		{
			name:   "PutInt32Release",
			symbol: "atomicbuffer_storeRelease32",
			want:   []string{"488b442408", "8b5c2410", "8918", retAMD64},
		},
		{
			name:   "PutInt64Release",
			symbol: "atomicbuffer_storeRelease64",
			want:   []string{"488b442408", "488b5c2410", "488918", retAMD64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, symbolEncodings(t, goTool, probe, tt.symbol))
		})
	}

	t.Run("race", func(t *testing.T) {
		if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
			t.Skip("race probe: host is not linux/amd64")
		}
		cgo, err := exec.Command(goTool, "env", "CGO_ENABLED").Output()
		require.NoError(t, err)
		if strings.TrimSpace(string(cgo)) != "1" {
			t.Skip("race probe: cgo disabled")
		}
		cc, err := exec.Command(goTool, "env", "CC").Output()
		require.NoError(t, err)
		if _, err := exec.LookPath(strings.TrimSpace(string(cc))); err != nil {
			t.Skip("race probe: no C compiler")
		}

		goTool, raceProbe := buildProbe(t, "amd64", true)
		objdump := exec.Command(goTool, "tool", "objdump", "-s", "^atomicbuffer_", raceProbe)
		out, err := objdump.CombinedOutput()
		require.NoError(t, err, string(out))
		require.Empty(t, string(out))
	})
}
