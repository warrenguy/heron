package aeron_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOfferWithOffsetCodegen asserts the calls each OfferWithOffset makes.
// The success path calls only runtime.memmove. The other allowed calls sit on
// failure, fragmented, panic, or stack-growth branches.
func TestOfferWithOffsetCodegen(t *testing.T) {
	t.Parallel()

	const aeronPrefix = "github.com/warrenguy/heron/aeron."
	common := []string{
		"runtime.memmove", "runtime.morestack_noctxt.abi0", "runtime.gopanic",
		"runtime.convTstring", aeronPrefix + "invalidOfferSliceMessage",
	}
	concurrent := []string{
		aeronPrefix + "(*Publication).appendFragmentedMessage",
		aeronPrefix + "(*Publication).handleEndOfLog", aeronPrefix + "backPressureStatus",
	}
	exclusive := []string{
		aeronPrefix + "(*ExclusivePublication).appendFragmentedMessage",
		aeronPrefix + "(*ExclusivePublication).rotateAtTermEnd",
	}

	backPressure := aeronPrefix + "backPressureStatusForLog"

	// backPressureStatusForLog inlines on arm64 and reads the connected flag.
	connectedFlag := "github.com/warrenguy/heron/aeron/logbuffer.IsConnectedVolatile"
	allowed := map[string]map[string][]string{
		"amd64": {
			"Publication":          concurrent,
			"ExclusivePublication": slices.Concat(exclusive, []string{backPressure}),
		},
		"arm64": {
			"Publication":          concurrent,
			"ExclusivePublication": slices.Concat(exclusive, []string{connectedFlag}),
		},
	}

	for goarch, receivers := range allowed {
		probe := filepath.Join(t.TempDir(), "offerprobe-"+goarch)
		build := exec.Command("go", "build", "-o", probe, "./testdata/offerprobe")
		build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+goarch, "GOFLAGS=", "CGO_ENABLED=0")
		out, err := build.CombinedOutput()
		require.NoError(t, err, string(out))

		for receiver, calls := range receivers {
			t.Run(goarch+"/"+receiver, func(t *testing.T) {
				for _, target := range callTargets(t, probe, aeronPrefix+"(*"+receiver+").OfferWithOffset") {
					ok := slices.Contains(common, target) || slices.Contains(calls, target)
					require.True(t, ok || strings.HasPrefix(target, "runtime.panic"), "calls %s", target)
				}
			})
		}
	}
}

// objdumpCall captures the target of one direct CALL.
var objdumpCall = regexp.MustCompile(`\tCALL (\S+)\(SB\)`)

// callTargets returns the CALL targets of symbol in probe. It fails t when the
// symbol is missing, has no memmove, or makes an indirect call.
func callTargets(t *testing.T, probe, symbol string) []string {
	t.Helper()
	out, err := exec.Command("go", "tool", "objdump", "-s", "^"+regexp.QuoteMeta(symbol)+"$", probe).
		CombinedOutput()
	require.NoError(t, err, string(out))

	var targets []string
	for _, match := range objdumpCall.FindAllStringSubmatch(string(out), -1) {
		targets = append(targets, match[1])
	}

	require.Contains(t, targets, "runtime.memmove", symbol)
	require.Len(t, targets, strings.Count(string(out), "\tCALL "), "indirect call")
	return targets
}
