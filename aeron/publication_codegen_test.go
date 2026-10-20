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

// TestPublicationCodegen asserts the calls that OfferWithOffset, TryClaim,
// Commit, and Abort make on both publication types. The OfferWithOffset
// success path calls only runtime.memmove. The claim success paths do not
// call anything. Every other allowed call sits on a failure, fragmented,
// panic, or stack-growth branch.
func TestPublicationCodegen(t *testing.T) {
	t.Parallel()

	const aeronPrefix = "github.com/warrenguy/heron/aeron."
	offer := []string{
		"runtime.memmove", "runtime.gopanic", "runtime.convTstring",
		aeronPrefix + "invalidOfferSliceMessage",
	}
	claimLength := []string{aeronPrefix + "invalidClaimLengthError"}
	claimFrame := []string{
		aeronPrefix + "claimFrameError", aeronPrefix + "invalidTermIndexError",
		aeronPrefix + "invalidFrameOffsetError",
	}
	concurrent := []string{
		aeronPrefix + "(*Publication).handleEndOfLog", aeronPrefix + "backPressureStatus",
	}

	// backPressureStatusForLog inlines on arm64 and reads the connected flag.
	exclusiveBackPressure := map[string]string{
		"amd64": aeronPrefix + "backPressureStatusForLog",
		"arm64": "github.com/warrenguy/heron/aeron/logbuffer.IsConnectedVolatile",
	}

	for goarch, backPressure := range exclusiveBackPressure {
		exclusive := []string{
			aeronPrefix + "(*ExclusivePublication).rotateAtTermEnd", backPressure,
		}
		allowed := map[string][]string{
			"(*Publication).OfferWithOffset": slices.Concat(offer, concurrent, []string{
				aeronPrefix + "(*Publication).appendFragmentedMessage",
			}),
			"(*ExclusivePublication).OfferWithOffset": slices.Concat(offer, exclusive, []string{
				aeronPrefix + "(*ExclusivePublication).appendFragmentedMessage",
			}),
			"(*Publication).TryClaim":          slices.Concat(claimLength, concurrent),
			"(*ExclusivePublication).TryClaim": slices.Concat(claimLength, exclusive),
			"(*Publication).Commit":            claimFrame,
			"(*ExclusivePublication).Commit":   claimFrame,
			"(*Publication).Abort":             claimFrame,
			"(*ExclusivePublication).Abort":    claimFrame,
		}

		probe := filepath.Join(t.TempDir(), "codegenprobe-"+goarch)
		build := exec.Command("go", "build", "-o", probe, "./testdata/codegenprobe")
		build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+goarch, "GOFLAGS=", "CGO_ENABLED=0")
		out, err := build.CombinedOutput()
		require.NoError(t, err, string(out))

		for method, calls := range allowed {
			t.Run(goarch+"/"+method, func(t *testing.T) {
				targets := callTargets(t, probe, aeronPrefix+method)
				if strings.HasSuffix(method, ".OfferWithOffset") {
					require.Contains(t, targets, "runtime.memmove", method)
				}
				for _, target := range targets {
					ok := slices.Contains(calls, target) ||
						target == "runtime.morestack_noctxt.abi0" ||
						strings.HasPrefix(target, "runtime.panic")
					require.True(t, ok, "calls %s", target)
				}
			})
		}
	}
}

// objdumpCall captures the target of one direct CALL.
var objdumpCall = regexp.MustCompile(`\tCALL (\S+)\(SB\)`)

// callTargets returns the CALL targets of symbol in probe. It fails t when the
// symbol is missing or makes an indirect call.
func callTargets(t *testing.T, probe, symbol string) []string {
	t.Helper()
	out, err := exec.Command("go", "tool", "objdump", "-s", "^"+regexp.QuoteMeta(symbol)+"$", probe).
		CombinedOutput()
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "TEXT "+symbol+"(SB)", "missing symbol")

	var targets []string
	for _, match := range objdumpCall.FindAllStringSubmatch(string(out), -1) {
		targets = append(targets, match[1])
	}

	require.Len(t, targets, strings.Count(string(out), "\tCALL "), "indirect call")
	return targets
}
