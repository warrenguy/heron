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

// TestPublicationCodegen asserts the calls that the offer path, TryClaim,
// Commit, and Abort make on both publication types.
//
// Success paths call at most runtime.memmove or offerMessage. Every other
// allowed call sits on a failure, fragmented, gather, supplier, panic, or
// stack-growth branch.
func TestPublicationCodegen(t *testing.T) {
	t.Parallel()

	const aeronPrefix = "github.com/warrenguy/luas/aeron."
	offerBounds := []string{
		"runtime.gopanic", "runtime.convTstring", aeronPrefix + "invalidOfferSliceMessage",
	}
	offer := slices.Concat(offerBounds, []string{"runtime.memmove"})
	offerFrame := []string{"runtime.memmove", aeronPrefix + "commitOfferFrame"}
	claimLength := []string{aeronPrefix + "invalidClaimLengthError"}
	claimFrame := []string{
		aeronPrefix + "claimFrameError", aeronPrefix + "invalidTermIndexError",
		aeronPrefix + "invalidFrameOffsetError",
	}
	concurrent := []string{
		aeronPrefix + "(*Publication).handleEndOfLog", aeronPrefix + "backPressureStatus",
	}

	// IsConnectedVolatile inlines on amd64. Its boundsCheck panic call remains.
	exclusiveBackPressure := map[string]string{
		"amd64": "runtime.gopanic",
		"arm64": "github.com/warrenguy/luas/aeron/logbuffer.IsConnectedVolatile",
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
			"(*Publication).OfferWithReservedValue": slices.Concat(offerBounds, []string{
				aeronPrefix + "(*Publication).offerMessage",
			}),
			"(*ExclusivePublication).OfferWithReservedValue": slices.Concat(offerBounds, []string{
				aeronPrefix + "(*ExclusivePublication).offerMessage",
			}),
			"(*Publication).offerMessage": slices.Concat(offerFrame, concurrent, []string{
				aeronPrefix + "(*Publication).appendFragmentedMessage",
			}),
			"(*ExclusivePublication).offerMessage": slices.Concat(offerFrame, exclusive, []string{
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
				copiesPayload := strings.HasSuffix(method, ".OfferWithOffset") ||
					strings.HasSuffix(method, ".offerMessage")
				if copiesPayload {
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
