package atomicbuffer_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
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
)

// buildProbe cross-builds testdata/fenceprobe for linux and goarch and
// returns the binary path. A race build needs cgo.
func buildProbe(t *testing.T, goarch string, race bool) string {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "fenceprobe")
	args := []string{"build", "-o", probe}
	if race {
		args = append(args, "-race")
	}

	build := exec.Command("go", append(args, "./testdata/fenceprobe")...)
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+goarch, "GOFLAGS=")
	if !race {
		build.Env = append(build.Env, "CGO_ENABLED=0")
	}

	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))
	return probe
}

// instruction is one objdump instruction line: the file:line the compiler
// gives it, its hex encoding, and its assembler text.
type instruction struct {
	source   string
	encoding string
	text     string
}

// objdumpSymbol returns the instructions of the symbol named exactly symbol
// in probe, without NOP or alignment padding.
func objdumpSymbol(t *testing.T, probe, symbol string) []instruction {
	t.Helper()
	pattern := "^" + regexp.QuoteMeta(symbol) + "$"
	out, err := exec.Command("go", "tool", "objdump", "-s", pattern, probe).CombinedOutput()
	require.NoError(t, err, string(out))

	var instructions []instruction
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.FieldsFunc(line, func(r rune) bool { return r == '\t' })
		if len(fields) < 4 {
			continue
		}

		text := strings.TrimSpace(fields[3])
		if text == "?" || strings.HasPrefix(text, "NO") {
			continue
		}

		instructions = append(instructions, instruction{
			source:   strings.TrimSpace(fields[0]),
			encoding: strings.TrimSpace(fields[2]),
			text:     text,
		})
	}

	require.NotEmpty(t, instructions, symbol)
	return instructions
}

// callTargets returns the target of every CALL in instructions.
func callTargets(instructions []instruction) []string {
	var targets []string
	for _, instruction := range instructions {
		if target, ok := strings.CutPrefix(instruction.text, "CALL "); ok {
			targets = append(targets, strings.TrimSuffix(target, "(SB)"))
		}
	}
	return targets
}

// fenceLoad matches the amd64 ReleaseFence load.
var fenceLoad = regexp.MustCompile(`^MOVL \S+/atomicbuffer\.fenceWord\(SB\), `)

// memoryStore matches a store of a register to memory.
var memoryStore = regexp.MustCompile(`^MOV[LQ] [A-Z][A-Z0-9]*, [^(]*\(`)

// requireReleaseStore asserts that instructions hold one release store with
// its fence:
//   - amd64: a plain store from release_amd64.go directly after a fenceWord
//     load. A store fence also has a fenceWord load directly after it.
//   - arm64: one STLR or STLRW. A store fence is an STLR whose next memory
//     access is an LDARW.
func requireReleaseStore(t *testing.T, goarch string, instructions []instruction, storeFence bool) {
	t.Helper()
	var stores []int
	for i, instruction := range instructions {
		isAMD64Store := strings.HasPrefix(instruction.source, "release_amd64.go:") &&
			memoryStore.MatchString(instruction.text) &&
			!strings.Contains(instruction.text, "(SP)")
		isARM64Store := strings.HasPrefix(instruction.text, "STLR")
		if (goarch == "amd64" && isAMD64Store) || (goarch == "arm64" && isARM64Store) {
			stores = append(stores, i)
		}
	}

	require.Len(t, stores, 1, "release stores")
	store := stores[0]
	if goarch == "amd64" {
		require.Regexp(t, fenceLoad, instructions[store-1].text, "fence before the store")
		if storeFence {
			require.Regexp(t, fenceLoad, instructions[store+1].text, "fence after the store")
		}
		return
	}

	if storeFence {
		next := store + 1
		for next < len(instructions)-1 && !strings.Contains(instructions[next].text, "(R") {
			next++
		}

		require.True(t, strings.HasPrefix(instructions[next].text, "LDARW "), instructions[next].text)
	}
}

func TestFenceARM64Barrier(t *testing.T) {
	t.Parallel()
	probe := buildProbe(t, "arm64", false)

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
			var encodings []string
			for _, instruction := range objdumpSymbol(t, probe, tt.symbol+".abi0") {
				encodings = append(encodings, instruction.encoding)
			}

			require.Equal(t, tt.want, encodings)
		})
	}
}

// TestReleaseStoreCodegen asserts that each release store inlines into its
// probe wrapper with the fence the ordering argument needs. See
// requireReleaseStore.
//
// Race builds keep sync/atomic stores so the race detector sees them.
func TestReleaseStoreCodegen(t *testing.T) {
	t.Parallel()

	const atomicbufferPrefix = "github.com/warrenguy/heron/atomicbuffer."
	wrappers := []struct {
		symbol     string
		storeFence bool

		// raceSymbol is the primitive behind the wrapper. raceStore is the
		// sync/atomic store it calls in a race build.
		raceSymbol string
		raceStore  string
	}{
		{
			symbol:     "main.putInt32Release",
			raceSymbol: atomicbufferPrefix + "AtomicBuffer.PutInt32Release",
			raceStore:  "sync/atomic.StoreInt32",
		},
		{
			symbol:     "main.putInt64Release",
			raceSymbol: atomicbufferPrefix + "AtomicBuffer.PutInt64Release",
			raceStore:  "sync/atomic.StoreInt64",
		},
		{
			symbol:     "main.storeInt64Release",
			raceSymbol: atomicbufferPrefix + "StoreInt64Release",
			raceStore:  "sync/atomic.StoreInt64",
		},
		{
			symbol:     "main.putInt64ReleaseStoreFenceAt",
			storeFence: true,
			raceSymbol: atomicbufferPrefix + "AtomicBuffer.PutInt64ReleaseStoreFenceAt",
			raceStore:  "sync/atomic.StoreInt64",
		},
	}

	for _, goarch := range []string{"amd64", "arm64"} {
		t.Run(goarch, func(t *testing.T) {
			probe := buildProbe(t, goarch, false)
			for _, wrapper := range wrappers {
				instructions := objdumpSymbol(t, probe, wrapper.symbol)
				requireReleaseStore(t, goarch, instructions, wrapper.storeFence)

				for _, target := range callTargets(instructions) {
					isRuntime := strings.HasPrefix(target, "runtime.")
					require.True(t, isRuntime, "%s calls %s", wrapper.symbol, target)
				}
			}
		})
	}

	t.Run("race", func(t *testing.T) {
		if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
			t.Skip("race probe: host is not linux/amd64")
		}

		cgo, err := exec.Command("go", "env", "CGO_ENABLED").Output()
		require.NoError(t, err)
		if strings.TrimSpace(string(cgo)) != "1" {
			t.Skip("race probe: cgo disabled")
		}

		cc, err := exec.Command("go", "env", "CC").Output()
		require.NoError(t, err)
		if _, err := exec.LookPath(strings.TrimSpace(string(cc))); err != nil {
			t.Skip("race probe: no C compiler")
		}

		probe := buildProbe(t, "amd64", true)
		for _, wrapper := range wrappers {
			// The primitive may inline into the wrapper. Otherwise the
			// wrapper calls it.
			targets := callTargets(objdumpSymbol(t, probe, wrapper.symbol))
			if !slices.Contains(targets, wrapper.raceStore) {
				require.Contains(t, targets, wrapper.raceSymbol, wrapper.symbol)
				targets = callTargets(objdumpSymbol(t, probe, wrapper.raceSymbol))
			}

			require.Contains(t, targets, wrapper.raceStore, wrapper.symbol)
		}
	})
}
