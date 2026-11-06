//go:build rapid

package linearizability

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const (
	bruteForceMaxOperations = 7
	bruteForceMaxKeys       = 2
	bruteForceMaxClients    = 3

	serializedMaxOperations = 200
	serializedMaxKeys       = 8

	serializedPointSpacing = 10

	// serializedMaxReach caps how far an interval extends from its linearization
	// point. The cap limits the overlap the search explores.
	serializedMaxReach = 25
)

func testKey(index int) string {
	return "k" + strconv.Itoa(index)
}

func drawInput(rt *rapid.T, index, keyCount int) Input {
	rt.Helper()

	in := Input{
		Kind: rapid.SampledFrom([]OpKind{OpGet, OpPut, OpAppend}).Draw(rt, "opKind"),
		Key:  testKey(rapid.IntRange(0, keyCount-1).Draw(rt, "opKey")),
	}
	if in.Kind != OpGet {
		in.Value = testToken(index)
	}
	return in
}

// applyToReference runs one operation on a reference map. It does not use
// Store or the model.
func applyToReference(values map[string]string, in Input) string {
	switch in.Kind {
	case OpGet:
		return values[in.Key]
	case OpPut:
		values[in.Key] = in.Value
		return ""
	case OpAppend:
		values[in.Key] += in.Value
		return values[in.Key]
	default:
		panic("unknown kind in reference run")
	}
}

// drawSmallHistory draws per-client intervals, then takes outputs from a
// reference run. A run in call order gives a linearizable history. A drawn
// permutation or changed output can break linearizability.
func drawSmallHistory(rt *rapid.T) []porcupine.Operation {
	rt.Helper()

	count := rapid.IntRange(1, bruteForceMaxOperations).Draw(rt, "count")
	keyCount := rapid.IntRange(1, bruteForceMaxKeys).Draw(rt, "keyCount")
	clientCount := rapid.IntRange(1, bruteForceMaxClients).Draw(rt, "clientCount")

	clocks := make([]int64, clientCount)
	ops := make([]porcupine.Operation, count)
	for i := range ops {
		client := rapid.IntRange(0, clientCount-1).Draw(rt, "client")
		in := drawInput(rt, i, keyCount)
		call := clocks[client] + int64(rapid.IntRange(0, 4).Draw(rt, "callGap"))
		ops[i] = porcupine.Operation{ClientId: client, Input: in, Call: call}

		abandoned := in.Kind != OpGet && rapid.IntRange(0, 3).Draw(rt, "abandoned") == 0
		if abandoned {
			ops[i].Output = Output{Unknown: true}
			ops[i].Return = math.MaxInt64
			clocks[client] = call
			continue
		}
		ops[i].Return = call + int64(rapid.IntRange(0, 4).Draw(rt, "length"))
		clocks[client] = ops[i].Return + 1
	}

	order := make([]int, count)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return cmp.Compare(ops[a].Call, ops[b].Call)
	})
	if rapid.Bool().Draw(rt, "permute") {
		order = rapid.Permutation(order).Draw(rt, "order")
	}

	values := make(map[string]string)
	for _, i := range order {
		in := ops[i].Input.(Input)
		if ops[i].Return == math.MaxInt64 {
			if rapid.Bool().Draw(rt, "applyAbandoned") {
				applyToReference(values, in)
			}
			continue
		}
		output := applyToReference(values, in)
		if rapid.IntRange(0, 7).Draw(rt, "changeOutput") == 0 {
			output = rapid.SampledFrom([]string{"", "x", testToken(i)}).Draw(rt, "changedOutput")
		}
		ops[i].Output = Output{Value: output}
	}
	return ops
}

// isLinearizableByBruteForce tries every order that keeps real-time order. It
// places or omits each pending write. It must place every returned operation.
func isLinearizableByBruteForce(ops []porcupine.Operation) bool {
	values := make(map[string]string)
	placed := make([]bool, len(ops))

	canPlace := func(candidate int) bool {
		for other := range ops {
			if !placed[other] && ops[other].Return < ops[candidate].Call {
				return false
			}
		}
		return true
	}

	hasPlacedAllReturned := func() bool {
		for i := range ops {
			if !placed[i] && ops[i].Return != math.MaxInt64 {
				return false
			}
		}
		return true
	}

	var place func() bool
	place = func() bool {
		if hasPlacedAllReturned() {
			return true
		}
		for i := range ops {
			if placed[i] || !canPlace(i) {
				continue
			}

			in := ops[i].Input.(Input)
			out := ops[i].Output.(Output)
			before := values[in.Key]
			reply := applyToReference(values, in)
			accepted := in.Kind == OpPut || out.Unknown || reply == out.Value
			if accepted {
				placed[i] = true
				if place() {
					return true
				}
				placed[i] = false
			}

			values[in.Key] = before
		}
		return false
	}
	return place()
}

func unpartitionedModel() porcupine.Model {
	keys := make([]string, bruteForceMaxKeys)
	for i := range keys {
		keys[i] = testKey(i)
	}

	return porcupine.Model{
		Init: func() any { return [bruteForceMaxKeys]string{} },
		Step: func(state, input, output any) (bool, any) {
			values := state.([bruteForceMaxKeys]string)
			index := slices.Index(keys, input.(Input).Key)
			accepted, next := stepKey(values[index], input, output)
			values[index] = next.(string)
			return accepted, values
		},
	}
}

func TestModel_matchesBruteForce_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		ops := drawSmallHistory(rt)

		want := porcupine.Illegal
		if isLinearizableByBruteForce(ops) {
			want = porcupine.Ok
		}

		got := Check(ops, testCheckTimeout)
		require.Equal(rt, want, got)

		unpartitioned := porcupine.CheckOperationsTimeout(unpartitionedModel(), ops, testCheckTimeout)
		require.Equal(rt, got, unpartitioned)
	})
}

func TestModel_acceptsSerializedRuns_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(1, serializedMaxOperations).Draw(rt, "count")
		keyCount := rapid.IntRange(1, serializedMaxKeys).Draw(rt, "keyCount")

		values := make(map[string]string)
		ops := make([]porcupine.Operation, 0, count+1)
		var lastReturn int64
		for i := range count {
			in := drawInput(rt, i, keyCount)
			point := int64(i+1)*serializedPointSpacing + serializedMaxReach
			returnTime := point + int64(rapid.IntRange(0, serializedMaxReach).Draw(rt, "after"))
			lastReturn = max(lastReturn, returnTime)
			ops = append(ops, porcupine.Operation{
				Input:  in,
				Call:   point - int64(rapid.IntRange(0, serializedMaxReach).Draw(rt, "before")),
				Output: Output{Value: applyToReference(values, in)},
				Return: returnTime,
			})
		}

		key := testKey(rapid.IntRange(0, keyCount-1).Draw(rt, "closingKey"))
		ops = append(ops, getOp(key, values[key], lastReturn+1, lastReturn+2))

		// Phase: serialized run.
		require.Equal(rt, porcupine.Ok, Check(ops, testCheckTimeout))

		// Phase: wrong closing get.
		ops[len(ops)-1].Output = Output{Value: "x"}
		require.Equal(rt, porcupine.Illegal, Check(ops, testCheckTimeout))
	})
}
