package linearizability

import (
	"fmt"
	"hash/maphash"
	"strings"

	"github.com/anishathalye/porcupine"
)

var stateHashSeed = maphash.MakeSeed()

// Model returns the sequential key-value model, partitioned by key.
//
// Linearizability is local, so a history is linearizable exactly when each
// key's subhistory is. Each key's state is a string that starts empty.
//
// [porcupine.Model.Step] accepts these steps:
//   - get: the output value equals the state.
//   - put: always. The state becomes the input value.
//   - append: the state becomes the state plus the input value. The output is
//     unknown or its value equals the new state.
func Model() porcupine.Model {
	return porcupine.Model{
		Partition:                 partitionByKey,
		Init:                      func() any { return "" },
		Step:                      stepKey,
		Hash:                      hashValue,
		DescribeOperation:         describeOperation,
		DescribeOperationMetadata: describeMetadata,
	}
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	indexByKey := make(map[string]int)
	var partitions [][]porcupine.Operation
	for _, op := range history {
		key := op.Input.(Input).Key
		index, seen := indexByKey[key]
		if !seen {
			index = len(partitions)
			indexByKey[key] = index
			partitions = append(partitions, nil)
		}
		partitions[index] = append(partitions[index], op)
	}
	return partitions
}

func stepKey(state, input, output any) (bool, any) {
	value := state.(string)
	in := input.(Input)
	out := output.(Output)
	switch in.Kind {
	case OpGet:
		return out.Value == value, value
	case OpPut:
		return true, in.Value
	case OpAppend:
		if out.Unknown {
			return true, value + in.Value
		}

		// The search caches accepted states across retries of this step.
		// Compare in place, then reuse the output string.
		accepted := len(out.Value) == len(value)+len(in.Value) &&
			strings.HasPrefix(out.Value, value) &&
			strings.HasSuffix(out.Value, in.Value)
		return accepted, out.Value
	default:
		return false, value
	}
}

func hashValue(state any) uint64 {
	return maphash.String(stateHashSeed, state.(string))
}

func describeOperation(input, output any) string {
	in := input.(Input)
	out := output.(Output)
	call := fmt.Sprintf("%s(%s, %s)", in.Kind, in.Key, in.Value)
	if in.Kind == OpGet {
		call = fmt.Sprintf("%s(%s)", in.Kind, in.Key)
	}

	switch {
	case out.Unknown:
		return call + " -> ?"
	case in.Kind == OpPut:
		return call
	default:
		return call + " -> " + out.Value
	}
}

// describeMetadata formats the leadership term of a returned operation. A
// pending write has nil metadata.
func describeMetadata(info any) string {
	if info == nil {
		return ""
	}
	return fmt.Sprintf("term %d", info)
}
