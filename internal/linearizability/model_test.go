package linearizability

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
)

const testCheckTimeout = 10 * time.Second

func testToken(index int) string {
	return strconv.Itoa(index) + ","
}

func getOp(key, want string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		Input:  Input{Kind: OpGet, Key: key},
		Call:   call,
		Output: Output{Value: want},
		Return: ret,
	}
}

func putOp(key, value string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		Input:  Input{Kind: OpPut, Key: key, Value: value},
		Call:   call,
		Output: Output{},
		Return: ret,
	}
}

func appendOp(key, value, want string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		Input:  Input{Kind: OpAppend, Key: key, Value: value},
		Call:   call,
		Output: Output{Value: want},
		Return: ret,
	}
}

func pendingOp(kind OpKind, key, value string, call int64) porcupine.Operation {
	return porcupine.Operation{
		Input:  Input{Kind: kind, Key: key, Value: value},
		Call:   call,
		Output: Output{Unknown: true},
		Return: math.MaxInt64,
	}
}

func TestModel_checksHistories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ops  []porcupine.Operation
		want porcupine.CheckResult
	}{
		{
			name: "sequential appends then a get",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				appendOp("k0", "2,", "1,2,", 20, 30),
				getOp("k0", "1,2,", 40, 50),
			},
			want: porcupine.Ok,
		},
		{
			name: "concurrent appends, get sees call order",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 20),
				appendOp("k0", "2,", "1,2,", 5, 25),
				getOp("k0", "1,2,", 30, 40),
			},
			want: porcupine.Ok,
		},
		{
			name: "concurrent appends, get sees reverse order",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "2,1,", 0, 20),
				appendOp("k0", "2,", "2,", 5, 25),
				getOp("k0", "2,1,", 30, 40),
			},
			want: porcupine.Ok,
		},
		{
			name: "touching intervals count as concurrent",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "2,1,", 0, 10),
				appendOp("k0", "2,", "2,", 10, 20),
			},
			want: porcupine.Ok,
		},
		{
			name: "abandoned append that a later get sees",
			ops: []porcupine.Operation{
				pendingOp(OpAppend, "k0", "1,", 0),
				getOp("k0", "1,", 10, 20),
			},
			want: porcupine.Ok,
		},
		{
			name: "abandoned append that a later get does not see",
			ops: []porcupine.Operation{
				pendingOp(OpAppend, "k0", "1,", 0),
				getOp("k0", "", 10, 20),
			},
			want: porcupine.Ok,
		},
		{
			name: "abandoned put that a later get sees",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				pendingOp(OpPut, "k0", "2,", 20),
				getOp("k0", "2,", 30, 40),
			},
			want: porcupine.Ok,
		},
		{
			name: "each key keeps its own value",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				appendOp("k1", "2,", "2,", 20, 30),
				getOp("k0", "1,", 40, 50),
				getOp("k1", "2,", 60, 70),
			},
			want: porcupine.Ok,
		},
		{
			name: "stale read after a returned append",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				getOp("k0", "", 20, 30),
			},
			want: porcupine.Illegal,
		},
		{
			name: "lost put",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				putOp("k0", "2,", 20, 30),
				getOp("k0", "1,", 40, 50),
			},
			want: porcupine.Illegal,
		},
		{
			name: "duplicate append",
			ops: []porcupine.Operation{
				appendOp("k0", "5,", "5,", 0, 10),
				getOp("k0", "5,5,", 20, 30),
			},
			want: porcupine.Illegal,
		},
		{
			name: "appends seen against real-time order",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "2,1,", 0, 10),
				appendOp("k0", "2,", "2,", 20, 30),
			},
			want: porcupine.Illegal,
		},
		{
			name: "read of a value that the writes cannot produce",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				getOp("k0", "7,", 20, 30),
			},
			want: porcupine.Illegal,
		},
		{
			name: "abandoned append seen before its call",
			ops: []porcupine.Operation{
				getOp("k0", "1,", 0, 10),
				pendingOp(OpAppend, "k0", "1,", 20),
			},
			want: porcupine.Illegal,
		},
		{
			name: "violation on one key beside a clean key",
			ops: []porcupine.Operation{
				appendOp("k0", "1,", "1,", 0, 10),
				getOp("k0", "1,", 20, 30),
				appendOp("k1", "2,", "2,", 0, 10),
				getOp("k1", "", 20, 30),
			},
			want: porcupine.Illegal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, Check(test.ops, testCheckTimeout))
		})
	}
}

func TestModel_describesOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  Input
		output Output
		want   string
	}{
		{
			name:   "get",
			input:  Input{Kind: OpGet, Key: "k1"},
			output: Output{Value: "3,7,"},
			want:   "get(k1) -> 3,7,",
		},
		{
			name:   "append",
			input:  Input{Kind: OpAppend, Key: "k1", Value: "9,"},
			output: Output{Value: "3,7,9,"},
			want:   "append(k1, 9,) -> 3,7,9,",
		},
		{
			name:  "put",
			input: Input{Kind: OpPut, Key: "k1", Value: "9,"},
			want:  "put(k1, 9,)",
		},
		{
			name:   "unknown append",
			input:  Input{Kind: OpAppend, Key: "k1", Value: "9,"},
			output: Output{Unknown: true},
			want:   "append(k1, 9,) -> ?",
		},
		{
			name:   "unknown put",
			input:  Input{Kind: OpPut, Key: "k1", Value: "9,"},
			output: Output{Unknown: true},
			want:   "put(k1, 9,) -> ?",
		},
	}

	model := Model()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, model.DescribeOperation(test.input, test.output))
		})
	}

	require.Equal(t, "term 4", model.DescribeOperationMetadata(int64(4)))
	require.Empty(t, model.DescribeOperationMetadata(nil))
}
