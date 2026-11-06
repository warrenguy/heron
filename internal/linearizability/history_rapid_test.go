//go:build rapid

package linearizability

import (
	"math"
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const (
	historyMaxSteps   = 80
	historyMaxClients = 3
)

type referenceOperation struct {
	in          Input
	call        int64
	output      Output
	returned    int64
	term        int64
	state       operationState
	lateReplied bool
}

func (r *referenceOperation) isInHistory() bool {
	return r.state == operationReturned || (r.state == operationAbandoned && r.in.Kind != OpGet)
}

func TestHistory_model_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		var now int64
		history := NewHistory(func() int64 {
			now++
			return now
		})

		var (
			reference    []*referenceOperation
			abandoned    []int
			returned     []int
			wantCounts   Counts
			openByClient = make(map[int]int)
		)

		steps := rapid.IntRange(1, historyMaxSteps).Draw(rt, "steps")
		for range steps {
			switch rapid.SampledFrom([]string{"advance", "late", "second", "unknown"}).Draw(rt, "step") {
			case "late":
				if len(abandoned) == 0 {
					continue
				}
				id := rapid.SampledFrom(abandoned).Draw(rt, "lateID")
				entry := reference[id]
				err := history.Return(id, Output{Value: "late"}, 99)
				if entry.lateReplied {
					require.ErrorContains(rt, err, "second reply")
				} else {
					require.NoError(rt, err)
					entry.lateReplied = true
					wantCounts.Late.increment(entry.in.Kind)
				}
			case "second":
				if len(returned) == 0 {
					continue
				}
				id := rapid.SampledFrom(returned).Draw(rt, "secondID")
				require.ErrorContains(rt, history.Return(id, Output{Value: "second"}, 99), "second reply")
			case "unknown":
				require.ErrorContains(rt, history.Return(len(reference), Output{}, 99), "unknown id")
			case "advance":
				client := rapid.IntRange(0, historyMaxClients-1).Draw(rt, "client")
				openID, open := openByClient[client]
				if !open {
					in := drawInput(rt, len(reference), 1)
					id := history.Call(client, in)
					require.Equal(rt, len(reference), id)
					reference = append(reference, &referenceOperation{
						in:    in,
						call:  now,
						state: operationOpen,
					})
					openByClient[client] = id
					continue
				}

				entry := reference[openID]
				delete(openByClient, client)
				switch rapid.SampledFrom([]string{"return", "discard", "abandon"}).Draw(rt, "end") {
				case "return":
					term := rapid.Int64Range(0, 9).Draw(rt, "term")
					output := Output{Value: rapid.StringN(0, 4, -1).Draw(rt, "value")}
					require.NoError(rt, history.Return(openID, output, term))
					entry.state = operationReturned
					entry.output = output
					entry.returned = now
					entry.term = term
					wantCounts.Returned.increment(entry.in.Kind)
					returned = append(returned, openID)
				case "discard":
					history.Discard(openID)
					entry.state = operationDiscarded
					wantCounts.Discarded.increment(entry.in.Kind)
				case "abandon":
					history.Abandon(openID)
					entry.state = operationAbandoned
					wantCounts.Abandoned.increment(entry.in.Kind)
					abandoned = append(abandoned, openID)
				}
			}
			require.Equal(rt, wantCounts, history.Counts())
		}

		for _, id := range openByClient {
			reference[id].state = operationAbandoned
			wantCounts.Abandoned.increment(reference[id].in.Kind)
		}

		ops := history.Operations()
		require.Equal(rt, wantCounts, history.Counts())
		requireOperationsMatch(rt, reference, ops)
		requireSequentialRows(rt, ops)
	})
}

// requireOperationsMatch asserts that ops holds exactly the returned operations
// and the pending writes, in call order.
func requireOperationsMatch(
	rt *rapid.T,
	reference []*referenceOperation,
	ops []porcupine.Operation,
) {
	rt.Helper()

	next := 0
	for _, entry := range reference {
		if !entry.isInHistory() {
			continue
		}
		require.Less(rt, next, len(ops))
		op := ops[next]
		next++

		require.Equal(rt, entry.in, op.Input)
		require.Equal(rt, entry.call, op.Call)
		if entry.state == operationReturned {
			require.Less(rt, op.Call, op.Return)
			require.Equal(rt, entry.returned, op.Return)
			require.Equal(rt, entry.output, op.Output)
			require.Equal(rt, entry.term, op.Metadata)
			continue
		}
		require.Equal(rt, int64(math.MaxInt64), op.Return)
		require.Equal(rt, Output{Unknown: true}, op.Output)
		require.Nil(rt, op.Metadata)
	}
	require.Len(rt, ops, next)
}

// requireSequentialRows asserts that the operations of each client id do not
// overlap. A pending write therefore ends its row.
func requireSequentialRows(rt *rapid.T, ops []porcupine.Operation) {
	rt.Helper()

	lastReturn := make(map[int]int64)
	for _, op := range ops {
		if previous, seen := lastReturn[op.ClientId]; seen {
			require.Less(rt, previous, op.Call)
		}
		lastReturn[op.ClientId] = op.Return
	}
}
