package linearizability

import (
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
)

// newTestHistory returns a history on a clock that advances by 10 per read.
func newTestHistory() *History {
	var now int64
	return NewHistory(func() int64 {
		now += 10
		return now
	})
}

func TestKindCounts_total(t *testing.T) {
	t.Parallel()

	require.Equal(t, 6, KindCounts{Get: 1, Put: 2, Append: 3}.Total())
}

func TestHistory_operations(t *testing.T) {
	t.Parallel()

	get := Input{Kind: OpGet, Key: "k0"}
	put := Input{Kind: OpPut, Key: "k0", Value: "1,"}
	appendInput := Input{Kind: OpAppend, Key: "k0", Value: "2,"}

	t.Run("discarded operation is absent", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		history.Discard(history.Call(0, appendInput))

		require.Empty(t, history.Operations())
		require.Equal(t, KindCounts{Append: 1}, history.Counts().Discarded)
	})

	t.Run("abandoned get is absent", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		history.Abandon(history.Call(0, get))

		require.Empty(t, history.Operations())
		require.Equal(t, KindCounts{Get: 1}, history.Counts().Abandoned)
	})

	t.Run("abandoned write goes pending", func(t *testing.T) {
		t.Parallel()

		for _, in := range []Input{put, appendInput} {
			history := newTestHistory()
			history.Abandon(history.Call(0, in))

			want := pendingOp(in.Kind, in.Key, in.Value, 10)
			require.Equal(t, []porcupine.Operation{want}, history.Operations())
		}
	})

	t.Run("returned operation carries its term", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		require.NoError(t, history.Return(history.Call(0, appendInput), Output{Value: "2,"}, 7))

		want := appendOp("k0", "2,", "2,", 10, 20)
		want.Metadata = int64(7)
		require.Equal(t, []porcupine.Operation{want}, history.Operations())
		require.Equal(t, KindCounts{Append: 1}, history.Counts().Returned)
	})

	t.Run("return ignores a late reply and counts it", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		id := history.Call(0, appendInput)
		history.Abandon(id)
		require.NoError(t, history.Return(id, Output{Value: "2,"}, 7))

		want := pendingOp(OpAppend, "k0", "2,", 10)
		require.Equal(t, []porcupine.Operation{want}, history.Operations())
		require.Equal(t, Counts{
			Abandoned: KindCounts{Append: 1},
			Late:      KindCounts{Append: 1},
		}, history.Counts())
	})

	t.Run("late reply to an abandoned get stays out of the history", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		id := history.Call(0, get)
		history.Abandon(id)
		require.NoError(t, history.Return(id, Output{Value: "2,"}, 7))

		require.Empty(t, history.Operations())
		require.Equal(t, KindCounts{Get: 1}, history.Counts().Late)
	})

	t.Run("return rejects a broken reply path", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()

		// Phase: unknown id.
		require.ErrorContains(t, history.Return(0, Output{}, 1), "unknown id")
		require.ErrorContains(t, history.Return(-1, Output{}, 1), "unknown id")

		// Phase: second reply.
		returned := history.Call(0, put)
		require.NoError(t, history.Return(returned, Output{}, 1))
		require.ErrorContains(t, history.Return(returned, Output{}, 2), "second reply")

		// Phase: second reply to an abandoned operation.
		abandoned := history.Call(0, get)
		history.Abandon(abandoned)
		require.NoError(t, history.Return(abandoned, Output{}, 2))
		require.ErrorContains(t, history.Return(abandoned, Output{}, 3), "second reply")
		require.Equal(t, KindCounts{Get: 1}, history.Counts().Late)

		// Phase: reply to a discarded operation.
		discarded := history.Call(0, put)
		history.Discard(discarded)
		require.ErrorContains(t, history.Return(discarded, Output{}, 2), "discarded")

		// Phase: only the returned operation stays.
		ops := history.Operations()
		require.Len(t, ops, 1)
		require.Equal(t, int64(1), ops[0].Metadata)
	})

	t.Run("call after an abandon takes a fresh client id", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		require.NoError(t, history.Return(history.Call(0, put), Output{}, 1))
		history.Abandon(history.Call(0, put))
		require.NoError(t, history.Return(history.Call(0, put), Output{}, 1))
		require.NoError(t, history.Return(history.Call(1, put), Output{}, 1))
		history.Abandon(history.Call(0, get))
		require.NoError(t, history.Return(history.Call(0, put), Output{}, 1))

		ops := history.Operations()
		require.Len(t, ops, 5)
		require.Equal(t, ops[0].ClientId, ops[1].ClientId)
		require.NotEqual(t, ops[1].ClientId, ops[2].ClientId)
		require.NotEqual(t, ops[2].ClientId, ops[3].ClientId)
		require.NotEqual(t, ops[1].ClientId, ops[3].ClientId)
		require.NotEqual(t, ops[2].ClientId, ops[4].ClientId)
		require.NotEqual(t, ops[3].ClientId, ops[4].ClientId)
	})

	t.Run("call after a discard keeps the client id", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		require.NoError(t, history.Return(history.Call(0, put), Output{}, 1))
		history.Discard(history.Call(0, put))
		require.NoError(t, history.Return(history.Call(0, put), Output{}, 1))

		ops := history.Operations()
		require.Len(t, ops, 2)
		require.Equal(t, ops[0].ClientId, ops[1].ClientId)
	})

	t.Run("operations abandons open operations", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		openWrite := history.Call(0, appendInput)
		history.Call(1, get)

		want := pendingOp(OpAppend, "k0", "2,", 10)
		require.Equal(t, []porcupine.Operation{want}, history.Operations())
		require.Equal(t, KindCounts{Get: 1, Append: 1}, history.Counts().Abandoned)

		require.NoError(t, history.Return(openWrite, Output{Value: "2,"}, 1))
		require.Equal(t, KindCounts{Append: 1}, history.Counts().Late)
	})

	t.Run("operations returns call order", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		first := history.Call(0, put)
		second := history.Call(1, appendInput)
		require.NoError(t, history.Return(second, Output{Value: "2,"}, 1))
		require.NoError(t, history.Return(first, Output{}, 1))

		ops := history.Operations()
		require.Len(t, ops, 2)
		require.Equal(t, put, ops[0].Input)
		require.Equal(t, appendInput, ops[1].Input)
	})

	t.Run("discard and abandon leave an ended operation unchanged", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()

		// Phase: a returned operation stays returned.
		id := history.Call(0, put)
		require.NoError(t, history.Return(id, Output{}, 1))
		history.Abandon(id)
		history.Discard(id)
		history.Abandon(id + 1)
		history.Discard(-1)

		ops := history.Operations()
		require.Len(t, ops, 1)
		require.Equal(t, int64(1), ops[0].Metadata)
		require.Equal(t, Counts{Returned: KindCounts{Put: 1}}, history.Counts())

		// Phase: an abandoned operation stays abandoned.
		abandoned := history.Call(0, put)
		history.Abandon(abandoned)
		history.Discard(abandoned)
		require.Equal(t, KindCounts{Put: 1}, history.Counts().Abandoned)
		require.Zero(t, history.Counts().Discarded.Total())
	})

	t.Run("history checks through the model", func(t *testing.T) {
		t.Parallel()

		history := newTestHistory()
		require.NoError(t, history.Return(history.Call(0, appendInput), Output{Value: "2,"}, 1))
		history.Abandon(history.Call(0, put))
		require.NoError(t, history.Return(history.Call(1, get), Output{Value: "1,"}, 1))

		require.Equal(t, porcupine.Ok, Check(history.Operations(), testCheckTimeout))
	})
}
