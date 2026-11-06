//go:build rapid

package linearizability

import (
	"slices"
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const storeMaxSteps = 100

func TestStore_matchesModel_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		keyGenerator := rapid.SliceOfN(rapid.Byte(), 0, 16)
		keys := rapid.SliceOfN(keyGenerator, 1, 4).Draw(rt, "keys")
		prefix := rapid.SliceOfN(rapid.Byte(), 0, 4).Draw(rt, "prefix")

		history := newTestHistory()
		store := NewStore()

		steps := rapid.IntRange(1, storeMaxSteps).Draw(rt, "steps")
		for i := range steps {
			in := Input{
				Kind: rapid.SampledFrom([]OpKind{OpGet, OpPut, OpAppend}).Draw(rt, "kind"),
				Key:  string(rapid.SampledFrom(keys).Draw(rt, "key")),
			}
			if in.Kind != OpGet {
				in.Value = testToken(i)
			}

			id := history.Call(0, in)
			opID := int64(id)

			request := EncodeRequest(slices.Clone(prefix), opID, in)
			requireRoundTrip(rt, prefix, request, DecodeRequest, opID, in)

			out := store.Apply(in)

			reply := EncodeReply(slices.Clone(prefix), opID, out)
			requireRoundTrip(rt, prefix, reply, DecodeReply, opID, out)

			require.NoError(rt, history.Return(id, out, 1))
		}

		require.Equal(rt, porcupine.Ok, Check(history.Operations(), testCheckTimeout))
	})
}

// requireRoundTrip asserts that encoded starts with prefix. decode must read
// the rest as wantID and want. decode must reject every truncated payload and a
// payload with a trailing byte.
func requireRoundTrip[T any](
	rt *rapid.T,
	prefix, encoded []byte,
	decode func([]byte) (int64, T, error),
	wantID int64,
	want T,
) {
	rt.Helper()

	require.Equal(rt, prefix, encoded[:len(prefix)])
	payload := encoded[len(prefix):]
	opID, value, err := decode(payload)
	require.NoError(rt, err)
	require.Equal(rt, wantID, opID)
	require.Equal(rt, want, value)
	for length := range len(payload) {
		_, _, err = decode(payload[:length])
		require.Error(rt, err)
	}
	_, _, err = decode(append(slices.Clone(payload), 0))
	require.Error(rt, err)
}
