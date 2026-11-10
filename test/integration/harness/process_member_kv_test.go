//go:build integration

package harness

import (
	"encoding/json"
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/service"
	"github.com/warrenguy/luas/internal/linearizability"
)

func TestApplyDigest_matchesAcrossReplay(t *testing.T) {
	requests := []linearizability.Input{
		{Kind: linearizability.OpPut, Key: "k0", Value: "ab"},
		{Kind: linearizability.OpAppend, Key: "k0", Value: "cd"},
		{Kind: linearizability.OpGet, Key: "k0"},
	}

	first := applyKeyValue(requests)
	replayed := applyKeyValue(requests)

	require.Equal(t, int64(len(requests)), first.appliedCount())
	require.Equal(t, first.applyDigest(), replayed.applyDigest())
	require.Equal(t, first.appliedCount(), replayed.appliedCount())

	other := applyKeyValue([]linearizability.Input{
		{Kind: linearizability.OpPut, Key: "k1", Value: "zz"},
	})
	require.NotEqual(t, first.applyDigest(), other.applyDigest())
}

func TestApplyDigest_coversPutContent(t *testing.T) {
	base := applyKeyValue([]linearizability.Input{
		{Kind: linearizability.OpPut, Key: "k0", Value: "ab"},
	})
	diverged := []linearizability.Input{
		{Kind: linearizability.OpPut, Key: "k9", Value: "ab"},
		{Kind: linearizability.OpPut, Key: "k0", Value: "zz"},
	}

	for _, in := range diverged {
		other := applyKeyValue([]linearizability.Input{in})
		require.NotEqual(t, base.applyDigest(), other.applyDigest(), "%+v", in)
	}
}

func TestKeyValueProcessService_keepsFirstDecodeError(t *testing.T) {
	valid := linearizability.EncodeRequest(nil, 7, linearizability.Input{
		Kind:  linearizability.OpPut,
		Key:   "k0",
		Value: "ab",
	})
	bad := []byte{1, 2, 3}
	later := []byte{9}
	_, _, wantErr := linearizability.DecodeRequest(bad)
	require.Error(t, wantErr)

	svc := applyRaw(valid, bad, later)
	ref := applyRaw(valid)

	require.Equal(t, ref.applyDigest(), svc.applyDigest())
	require.Equal(t, int64(1), svc.appliedCount())
	require.EqualError(t, svc.DecodeError(), wantErr.Error())

	session := &recordingSession{}
	get := linearizability.EncodeRequest(nil, 8, linearizability.Input{
		Kind: linearizability.OpGet,
		Key:  "k0",
	})
	svc.SessionMessage(session, 0, get)
	require.Len(t, session.got, 1)

	_, out, err := linearizability.DecodeReply(session.got[0])
	require.NoError(t, err)
	require.Equal(t, "ab", out.Value)
}

func applyKeyValue(requests []linearizability.Input) *keyValueProcessService {
	payloads := make([][]byte, len(requests))

	for i, in := range requests {
		payloads[i] = linearizability.EncodeRequest(nil, int64(i+1), in)
	}

	return applyRaw(payloads...)
}

func applyRaw(payloads ...[]byte) *keyValueProcessService {
	svc := newKeyValueProcessService(&statusSink{enc: json.NewEncoder(io.Discard)})

	for _, payload := range payloads {
		svc.SessionMessage(nil, 0, payload)
	}

	return svc
}

type recordingSession struct {
	service.ClientSession

	got [][]byte
}

func (s *recordingSession) Offer(payload []byte) int64 {
	s.got = append(s.got, slices.Clone(payload))
	return 1
}
