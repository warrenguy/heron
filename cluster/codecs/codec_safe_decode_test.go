package codecs_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func requireFastSlowDecodeParity[T any](
	tb require.TestingT,
	buf []byte,
	offset int,
	maxVarLen int,
	templateID uint16,
	tryFast func([]byte, int) (T, bool),
) {
	fast, fastOK := tryFast(buf, offset)
	slow, slowOK := codecs.TryDecodeByTemplate(buf, offset, maxVarLen)
	slowMatch := slowOK && slow.TemplateID == templateID
	require.Equal(tb, fastOK, slowMatch)
	if fastOK {
		requireDecodedFieldsEqual[T](tb, fast, slow.Fields)
	}
}

// fastSlowDecodeParities holds registered typed TryDecode*At parity helpers.
// The fuzz target walks this slice. Append one entry when a typed decoder
// joins the registry. The slice does not list every package TryDecode*At.
var fastSlowDecodeParities = []func(tb require.TestingT, buf []byte, offset int){
	requireSessionMessageHeaderFastSlowParity,
	requireSessionConnectRequestFastSlowParity,
	requireChallengeResponseFastSlowParity,
	requireSessionCloseRequestFastSlowParity,
	requireSessionKeepAliveFastSlowParity,
	requireSnapshotMarkerFastSlowParity,
	requireClientSessionFastSlowParity,
	requireTimerFastSlowParity,
	requireConsensusModuleFastSlowParity,
	requireClusterSessionFastSlowParity,
	requirePendingMessageTrackerFastSlowParity,
}

func requireSessionMessageHeaderFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		codecTestMaxVarLen,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		codecs.TryDecodeSessionMessageHeaderAt,
	)
}

func encodeSessionMessageHeaderAt(
	tb require.TestingT,
	offset int,
	actingVersion uint16,
	fields codecs.SessionMessageHeaderFields,
) []byte {
	buf := make([]byte, offset+fuzzClusterCodecMaxLen)
	wireEnd, ok := codecs.EncodeSessionMessageHeader(buf, offset, actingVersion, fields)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func TestSessionMessageHeader_fastSlowDecodeParity_table(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionMessageHeaderSbeSchemaVersion)
	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	}
	validWire := encodeSessionMessageHeaderAt(t, 0, actingVersion, fields)

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "valid", buf: validWire},
		{name: "truncated_header", buf: validWire[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: validWire[:len(validWire)-1]},
		{
			name: "unknown_template_id",
			buf: cloneMutatedWire(validWire, func(w []byte) {
				w[2], w[3] = 0xFF, 0x7F
			}),
		},
		{
			name: "schema_mismatch",
			buf: cloneMutatedWire(validWire, func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "offset_out_of_bounds", buf: validWire, offset: 10_000},
		{name: "offset_near_maxint", buf: validWire, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireSessionMessageHeaderFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}

func requireSessionConnectRequestFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		codecs.ClusterDecodeMaxVarLen,
		clusterwire.SessionConnectRequestSbeTemplateID,
		codecs.TryDecodeSessionConnectRequestAt,
	)
}

func TestTryDecodeSessionConnectRequestAt_roundtrip(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	fields := codecs.SessionConnectRequestFields{
		CorrelationID:      42,
		ResponseStreamID:   102,
		Version:            7,
		ResponseChannel:    []byte("aeron:udp?endpoint=127.0.0.1:40123"),
		EncodedCredentials: []byte{1, 2, 3, 4},
		ClientInfo:         []byte("heron-client"),
	}
	wire := encodeSessionConnectRequestWire(t, actingVersion, fields)

	got, ok := codecs.TryDecodeSessionConnectRequestAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, fields, got)

	// Var slices must be retainable copies, not views into wire. Mutate every
	// wire byte after decode. Independent expected clones stay equal.
	wantChannel := bytes.Clone(fields.ResponseChannel)
	wantCreds := bytes.Clone(fields.EncodedCredentials)
	wantInfo := bytes.Clone(fields.ClientInfo)
	for i := range wire {
		wire[i] ^= 0xFF
	}
	require.Equal(t, wantChannel, got.ResponseChannel)
	require.Equal(t, wantCreds, got.EncodedCredentials)
	require.Equal(t, wantInfo, got.ClientInfo)
}

func TestTryDecodeSessionConnectRequestAt_versionGates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		actingVersion uint16
		fields        codecs.SessionConnectRequestFields
		wantClient    []byte
	}{
		{
			name:          "clientInfo_nil_pre_v14",
			actingVersion: codecs.SessionConnectClientInfoMinVersion - 1,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      4,
				ResponseStreamID:   103,
				Version:            2,
				ResponseChannel:    []byte("aeron:ipc"),
				EncodedCredentials: []byte{1, 2, 3},
				ClientInfo:         []byte("must-drop"),
			},
			wantClient: nil,
		},
		{
			name:          "clientInfo_present_v14",
			actingVersion: codecs.SessionConnectClientInfoMinVersion,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      5,
				ResponseStreamID:   104,
				Version:            4,
				ResponseChannel:    []byte("aeron:udp"),
				EncodedCredentials: []byte{9, 8, 7},
				ClientInfo:         []byte("heron-client"),
			},
			wantClient: []byte("heron-client"),
		},
		{
			name:          "version_field_absent_pre_v2",
			actingVersion: codecs.SessionConnectVersionMinVersion - 1,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:    3,
				ResponseStreamID: 102,
				Version:          99,
			},
			wantClient: nil,
		},
		{
			name:          "version_field_present_v2",
			actingVersion: codecs.SessionConnectVersionMinVersion,
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      20,
				ResponseStreamID:   21,
				Version:            1,
				ResponseChannel:    []byte("rc"),
				EncodedCredentials: nil,
			},
			wantClient: nil,
		},
		{
			name:          "all_fields_at_schema_version",
			actingVersion: uint16(clusterwire.SessionConnectRequestSbeSchemaVersion),
			fields: codecs.SessionConnectRequestFields{
				CorrelationID:      10,
				ResponseStreamID:   11,
				Version:            12,
				ResponseChannel:    []byte("channel"),
				EncodedCredentials: []byte{0xAA, 0xBB},
				ClientInfo:         []byte("client"),
			},
			wantClient: []byte("client"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wire := encodeSessionConnectRequestWire(t, tc.actingVersion, tc.fields)
			got, ok := codecs.TryDecodeSessionConnectRequestAt(wire, 0)
			require.True(t, ok)

			// Encode drops ClientInfo below v14. Version field absent below v2.
			// ResponseChannel and credentials always round-trip at acting version.
			want := tc.fields
			want.ClientInfo = tc.wantClient
			if tc.actingVersion < codecs.SessionConnectVersionMinVersion {
				want.Version = 0
			}
			require.Equal(t, want, got)
			requireSessionConnectRequestFastSlowParity(t, wire, 0)
		})
	}
}

func TestTryDecodeSessionConnectRequestAt_reject(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	valid := encodeSessionConnectRequestWire(t, actingVersion, codecs.SessionConnectRequestFields{
		CorrelationID:      1,
		ResponseStreamID:   2,
		Version:            3,
		ResponseChannel:    []byte("ch"),
		EncodedCredentials: []byte{1},
		ClientInfo:         []byte("info"),
	})

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "truncated_header", buf: valid[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: valid[:len(valid)-1]},
		{
			name: "wrong_template",
			buf: cloneMutatedWire(valid, func(w []byte) {
				binary.LittleEndian.PutUint16(
					w[2:],
					clusterwire.SessionKeepAliveSbeTemplateID,
				)
			}),
		},
		{
			name: "wrong_schema",
			buf: cloneMutatedWire(valid, func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "negative_offset", buf: valid, offset: -1},
		{name: "offset_out_of_bounds", buf: valid, offset: 10_000},
		{name: "offset_near_maxint", buf: valid, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := codecs.TryDecodeSessionConnectRequestAt(tc.buf, tc.offset)
			require.False(t, ok)
			requireSessionConnectRequestFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}

func TestTryDecodeSessionConnectRequestAt_varOverCap(t *testing.T) {
	t.Parallel()

	// Header and root block, then a first var length over ClusterDecodeMaxVarLen.
	// The buffer holds the claimed payload so ReadVarSlice hits the maxLen path,
	// not the length-vs-remaining bounds path.
	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	blockLen := clusterwire.SessionConnectRequestSbeBlockLength
	headerAndBlock := messageHeaderLen + int(blockLen)
	overLen := codecs.ClusterDecodeMaxVarLen + 1
	buf := make([]byte, headerAndBlock+codecs.VarLengthPrefixSize+overLen)
	binary.LittleEndian.PutUint16(buf[0:], blockLen)
	binary.LittleEndian.PutUint16(buf[2:], clusterwire.SessionConnectRequestSbeTemplateID)
	binary.LittleEndian.PutUint16(buf[4:], clusterwire.SessionConnectRequestSbeSchemaID)
	binary.LittleEndian.PutUint16(buf[6:], actingVersion)
	binary.LittleEndian.PutUint32(buf[headerAndBlock:], uint32(overLen))

	_, ok := codecs.TryDecodeSessionConnectRequestAt(buf, 0)
	require.False(t, ok)
	requireSessionConnectRequestFastSlowParity(t, buf, 0)
}

func requireChallengeResponseFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		codecs.ClusterDecodeMaxVarLen,
		clusterwire.ChallengeResponseSbeTemplateID,
		codecs.TryDecodeChallengeResponseAt,
	)
}

func TestTryDecodeChallengeResponseAt_roundtrip(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.ChallengeResponseSbeSchemaVersion)
	fields := codecs.ChallengeResponseFields{
		CorrelationID:      42,
		ClusterSessionID:   7,
		EncodedCredentials: []byte{1, 2, 3, 4},
	}
	wire := encodeChallengeResponseWire(t, actingVersion, fields)

	got, ok := codecs.TryDecodeChallengeResponseAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, fields, got)

	// Var slices must be retainable copies, not views into wire. Mutate every
	// wire byte after decode. Independent expected clones stay equal.
	wantCreds := bytes.Clone(fields.EncodedCredentials)
	for i := range wire {
		wire[i] ^= 0xFF
	}
	require.Equal(t, wantCreds, got.EncodedCredentials)
}

func TestTryDecodeChallengeResponseAt_reject(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.ChallengeResponseSbeSchemaVersion)
	valid := encodeChallengeResponseWire(t, actingVersion, codecs.ChallengeResponseFields{
		CorrelationID:      1,
		ClusterSessionID:   2,
		EncodedCredentials: []byte{0xAA},
	})

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "truncated_header", buf: valid[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: valid[:len(valid)-1]},
		{
			name: "wrong_template",
			buf: cloneMutatedWire(valid, func(w []byte) {
				binary.LittleEndian.PutUint16(
					w[2:],
					clusterwire.SessionKeepAliveSbeTemplateID,
				)
			}),
		},
		{
			name: "wrong_schema",
			buf: cloneMutatedWire(valid, func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "negative_offset", buf: valid, offset: -1},
		{name: "offset_out_of_bounds", buf: valid, offset: 10_000},
		{name: "offset_near_maxint", buf: valid, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := codecs.TryDecodeChallengeResponseAt(tc.buf, tc.offset)
			require.False(t, ok)
			requireChallengeResponseFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}

func TestTryDecodeChallengeResponseAt_varOverCap(t *testing.T) {
	t.Parallel()

	// Header and root block, then a var length over ClusterDecodeMaxVarLen.
	// The buffer holds the claimed payload so ReadVarSlice hits the maxLen path,
	// not the length-vs-remaining bounds path.
	actingVersion := uint16(clusterwire.ChallengeResponseSbeSchemaVersion)
	blockLen := clusterwire.ChallengeResponseSbeBlockLength
	headerAndBlock := messageHeaderLen + int(blockLen)
	overLen := codecs.ClusterDecodeMaxVarLen + 1
	buf := make([]byte, headerAndBlock+codecs.VarLengthPrefixSize+overLen)
	binary.LittleEndian.PutUint16(buf[0:], blockLen)
	binary.LittleEndian.PutUint16(buf[2:], clusterwire.ChallengeResponseSbeTemplateID)
	binary.LittleEndian.PutUint16(buf[4:], clusterwire.ChallengeResponseSbeSchemaID)
	binary.LittleEndian.PutUint16(buf[6:], actingVersion)
	binary.LittleEndian.PutUint32(buf[headerAndBlock:], uint32(overLen))

	_, ok := codecs.TryDecodeChallengeResponseAt(buf, 0)
	require.False(t, ok)
	requireChallengeResponseFastSlowParity(t, buf, 0)
}

func requireSessionCloseRequestFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.SessionCloseRequestSbeTemplateID,
		codecs.TryDecodeSessionCloseRequestAt,
	)
}

func encodeSessionCloseRequestWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.SessionCloseRequestFields,
) []byte {
	return encodeClusterWire(t, func(buf []byte) (wireEnd int, ok bool) {
		return codecs.EncodeSessionCloseRequest(buf, 0, actingVersion, fields)
	})
}

func TestTryDecodeSessionCloseRequestAt_roundtrip(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionCloseRequestSbeSchemaVersion)
	fields := codecs.SessionCloseRequestFields{
		LeadershipTermID: 11,
		ClusterSessionID: 22,
	}
	wire := encodeSessionCloseRequestWire(t, actingVersion, fields)
	got, ok := codecs.TryDecodeSessionCloseRequestAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, fields, got)
	requireSessionCloseRequestFastSlowParity(t, wire, 0)
}

func TestTryDecodeSessionCloseRequestAt_reject(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionCloseRequestSbeSchemaVersion)
	valid := encodeSessionCloseRequestWire(t, actingVersion, codecs.SessionCloseRequestFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
	})

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "truncated_header", buf: valid[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: valid[:len(valid)-1]},
		{
			name: "wrong_template",
			buf: cloneMutatedWire(valid, func(w []byte) {
				binary.LittleEndian.PutUint16(
					w[2:],
					clusterwire.SessionKeepAliveSbeTemplateID,
				)
			}),
		},
		{
			name: "wrong_schema",
			buf: cloneMutatedWire(valid, func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "negative_offset", buf: valid, offset: -1},
		{name: "offset_out_of_bounds", buf: valid, offset: 10_000},
		{name: "offset_near_maxint", buf: valid, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := codecs.TryDecodeSessionCloseRequestAt(tc.buf, tc.offset)
			require.False(t, ok)
			requireSessionCloseRequestFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}

func requireSessionKeepAliveFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.SessionKeepAliveSbeTemplateID,
		codecs.TryDecodeSessionKeepAliveAt,
	)
}

func encodeSessionKeepAliveWire(
	t *testing.T,
	actingVersion uint16,
	fields codecs.SessionKeepAliveFields,
) []byte {
	return encodeClusterWire(t, func(buf []byte) (wireEnd int, ok bool) {
		return codecs.EncodeSessionKeepAlive(buf, 0, actingVersion, fields)
	})
}

func TestTryDecodeSessionKeepAliveAt_roundtrip(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: 33,
		ClusterSessionID: 44,
	}
	wire := encodeSessionKeepAliveWire(t, actingVersion, fields)
	got, ok := codecs.TryDecodeSessionKeepAliveAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, fields, got)
	requireSessionKeepAliveFastSlowParity(t, wire, 0)
}

func TestTryDecodeSessionKeepAliveAt_reject(t *testing.T) {
	t.Parallel()

	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)
	valid := encodeSessionKeepAliveWire(t, actingVersion, codecs.SessionKeepAliveFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
	})

	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "truncated_header", buf: valid[:messageHeaderLen-1]},
		{name: "one_byte_short", buf: valid[:len(valid)-1]},
		{
			name: "wrong_template",
			buf: cloneMutatedWire(valid, func(w []byte) {
				binary.LittleEndian.PutUint16(
					w[2:],
					clusterwire.SessionCloseRequestSbeTemplateID,
				)
			}),
		},
		{
			name: "wrong_schema",
			buf: cloneMutatedWire(valid, func(w []byte) {
				w[4], w[5] = 0, 0
			}),
		},
		{name: "negative_offset", buf: valid, offset: -1},
		{name: "offset_out_of_bounds", buf: valid, offset: 10_000},
		{name: "offset_near_maxint", buf: valid, offset: math.MaxInt},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ok := codecs.TryDecodeSessionKeepAliveAt(tc.buf, tc.offset)
			require.False(t, ok)
			requireSessionKeepAliveFastSlowParity(t, tc.buf, tc.offset)
		})
	}
}

func requireSnapshotMarkerFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.SnapshotMarkerSbeTemplateID,
		codecs.TryDecodeSnapshotMarkerAt,
	)
}

func requireClientSessionFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		codecs.ClusterDecodeMaxVarLen,
		clusterwire.ClientSessionSbeTemplateID,
		codecs.TryDecodeClientSessionAt,
	)
}

func requireTimerFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.TimerSbeTemplateID,
		codecs.TryDecodeTimerAt,
	)
}

func requireConsensusModuleFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.ConsensusModuleSbeTemplateID,
		codecs.TryDecodeConsensusModuleAt,
	)
}

func requireClusterSessionFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		codecs.ClusterDecodeMaxVarLen,
		clusterwire.ClusterSessionSbeTemplateID,
		codecs.TryDecodeClusterSessionAt,
	)
}

func requirePendingMessageTrackerFastSlowParity(tb require.TestingT, buf []byte, offset int) {
	requireFastSlowDecodeParity(
		tb,
		buf,
		offset,
		0,
		clusterwire.PendingMessageTrackerSbeTemplateID,
		codecs.TryDecodePendingMessageTrackerAt,
	)
}
