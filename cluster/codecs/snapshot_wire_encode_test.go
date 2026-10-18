package codecs_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func TestSnapshotMarker_roundtrip(t *testing.T) {
	currentVersion := clusterwire.SnapshotMarkerSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.SnapshotMarkerFields]{
		{name: "populated", fields: snapshotMarkerSampleFields()},
		{name: "zero", fields: codecs.SnapshotMarkerFields{}},
		{
			name: "null_enums",
			fields: codecs.SnapshotMarkerFields{
				TypeID: codecs.ServiceSnapshotTypeID, LogPosition: math.MinInt64,
				LeadershipTermID: math.MinInt64, Index: math.MinInt32,
				Mark:     clusterwire.SnapshotMark_NULL_VALUE,
				TimeUnit: clusterwire.ClusterTimeUnit_NULL_VALUE,
			},
		},
		{
			name: "max_int",
			fields: codecs.SnapshotMarkerFields{
				TypeID: math.MaxInt64, LogPosition: math.MaxInt64,
				LeadershipTermID: math.MaxInt64, Index: math.MaxInt32,
				Mark:       clusterwire.SnapshotMark_SECTION,
				TimeUnit:   clusterwire.ClusterTimeUnit_MICROS,
				AppVersion: math.MaxInt32,
			},
		},
		{
			name: "raw_mark_and_time_unit",
			fields: codecs.SnapshotMarkerFields{
				TypeID: 99, LogPosition: 1, LeadershipTermID: 2, Index: 3,
				Mark:       clusterwire.SnapshotMark(7),
				TimeUnit:   clusterwire.ClusterTimeUnit(9),
				AppVersion: 4,
			},
		},
		{name: "acting_version_3", actingVer: 3, fields: snapshotMarkerSampleFields()},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.SnapshotMarkerSbeTemplateID,
		currentVersion,
		buildActingFieldRoundtripCases(
			currentVersion,
			fieldCases,
			codecs.EncodeSnapshotMarker,
			codecs.SnapshotMarkerOracleFields,
		),
	)
}

func TestClientSession_roundtrip(t *testing.T) {
	v := clusterwire.ClientSessionSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.ClientSessionFields]{
		{name: "populated", fields: clientSessionSampleFields()},
		{name: "empty_vars", fields: codecs.ClientSessionFields{ClusterSessionID: 1}},
		{
			name: "binary_principal",
			fields: codecs.ClientSessionFields{
				ClusterSessionID: math.MaxInt64, ResponseStreamID: math.MinInt32,
				ResponseChannel:  []byte("aeron:udp?endpoint=localhost:40123"),
				EncodedPrincipal: []byte{0x00, 0xFF, 0x80, 0x7F},
			},
		},
	}
	runCodecFieldsActingRoundtrips(
		t, clusterwire.ClientSessionSbeTemplateID, v, fieldCases, codecs.EncodeClientSession,
	)
}

func TestTimer_roundtrip(t *testing.T) {
	v := clusterwire.TimerSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.TimerFields]{
		{name: "populated", fields: timerSampleFields()},
		{name: "zero", fields: codecs.TimerFields{}},
		{
			name:   "max_int",
			fields: codecs.TimerFields{CorrelationID: math.MaxInt64, Deadline: math.MaxInt64},
		},
	}
	runCodecFieldsActingRoundtrips(
		t, clusterwire.TimerSbeTemplateID, v, fieldCases, codecs.EncodeTimer,
	)
}

func TestConsensusModule_roundtrip(t *testing.T) {
	currentVersion := clusterwire.ConsensusModuleSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.ConsensusModuleFields]{
		{name: "populated", fields: consensusModuleSampleFields()},
		{name: "zero", fields: codecs.ConsensusModuleFields{}},
		{
			name: "null_ids",
			fields: codecs.ConsensusModuleFields{
				NextSessionID:        math.MinInt64,
				NextServiceSessionID: math.MinInt64,
				LogServiceSessionID:  math.MinInt64,
			},
		},
		{
			name: "max_int",
			fields: codecs.ConsensusModuleFields{
				NextSessionID:          math.MaxInt64,
				NextServiceSessionID:   math.MaxInt64,
				LogServiceSessionID:    math.MaxInt64,
				PendingMessageCapacity: math.MaxInt32,
			},
		},
		{name: "acting_version_2", actingVer: 2, fields: consensusModuleSampleFields()},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.ConsensusModuleSbeTemplateID,
		currentVersion,
		buildActingFieldRoundtripCases(
			currentVersion,
			fieldCases,
			codecs.EncodeConsensusModule,
			codecs.ConsensusModuleOracleFields,
		),
	)
}

// At the schema version, encode writes every optional field even when 0. A
// claimed buffer may hold residual bytes.
func TestEncodeSnapshotMarker_dirtyBufferOptionalFieldsZero(t *testing.T) {
	actingVersion := uint16(clusterwire.SnapshotMarkerSbeSchemaVersion)
	fields := snapshotMarkerSampleFields()
	fields.TimeUnit = clusterwire.ClusterTimeUnit_MILLIS
	fields.AppVersion = 0

	buf := bytes.Repeat([]byte{0xFF}, codecs.SnapshotMarkerEncodedLen())
	wireEnd, ok := codecs.EncodeSnapshotMarker(buf, 0, actingVersion, fields)
	require.True(t, ok)
	require.Equal(t, codecs.SnapshotMarkerEncodedLen(), wireEnd)

	got, ok := codecs.TryDecodeSnapshotMarkerAt(buf, 0)
	require.True(t, ok)
	require.Equal(t, clusterwire.ClusterTimeUnit_MILLIS, got.TimeUnit)
	require.Equal(t, int32(0), got.AppVersion)
	require.Equal(t, fields, got)
}

func TestEncodeConsensusModule_dirtyBufferOptionalFieldsZero(t *testing.T) {
	actingVersion := uint16(clusterwire.ConsensusModuleSbeSchemaVersion)
	fields := codecs.ConsensusModuleFields{NextSessionID: 11}

	buf := bytes.Repeat([]byte{0xFF}, codecs.ConsensusModuleEncodedLen())
	wireEnd, ok := codecs.EncodeConsensusModule(buf, 0, actingVersion, fields)
	require.True(t, ok)
	require.Equal(t, codecs.ConsensusModuleEncodedLen(), wireEnd)

	got, ok := codecs.TryDecodeConsensusModuleAt(buf, 0)
	require.True(t, ok)
	require.Equal(t, int64(0), got.NextServiceSessionID)
	require.Equal(t, int64(0), got.LogServiceSessionID)
	require.Equal(t, int32(0), got.PendingMessageCapacity)
	require.Equal(t, fields, got)
}

// Below the optional-field min version, encode must not touch bytes past
// wireEnd. Garbage in a full-length buffer shows which tail bytes write.
func TestSnapshotEncode_belowMinVersionSkipsOptionalTail(t *testing.T) {
	const garbage byte = 0xAB

	t.Run("snapshot_marker_v3", func(t *testing.T) {
		actingVersion := uint16(3)
		fields := snapshotMarkerSampleFields()
		buf := bytes.Repeat([]byte{garbage}, codecs.SnapshotMarkerEncodedLen())

		wireEnd, ok := codecs.EncodeSnapshotMarker(buf, 0, actingVersion, fields)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+32, wireEnd)

		var msg clusterwire.SnapshotMarker
		timeUnitOff := messageHeaderLen + int(msg.TimeUnitEncodingOffset())
		appVersionOff := messageHeaderLen + int(msg.AppVersionEncodingOffset())
		require.Equal(t, wireEnd, timeUnitOff)
		require.Equal(t, bytes.Repeat([]byte{garbage}, 4), buf[timeUnitOff:timeUnitOff+4])
		require.Equal(t, bytes.Repeat([]byte{garbage}, 4), buf[appVersionOff:appVersionOff+4])

		got, ok := codecs.TryDecodeSnapshotMarkerAt(buf, 0)
		require.True(t, ok)
		require.Equal(t, codecs.SnapshotMarkerOracleFields(fields, actingVersion), got)
	})

	t.Run("consensus_module_v2", func(t *testing.T) {
		actingVersion := uint16(2)
		fields := consensusModuleSampleFields()
		buf := bytes.Repeat([]byte{garbage}, codecs.ConsensusModuleEncodedLen())

		wireEnd, ok := codecs.EncodeConsensusModule(buf, 0, actingVersion, fields)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+8, wireEnd)

		var msg clusterwire.ConsensusModule
		tailOff := messageHeaderLen + int(msg.NextServiceSessionIdEncodingOffset())
		require.Equal(t, wireEnd, tailOff)
		require.Equal(t, bytes.Repeat([]byte{garbage}, len(buf)-tailOff), buf[tailOff:])

		got, ok := codecs.TryDecodeConsensusModuleAt(buf, 0)
		require.True(t, ok)
		require.Equal(t, codecs.ConsensusModuleOracleFields(fields, actingVersion), got)
	})
}

func TestSnapshotOracleFields_belowMinVersion(t *testing.T) {
	t.Run("snapshot_marker_v3", func(t *testing.T) {
		got := codecs.SnapshotMarkerOracleFields(snapshotMarkerSampleFields(), 3)
		require.Equal(t, clusterwire.ClusterTimeUnit_NULL_VALUE, got.TimeUnit)
		require.Equal(t, int32(0), got.AppVersion)
		require.Equal(t, snapshotMarkerSampleFields().Mark, got.Mark)
	})
	t.Run("snapshot_marker_v4", func(t *testing.T) {
		got := codecs.SnapshotMarkerOracleFields(snapshotMarkerSampleFields(), 4)
		require.Equal(t, snapshotMarkerSampleFields(), got)
	})
	t.Run("consensus_module_v2", func(t *testing.T) {
		got := codecs.ConsensusModuleOracleFields(consensusModuleSampleFields(), 2)
		require.Equal(t, int64(math.MinInt64), got.NextServiceSessionID)
		require.Equal(t, int64(math.MinInt64), got.LogServiceSessionID)
		require.Equal(t, int32(0), got.PendingMessageCapacity)
		require.Equal(t, consensusModuleSampleFields().NextSessionID, got.NextSessionID)
	})
	t.Run("consensus_module_v3", func(t *testing.T) {
		got := codecs.ConsensusModuleOracleFields(consensusModuleSampleFields(), 3)
		require.Equal(t, consensusModuleSampleFields(), got)
	})
}

func TestSnapshotBlockLength_byActingVersion(t *testing.T) {
	cases := []struct {
		name       string
		templateID uint16
		blockLen   func(uint16) uint16
		actingVer  uint16
		want       uint16
		maxBlock   uint16
	}{
		{
			"snapshot_marker_v0", clusterwire.SnapshotMarkerSbeTemplateID,
			codecs.SnapshotMarkerBlockLength, 0, 32, clusterwire.SnapshotMarkerSbeBlockLength,
		},
		{
			"snapshot_marker_v3", clusterwire.SnapshotMarkerSbeTemplateID,
			codecs.SnapshotMarkerBlockLength, 3, 32, clusterwire.SnapshotMarkerSbeBlockLength,
		},
		{
			"snapshot_marker_v4", clusterwire.SnapshotMarkerSbeTemplateID,
			codecs.SnapshotMarkerBlockLength, 4, 40, clusterwire.SnapshotMarkerSbeBlockLength,
		},
		{
			"consensus_module_v0", clusterwire.ConsensusModuleSbeTemplateID,
			codecs.ConsensusModuleBlockLength, 0, 8, clusterwire.ConsensusModuleSbeBlockLength,
		},
		{
			"consensus_module_v2", clusterwire.ConsensusModuleSbeTemplateID,
			codecs.ConsensusModuleBlockLength, 2, 8, clusterwire.ConsensusModuleSbeBlockLength,
		},
		{
			"consensus_module_v3", clusterwire.ConsensusModuleSbeTemplateID,
			codecs.ConsensusModuleBlockLength, 3, 28, clusterwire.ConsensusModuleSbeBlockLength,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.blockLen(tc.actingVer))
			minBlock, maxBlock, ok := codecs.ClusterRootBlockLengthBounds(tc.templateID, tc.actingVer)
			require.True(t, ok)
			require.Equal(t, tc.want, minBlock)
			require.Equal(t, tc.maxBlock, maxBlock)
		})
	}
}

func TestTryDecodeAt_snapshot(t *testing.T) {
	v := clusterwire.SnapshotMarkerSbeSchemaVersion
	buf := make([]byte, 4096)

	t.Run("snapshot_marker", func(t *testing.T) {
		requireTryDecodeAt(
			t, buf, v, snapshotMarkerSampleFields(),
			codecs.EncodeSnapshotMarker, codecs.TryDecodeSnapshotMarkerAt,
		)
	})
	t.Run("snapshot_marker_v3", func(t *testing.T) {
		end, ok := codecs.EncodeSnapshotMarker(buf, 0, 3, snapshotMarkerSampleFields())
		require.True(t, ok)
		got, ok := codecs.TryDecodeSnapshotMarkerAt(buf[:end], 0)
		require.True(t, ok)
		require.Equal(t, codecs.SnapshotMarkerOracleFields(snapshotMarkerSampleFields(), 3), got)
	})
	t.Run("client_session", func(t *testing.T) {
		fields := clientSessionSampleFields()
		end, ok := codecs.EncodeClientSession(buf, 0, v, fields)
		require.True(t, ok)
		wire := buf[:end]
		got, ok := codecs.TryDecodeClientSessionAt(wire, 0)
		require.True(t, ok)
		require.Equal(t, fields, got)

		// Var slices must be copies, not views into wire. Mutate every wire
		// byte after decode. Clones taken before the mutation stay equal.
		wantChannel := bytes.Clone(fields.ResponseChannel)
		wantPrincipal := bytes.Clone(fields.EncodedPrincipal)
		for i := range wire {
			wire[i] ^= 0xFF
		}
		require.Equal(t, wantChannel, got.ResponseChannel)
		require.Equal(t, wantPrincipal, got.EncodedPrincipal)
	})
	t.Run("client_session_large_principal", func(t *testing.T) {
		requireTryDecodeAt(
			t, buf, v,
			codecs.ClientSessionFields{
				ClusterSessionID: 1, ResponseStreamID: 2,
				ResponseChannel:  []byte("aeron:ipc"),
				EncodedPrincipal: clusterLargeVarPayload,
			},
			codecs.EncodeClientSession, codecs.TryDecodeClientSessionAt,
		)
	})
	t.Run("timer", func(t *testing.T) {
		requireTryDecodeAt(
			t, buf, v, timerSampleFields(), codecs.EncodeTimer, codecs.TryDecodeTimerAt,
		)
	})
	t.Run("consensus_module", func(t *testing.T) {
		requireTryDecodeAt(
			t, buf, v, consensusModuleSampleFields(),
			codecs.EncodeConsensusModule, codecs.TryDecodeConsensusModuleAt,
		)
	})
	t.Run("consensus_module_v2", func(t *testing.T) {
		end, ok := codecs.EncodeConsensusModule(buf, 0, 2, consensusModuleSampleFields())
		require.True(t, ok)
		got, ok := codecs.TryDecodeConsensusModuleAt(buf[:end], 0)
		require.True(t, ok)
		require.Equal(t, codecs.ConsensusModuleOracleFields(consensusModuleSampleFields(), 2), got)
	})
}

// A wire from a newer schema version decodes when its block fits the pinned
// schema.
func TestTryDecodeByTemplate_snapshotActingVersionAboveSchema(t *testing.T) {
	const actingVersion = clusterwire.SnapshotMarkerSbeSchemaVersion + 1
	cases := []struct {
		name       string
		templateID uint16
		encode     func([]byte) (int, bool)
		want       any
	}{
		{
			"snapshot_marker", clusterwire.SnapshotMarkerSbeTemplateID,
			func(b []byte) (int, bool) {
				return codecs.EncodeSnapshotMarker(b, 0, actingVersion, snapshotMarkerSampleFields())
			},
			snapshotMarkerSampleFields(),
		},
		{
			"client_session", clusterwire.ClientSessionSbeTemplateID,
			func(b []byte) (int, bool) {
				return codecs.EncodeClientSession(b, 0, actingVersion, clientSessionSampleFields())
			},
			clientSessionSampleFields(),
		},
		{
			"timer", clusterwire.TimerSbeTemplateID,
			func(b []byte) (int, bool) {
				return codecs.EncodeTimer(b, 0, actingVersion, timerSampleFields())
			},
			timerSampleFields(),
		},
		{
			"consensus_module", clusterwire.ConsensusModuleSbeTemplateID,
			func(b []byte) (int, bool) {
				return codecs.EncodeConsensusModule(b, 0, actingVersion, consensusModuleSampleFields())
			},
			consensusModuleSampleFields(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := encodeClusterWire(t, tc.encode)
			decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecTestMaxVarLen)
			require.True(t, ok)
			require.Equal(t, tc.templateID, decoded.TemplateID)
			requireDecodedClusterFields(t, tc.want, decoded.Fields)
		})
	}
}

func TestSnapshotEncodedLen_matchesFlyweight(t *testing.T) {
	require.Equal(
		t,
		int((&clusterwire.SnapshotMarker{}).SbeBlockAndHeaderLength()),
		codecs.SnapshotMarkerEncodedLen(),
	)
	require.Equal(
		t,
		int((&clusterwire.Timer{}).SbeBlockAndHeaderLength()),
		codecs.TimerEncodedLen(),
	)
	require.Equal(
		t,
		int((&clusterwire.ConsensusModule{}).SbeBlockAndHeaderLength()),
		codecs.ConsensusModuleEncodedLen(),
	)

	v := clusterwire.SnapshotMarkerSbeSchemaVersion
	buf := make([]byte, 64)
	end, ok := codecs.EncodeSnapshotMarker(buf, 0, v, snapshotMarkerSampleFields())
	require.True(t, ok)
	require.Equal(t, codecs.SnapshotMarkerEncodedLen(), end)
	end, ok = codecs.EncodeTimer(buf, 0, v, timerSampleFields())
	require.True(t, ok)
	require.Equal(t, codecs.TimerEncodedLen(), end)
	end, ok = codecs.EncodeConsensusModule(buf, 0, v, consensusModuleSampleFields())
	require.True(t, ok)
	require.Equal(t, codecs.ConsensusModuleEncodedLen(), end)

	fields := clientSessionSampleFields()
	wantLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ClientSessionSbeBlockLength,
		len(fields.ResponseChannel),
		len(fields.EncodedPrincipal),
	)
	require.True(t, ok)
	end, ok = codecs.EncodeClientSession(buf, 0, v, fields)
	require.True(t, ok)
	require.Equal(t, wantLen, end)
}

func TestSnapshotEncode_rejects(t *testing.T) {
	v := clusterwire.SnapshotMarkerSbeSchemaVersion
	buf := make([]byte, 8)
	requireEncodeRejects(t, buf, v, snapshotMarkerSampleFields(), codecs.EncodeSnapshotMarker)
	requireEncodeRejects(t, buf, v, clientSessionSampleFields(), codecs.EncodeClientSession)
	requireEncodeRejects(t, buf, v, timerSampleFields(), codecs.EncodeTimer)
	requireEncodeRejects(t, buf, v, consensusModuleSampleFields(), codecs.EncodeConsensusModule)

	shortByOne := make([]byte, codecs.SnapshotMarkerEncodedLen()-1)
	end, ok := codecs.EncodeSnapshotMarker(shortByOne, 0, v, snapshotMarkerSampleFields())
	require.False(t, ok)
	require.Equal(t, 0, end)

	big := make([]byte, 1024+codecs.ClusterEncodeMaxVarLen)
	end, ok = codecs.EncodeClientSession(big, 0, v, codecs.ClientSessionFields{
		EncodedPrincipal: make([]byte, codecs.ClusterEncodeMaxVarLen+1),
	})
	require.False(t, ok)
	require.Equal(t, 0, end)
}

func TestSnapshotFieldsFromLayout(t *testing.T) {
	v := clusterwire.SnapshotMarkerSbeSchemaVersion
	timerWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeTimer(buf, 0, v, timerSampleFields())
	})
	scheduleTimerWire := encodeScheduleTimerWire(t, v, scheduleTimerSampleFields())
	markerWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeSnapshotMarker(buf, 0, v, snapshotMarkerSampleFields())
	})
	moduleWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeConsensusModule(buf, 0, v, consensusModuleSampleFields())
	})

	t.Run("snapshot_marker", func(t *testing.T) {
		requireFieldsFromLayout(
			t, snapshotMarkerSampleFields(), markerWire,
			codecs.SnapshotMarkerFieldsFromLayout, moduleWire,
		)
	})
	t.Run("timer", func(t *testing.T) {
		requireFieldsFromLayout(
			t, timerSampleFields(), timerWire, codecs.TimerFieldsFromLayout, scheduleTimerWire,
		)
	})
	t.Run("consensus_module", func(t *testing.T) {
		requireFieldsFromLayout(
			t, consensusModuleSampleFields(), moduleWire,
			codecs.ConsensusModuleFieldsFromLayout, markerWire,
		)
	})
}

func snapshotMarkerSampleFields() codecs.SnapshotMarkerFields {
	return codecs.SnapshotMarkerFields{
		TypeID:           codecs.ConsensusModuleSnapshotTypeID,
		LogPosition:      1024,
		LeadershipTermID: 3,
		Index:            1,
		Mark:             clusterwire.SnapshotMark_BEGIN,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       5,
	}
}

func clientSessionSampleFields() codecs.ClientSessionFields {
	return codecs.ClientSessionFields{
		ClusterSessionID: 7,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0xAB, 0xCD},
	}
}

func timerSampleFields() codecs.TimerFields {
	return codecs.TimerFields{CorrelationID: 9, Deadline: 10000}
}

func consensusModuleSampleFields() codecs.ConsensusModuleFields {
	return codecs.ConsensusModuleFields{
		NextSessionID:          11,
		NextServiceSessionID:   12,
		LogServiceSessionID:    13,
		PendingMessageCapacity: 14,
	}
}
