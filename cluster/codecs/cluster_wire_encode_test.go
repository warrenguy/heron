package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestEncodeSessionKeepAlive_ZeroAllocs(t *testing.T) {
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
	}
	buf := make([]byte, messageHeaderLen+int(clusterwire.SessionKeepAliveSbeBlockLength))

	testutil.AssertZeroAllocs(t, "EncodeSessionKeepAlive", 1000, func() {
		_, _ = codecs.EncodeSessionKeepAlive(
			buf, 0, uint16(clusterwire.SessionKeepAliveSbeSchemaVersion), fields,
		)
	})
}

func TestNewLeadershipTermEvent_flyweightConstants(t *testing.T) {
	require.Equal(t, uint16(48), clusterwire.NewLeadershipTermEventSbeBlockLength)
	require.Equal(t, uint16(24), clusterwire.NewLeadershipTermEventSbeTemplateID)
	require.Equal(t, uint16(111), clusterwire.NewLeadershipTermEventSbeSchemaID)
	require.Equal(t, uint16(17), clusterwire.NewLeadershipTermEventSbeSchemaVersion)

	var msg clusterwire.NewLeadershipTermEvent
	require.Equal(t, uint64(4), msg.TimeUnitSinceVersion())
	require.Equal(t, uint64(4), msg.AppVersionSinceVersion())
	require.Equal(t, uint16(4), codecs.NewLeadershipTermEventTimeUnitMinVersion)
	require.Equal(
		t,
		uint64(clusterwire.MessageHeaderEncodedLength)+48,
		msg.SbeBlockAndHeaderLength(),
	)
}

func TestNewLeadershipTermEventBlockLength_versionGates(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		want      uint16
	}{
		{"v0_base_fields", 0, 40},
		{"v3_base_fields", 3, 40},
		{"v4_full_block", 4, 48},
		{"v16_full_block", 16, 48},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, codecs.NewLeadershipTermEventBlockLength(tc.actingVer))

			minBlock, maxBlock, ok := codecs.ClusterRootBlockLengthBounds(
				clusterwire.NewLeadershipTermEventSbeTemplateID,
				tc.actingVer,
			)
			require.True(t, ok)
			require.Equal(t, tc.want, minBlock)
			require.Equal(t, clusterwire.NewLeadershipTermEventSbeBlockLength, maxBlock)
		})
	}
}

func TestNewLeadershipTermEventEncodedLen(t *testing.T) {
	require.Equal(t, 56, codecs.NewLeadershipTermEventEncodedLen())
	require.Equal(
		t,
		codecs.MessageHeaderEncodedLen()+int(clusterwire.NewLeadershipTermEventSbeBlockLength),
		codecs.NewLeadershipTermEventEncodedLen(),
	)

	require.Equal(
		t,
		codecs.NewLeadershipTermEventEncodedLen(),
		mustClusterWireMessageLen(t, codecs.NewLeadershipTermEventBlockLength(16)),
	)
	require.Equal(
		t,
		messageHeaderLen+40,
		mustClusterWireMessageLen(t, codecs.NewLeadershipTermEventBlockLength(3)),
	)
}

func TestNewLeadershipTermEvent_varFieldCount_zero(t *testing.T) {
	for _, ver := range []uint16{0, 3, 4, 16} {
		count, ok := codecs.ClusterVarFieldCount(
			clusterwire.NewLeadershipTermEventSbeTemplateID,
			ver,
		)
		require.True(t, ok, "version=%d", ver)
		require.Equal(t, 0, count, "version=%d", ver)
	}
}

func TestNewLeadershipTermEventOracleFields(t *testing.T) {
	fields := codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    1,
		LogPosition:         2,
		Timestamp:           3,
		TermBaseLogPosition: 4,
		LeaderMemberID:      5,
		LogSessionID:        6,
		TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:          42,
	}

	t.Run("below_min_version_clears_optionals", func(t *testing.T) {
		want := fields
		want.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
		want.AppVersion = 0
		require.Equal(t, want, codecs.NewLeadershipTermEventOracleFields(fields, 3))
	})

	t.Run("at_min_version_keeps_fields", func(t *testing.T) {
		got := codecs.NewLeadershipTermEventOracleFields(fields, 4)
		require.Equal(t, fields, got)
	})

	t.Run("app_version_zero_roundtrips_at_full_version", func(t *testing.T) {
		zeroApp := fields
		zeroApp.AppVersion = 0
		got := codecs.NewLeadershipTermEventOracleFields(zeroApp, 16)
		require.Equal(t, zeroApp, got)
	})
}

func newLeadershipTermEventSampleFields() codecs.NewLeadershipTermEventFields {
	return codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    11,
		LogPosition:         22,
		Timestamp:           33,
		TermBaseLogPosition: 44,
		LeaderMemberID:      5,
		LogSessionID:        6,
		TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:          7,
	}
}

func requireNewLeadershipTermEventHeader(
	t *testing.T,
	buf []byte,
	offset int,
	blockLength, actingVersion uint16,
) {
	t.Helper()

	gotBlock, templateID, schemaID, version, ok := codecs.DecodeMessageHeader(buf, offset)
	require.True(t, ok)
	require.Equal(t, blockLength, gotBlock)
	require.Equal(t, clusterwire.NewLeadershipTermEventSbeTemplateID, templateID)
	require.Equal(t, clusterwire.NewLeadershipTermEventSbeSchemaID, schemaID)
	require.Equal(t, actingVersion, version)
}

func requireNewLeadershipTermEventDecodes(
	t *testing.T,
	buf []byte,
	offset int,
	want codecs.NewLeadershipTermEventFields,
) {
	t.Helper()

	got, ok := codecs.TryDecodeNewLeadershipTermEventAt(buf, offset)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestEncodeNewLeadershipTermEvent_template24_BufferBounds(t *testing.T) {
	actingVersion := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)
	fields := newLeadershipTermEventSampleFields()
	fullWireLen := codecs.NewLeadershipTermEventEncodedLen()
	v3WireLen := mustClusterWireMessageLen(t, codecs.NewLeadershipTermEventBlockLength(3))
	require.Equal(t, messageHeaderLen+40, v3WireLen)

	cases := []struct {
		name        string
		bufLen      int
		offset      int
		actingVer   uint16
		wantOK      bool
		wantWireEnd int
	}{
		{
			name:      "header_only",
			bufLen:    messageHeaderLen,
			actingVer: actingVersion,
			wantOK:    false,
		},
		{
			name:      "one_byte_short_current_version",
			bufLen:    fullWireLen - 1,
			actingVer: actingVersion,
			wantOK:    false,
		},
		{
			name:        "exact_fit_current_version",
			bufLen:      fullWireLen,
			actingVer:   actingVersion,
			wantOK:      true,
			wantWireEnd: fullWireLen,
		},
		{
			name:      "one_byte_short_v3",
			bufLen:    v3WireLen - 1,
			actingVer: 3,
			wantOK:    false,
		},
		{
			name:        "exact_fit_v3",
			bufLen:      v3WireLen,
			actingVer:   3,
			wantOK:      true,
			wantWireEnd: v3WireLen,
		},
		{
			name:      "negative_offset",
			bufLen:    fullWireLen,
			offset:    -1,
			actingVer: actingVersion,
			wantOK:    false,
		},
		{
			name:      "offset_past_header_room",
			bufLen:    fullWireLen,
			offset:    fullWireLen - messageHeaderLen + 1,
			actingVer: actingVersion,
			wantOK:    false,
		},
		{
			name:        "offset_nonzero_exact_fit",
			bufLen:      4 + fullWireLen,
			offset:      4,
			actingVer:   actingVersion,
			wantOK:      true,
			wantWireEnd: 4 + fullWireLen,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
				buf, tc.offset, tc.actingVer, fields,
			)
			require.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				require.Zero(t, wireEnd)
				return
			}
			require.Equal(t, tc.wantWireEnd, wireEnd)
			blockLength := codecs.NewLeadershipTermEventBlockLength(tc.actingVer)
			requireNewLeadershipTermEventHeader(
				t, buf, tc.offset, blockLength, tc.actingVer,
			)
			requireNewLeadershipTermEventDecodes(
				t, buf, tc.offset,
				codecs.NewLeadershipTermEventOracleFields(fields, tc.actingVer),
			)
		})
	}
}

func TestEncodeNewLeadershipTermEvent_template24_versionGatesOptionalTail(t *testing.T) {
	fields := newLeadershipTermEventSampleFields()
	// Large enough for full block either way; garbage proves which tail bytes write.
	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	const garbage byte = 0xAB

	t.Run("below_time_unit_min_skips_optional_tail", func(t *testing.T) {
		for i := range buf {
			buf[i] = garbage
		}
		actingVersion := uint16(3)
		wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(buf, 0, actingVersion, fields)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+40, wireEnd)

		bodyOffset := messageHeaderLen
		blockLength := codecs.NewLeadershipTermEventBlockLength(actingVersion)
		require.Equal(t, uint16(40), blockLength)
		requireNewLeadershipTermEventHeader(t, buf, 0, blockLength, actingVersion)
		requireNewLeadershipTermEventDecodes(
			t, buf, 0,
			codecs.NewLeadershipTermEventOracleFields(fields, actingVersion),
		)

		// Optional offsets sit past the v3 root; encode must not touch them.
		var nltEvent clusterwire.NewLeadershipTermEvent
		timeUnitOff := bodyOffset + int(nltEvent.TimeUnitEncodingOffset())
		appVersionOff := bodyOffset + int(nltEvent.AppVersionEncodingOffset())
		require.Equal(t, []byte{garbage, garbage, garbage, garbage}, buf[timeUnitOff:timeUnitOff+4])
		require.Equal(t, []byte{garbage, garbage, garbage, garbage}, buf[appVersionOff:appVersionOff+4])
	})

	t.Run("at_time_unit_min_writes_optional_tail", func(t *testing.T) {
		for i := range buf {
			buf[i] = garbage
		}
		actingVersion := codecs.NewLeadershipTermEventTimeUnitMinVersion
		wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(buf, 0, actingVersion, fields)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+48, wireEnd)

		blockLength := codecs.NewLeadershipTermEventBlockLength(actingVersion)
		require.Equal(t, uint16(48), blockLength)
		requireNewLeadershipTermEventHeader(t, buf, 0, blockLength, actingVersion)
		requireNewLeadershipTermEventDecodes(t, buf, 0, fields)
	})
}

func TestEncodeNewLeadershipTermEvent_template24_dirtyBufferAppVersionZero(t *testing.T) {
	actingVersion := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)
	fields := newLeadershipTermEventSampleFields()
	fields.AppVersion = 0
	fields.TimeUnit = clusterwire.ClusterTimeUnit_MILLIS

	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	const residual byte = 0xFF
	for i := range buf {
		buf[i] = residual
	}

	wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(buf, 0, actingVersion, fields)
	require.True(t, ok)
	require.Equal(t, codecs.NewLeadershipTermEventEncodedLen(), wireEnd)

	bodyOffset := messageHeaderLen
	var msg clusterwire.NewLeadershipTermEvent
	msg.WrapForDecode(
		buf,
		uint64(bodyOffset),
		uint64(clusterwire.NewLeadershipTermEventSbeBlockLength),
		uint64(actingVersion),
		uint64(len(buf)),
	)
	require.Equal(t, int32(0), msg.AppVersion())
	require.Equal(t, clusterwire.ClusterTimeUnit_MILLIS, msg.TimeUnit())

	// Always-write proof: residual 0xFF must not remain at optional field slots.
	timeUnitOff := bodyOffset + int(msg.TimeUnitEncodingOffset())
	appVersionOff := bodyOffset + int(msg.AppVersionEncodingOffset())
	require.Equal(t, []byte{0, 0, 0, 0}, buf[timeUnitOff:timeUnitOff+4])
	require.Equal(t, []byte{0, 0, 0, 0}, buf[appVersionOff:appVersionOff+4])
}

func TestEncodeNewLeadershipTermEvent_template24_ZeroAllocs(t *testing.T) {
	fields := newLeadershipTermEventSampleFields()
	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	actingVersion := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)

	testutil.AssertZeroAllocs(t, "EncodeNewLeadershipTermEvent", 1000, func() {
		_, _ = codecs.EncodeNewLeadershipTermEvent(buf, 0, actingVersion, fields)
	})
}

func TestNewLeadershipTermEvent_roundtrip(t *testing.T) {
	defaultVer := clusterwire.NewLeadershipTermEventSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.NewLeadershipTermEventFields]{
		{
			name:   "full_nanos",
			fields: newLeadershipTermEventSampleFields(),
		},
		{
			// Always-write-on-0 proof lives in the dirty-buffer test; a fresh
			// zeroed roundtrip buffer cannot distinguish write from omit.
			name: "app_version_zero",
			fields: codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    100,
				LogPosition:         200,
				Timestamp:           300,
				TermBaseLogPosition: 400,
				LeaderMemberID:      1,
				LogSessionID:        2,
				TimeUnit:            clusterwire.ClusterTimeUnit_MILLIS,
				AppVersion:          0,
			},
		},
		{
			name: "max_int",
			fields: codecs.NewLeadershipTermEventFields{
				LeadershipTermID:    math.MaxInt64,
				LogPosition:         math.MaxInt64,
				Timestamp:           math.MaxInt64,
				TermBaseLogPosition: math.MaxInt64,
				LeaderMemberID:      math.MaxInt32,
				LogSessionID:        math.MaxInt32,
				TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
				AppVersion:          math.MaxInt32,
			},
		},
		{
			name:      "acting_version_3_optional_absent",
			actingVer: 3,
			fields:    newLeadershipTermEventSampleFields(),
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.NewLeadershipTermEventSbeTemplateID,
		defaultVer,
		buildActingFieldRoundtripCases(
			defaultVer,
			fieldCases,
			codecs.EncodeNewLeadershipTermEvent,
			codecs.NewLeadershipTermEventOracleFields,
		),
	)
}

// Version 3 admits blockLength in [40, 48]. Pad past the 40-byte min root may hold
// residual TimeUnit/AppVersion bytes; decode must surface NULL/0 for both paths.
func TestTryDecodeNewLeadershipTermEventAt_v3_paddedBlock_rewritesOptionalTail(t *testing.T) {
	fields := newLeadershipTermEventSampleFields()
	require.NotEqual(t, clusterwire.ClusterTimeUnit_NULL_VALUE, fields.TimeUnit)
	require.NotZero(t, fields.AppVersion)

	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
		buf, 0, clusterwire.NewLeadershipTermEventSbeSchemaVersion, fields,
	)
	require.True(t, ok)
	require.Equal(t, codecs.NewLeadershipTermEventEncodedLen(), wireEnd)

	// Full 48-byte body with non-null optional tail; actingVersion 3 only.
	const actingVersion uint16 = 3
	buf[6], buf[7] = byte(actingVersion), 0
	require.Equal(t, byte(48), buf[0])
	require.Equal(t, byte(0), buf[1])

	want := codecs.NewLeadershipTermEventOracleFields(fields, actingVersion)
	require.Equal(t, clusterwire.ClusterTimeUnit_NULL_VALUE, want.TimeUnit)
	require.Equal(t, int32(0), want.AppVersion)

	got, ok := codecs.TryDecodeNewLeadershipTermEventAt(buf[:wireEnd], 0)
	require.True(t, ok)
	require.Equal(t, want, got)

	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd], 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, clusterwire.NewLeadershipTermEventSbeTemplateID, decoded.TemplateID)
	requireDecodedClusterFields(t, want, decoded.Fields)
}

func TestEncodeClusterWire_BufferBounds(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	connectFields := codecs.SessionConnectRequestFields{
		CorrelationID:      1,
		ResponseStreamID:   2,
		Version:            3,
		ResponseChannel:    []byte("aeron:ipc"),
		EncodedCredentials: []byte{1, 2, 3},
		ClientInfo:         []byte("client"),
	}
	connectBlockLength := codecs.ClusterMinRootBlockLength(
		clusterwire.SessionConnectRequestSbeTemplateID,
		actingVersion,
		clusterwire.SessionConnectRequestSbeBlockLength,
	)
	connectWireLen, ok := codecs.ClusterWireMessageLen(
		connectBlockLength,
		len(connectFields.ResponseChannel),
		len(connectFields.EncodedCredentials),
		len(connectFields.ClientInfo),
	)
	require.True(t, ok)

	challengeFields := codecs.ChallengeResponseFields{
		CorrelationID:      4,
		ClusterSessionID:   5,
		EncodedCredentials: []byte{6, 7, 8},
	}
	challengeWireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ChallengeResponseSbeBlockLength,
		len(challengeFields.EncodedCredentials),
	)
	require.True(t, ok)

	connectEncode := sessionConnectRequestEncodeFunc(actingVersion, connectFields)
	challengeEncode := func(buf []byte) (int, bool) {
		return codecs.EncodeChallengeResponse(
			buf, 0, uint16(clusterwire.ChallengeResponseSbeSchemaVersion), challengeFields,
		)
	}

	cases := []struct {
		name              string
		bufLen            int
		wantOK            bool
		wantWireEnd       int
		verifyConnectWire bool
		encode            func(buf []byte) (wireEnd int, ok bool)
	}{
		{
			name:   "connect_header_only",
			bufLen: messageHeaderLen,
			wantOK: false,
			encode: connectEncode,
		},
		{
			name:   "connect_one_byte_short",
			bufLen: connectWireLen - 1,
			wantOK: false,
			encode: connectEncode,
		},
		{
			name:              "connect_exact_fit",
			bufLen:            connectWireLen,
			wantOK:            true,
			wantWireEnd:       connectWireLen,
			verifyConnectWire: true,
			encode:            connectEncode,
		},
		{
			name:   "challenge_header_only",
			bufLen: messageHeaderLen,
			wantOK: false,
			encode: challengeEncode,
		},
		{
			name:   "challenge_one_byte_short",
			bufLen: challengeWireLen - 1,
			wantOK: false,
			encode: challengeEncode,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			wireEnd, ok := tc.encode(buf)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantWireEnd, wireEnd)
				if tc.verifyConnectWire {
					connectWire := encodeSessionConnectRequestWire(t, actingVersion, connectFields)
					require.Equal(t, connectWire, buf[:wireEnd])
				}
				return
			}
			require.Zero(t, wireEnd)
		})
	}
}

func repeatLargeVarLens(n int) []int {
	varLens := make([]int, n)
	largeVarLen := len(clusterLargeVarPayload)
	for i := range varLens {
		varLens[i] = largeVarLen
	}
	return varLens
}

func assertEncodeWireEndMatchesMessageLen(
	t *testing.T,
	offset int,
	blockLength uint16,
	varLens []int,
	encode func(buf []byte, offset int) (int, bool),
) {
	t.Helper()

	wantWireLen, ok := codecs.ClusterWireMessageLen(blockLength, varLens...)
	require.True(t, ok)

	buf := make([]byte, offset+wantWireLen)
	wireEnd, ok := encode(buf, offset)
	require.True(t, ok)
	require.Equal(t, offset+wantWireLen, wireEnd)
}

func TestEncodeClusterWire_wireEndMatchesClusterWireMessageLen(t *testing.T) {
	twoVarConnectVersion := codecs.SessionConnectClientInfoMinVersion - 1
	threeVarConnectVersion := uint16(clusterwire.SessionConnectRequestSbeSchemaVersion)
	maxVar := clusterLargeVarPayload

	twoVarConnectBlockLength := codecs.ClusterMinRootBlockLength(
		clusterwire.SessionConnectRequestSbeTemplateID,
		twoVarConnectVersion,
		clusterwire.SessionConnectRequestSbeBlockLength,
	)
	threeVarConnectBlockLength := codecs.ClusterMinRootBlockLength(
		clusterwire.SessionConnectRequestSbeTemplateID,
		threeVarConnectVersion,
		clusterwire.SessionConnectRequestSbeBlockLength,
	)
	sessionEventVersion := uint16(clusterwire.SessionEventSbeSchemaVersion)
	sessionEventBlockLength := codecs.SessionEventBlockLength(sessionEventVersion)

	cases := []struct {
		name        string
		offset      int
		blockLength uint16
		varLens     []int
		encode      func(buf []byte, offset int) (int, bool)
	}{
		{
			name:        "session_message_header",
			offset:      4,
			blockLength: clusterwire.SessionMessageHeaderSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeSessionMessageHeader(
					buf, offset, clusterwire.SessionMessageHeaderSbeSchemaVersion,
					codecs.SessionMessageHeaderFields{
						LeadershipTermID: 1,
						ClusterSessionID: 2,
						Timestamp:        3,
					},
				)
			},
		},
		{
			name:        "session_event_at_max_var",
			blockLength: sessionEventBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionEventFields{
					ClusterSessionID: 1,
					CorrelationID:    2,
					Detail:           maxVar,
				}
				return codecs.EncodeSessionEvent(
					buf, offset, sessionEventVersion, sessionEventBlockLength, fields,
				)
			},
		},
		{
			name:        "session_event_acting_v5_block32",
			blockLength: 32,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeSessionEvent(buf, offset, 5, 32, codecs.SessionEventFields{
					ClusterSessionID:         40,
					CorrelationID:            41,
					LeadershipTermID:         42,
					LeaderMemberID:           43,
					Code:                     clusterwire.EventCode_CLOSED,
					LeaderHeartbeatTimeoutNs: math.MinInt64,
					Detail:                   maxVar,
				})
			},
		},
		{
			name:        "session_connect_request_two_var_at_max",
			blockLength: twoVarConnectBlockLength,
			varLens:     repeatLargeVarLens(2),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionConnectRequestFields{
					CorrelationID:      1,
					ResponseStreamID:   2,
					ResponseChannel:    maxVar,
					EncodedCredentials: maxVar,
				}
				return codecs.EncodeSessionConnectRequest(buf, offset, twoVarConnectVersion, fields)
			},
		},
		{
			name:        "session_connect_request_three_var_at_max",
			blockLength: threeVarConnectBlockLength,
			varLens:     repeatLargeVarLens(3),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionConnectRequestFields{
					CorrelationID:      1,
					ResponseStreamID:   2,
					ResponseChannel:    maxVar,
					EncodedCredentials: maxVar,
					ClientInfo:         maxVar,
				}
				return codecs.EncodeSessionConnectRequest(buf, offset, threeVarConnectVersion, fields)
			},
		},
		{
			name:        "session_close_request",
			blockLength: clusterwire.SessionCloseRequestSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionCloseRequestFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
				}
				return codecs.EncodeSessionCloseRequest(
					buf, offset, uint16(clusterwire.SessionCloseRequestSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "session_keep_alive",
			blockLength: clusterwire.SessionKeepAliveSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.SessionKeepAliveFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
				}
				return codecs.EncodeSessionKeepAlive(
					buf, offset, uint16(clusterwire.SessionKeepAliveSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "new_leader_event_at_max_var",
			blockLength: clusterwire.NewLeaderEventSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.NewLeaderEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
					LeaderMemberID:   3,
					IngressEndpoints: maxVar,
				}
				return codecs.EncodeNewLeaderEvent(
					buf, offset, uint16(clusterwire.NewLeaderEventSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "new_leadership_term_event",
			blockLength: clusterwire.NewLeadershipTermEventSbeBlockLength,
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeNewLeadershipTermEvent(
					buf,
					offset,
					uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion),
					newLeadershipTermEventSampleFields(),
				)
			},
		},
		{
			name:        "new_leadership_term_event_acting_v3_block40",
			blockLength: codecs.NewLeadershipTermEventBlockLength(3),
			encode: func(buf []byte, offset int) (int, bool) {
				return codecs.EncodeNewLeadershipTermEvent(
					buf, offset, 3, newLeadershipTermEventSampleFields(),
				)
			},
		},
		{
			name:        "challenge_at_max_var",
			blockLength: clusterwire.ChallengeSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.ChallengeFields{
					CorrelationID:    1,
					ClusterSessionID: 2,
					EncodedChallenge: maxVar,
				}
				return codecs.EncodeChallenge(
					buf,
					offset,
					uint16(clusterwire.ChallengeSbeSchemaVersion),
					fields,
				)
			},
		},
		{
			name:        "challenge_response_at_max_var",
			blockLength: clusterwire.ChallengeResponseSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.ChallengeResponseFields{
					CorrelationID:      1,
					ClusterSessionID:   2,
					EncodedCredentials: maxVar,
				}
				return codecs.EncodeChallengeResponse(
					buf, offset, uint16(clusterwire.ChallengeResponseSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "admin_request_at_max_var",
			blockLength: clusterwire.AdminRequestSbeBlockLength,
			varLens:     repeatLargeVarLens(1),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.AdminRequestFields{
					LeadershipTermID: 1,
					ClusterSessionID: 2,
					CorrelationID:    3,
					RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
					Payload:          maxVar,
				}
				return codecs.EncodeAdminRequest(
					buf, offset, uint16(clusterwire.AdminRequestSbeSchemaVersion), fields,
				)
			},
		},
		{
			name:        "admin_response_at_max_var",
			blockLength: clusterwire.AdminResponseSbeBlockLength,
			varLens:     repeatLargeVarLens(2),
			encode: func(buf []byte, offset int) (int, bool) {
				fields := codecs.AdminResponseFields{
					ClusterSessionID: 1,
					CorrelationID:    2,
					RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
					ResponseCode:     clusterwire.AdminResponseCode_OK,
					Message:          maxVar,
					Payload:          maxVar,
				}
				return codecs.EncodeAdminResponse(
					buf, offset, uint16(clusterwire.AdminResponseSbeSchemaVersion), fields,
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertEncodeWireEndMatchesMessageLen(t, tc.offset, tc.blockLength, tc.varLens, tc.encode)
		})
	}
}

func TestEncodeClusterWire_OversizeVarField_returnsOkFalse(t *testing.T) {
	actingVersion := uint16(clusterwire.SessionEventSbeSchemaVersion)
	blockLength := codecs.SessionEventBlockLength(actingVersion)
	oversizeVarLen := codecs.ClusterEncodeMaxVarLen + 1

	t.Run("cluster_wire_message_len_rejects_per_field_over_encode_max", func(t *testing.T) {
		wireLen, ok := codecs.ClusterWireMessageLen(blockLength, oversizeVarLen)
		require.False(t, ok)
		require.Zero(t, wireLen)
	})

	t.Run("encode_session_event_rejects_per_field_over_encode_max", func(t *testing.T) {
		fields := codecs.SessionEventFields{
			ClusterSessionID: 1,
			CorrelationID:    2,
			Detail:           make([]byte, oversizeVarLen),
		}
		buf := make([]byte, messageHeaderLen+int(blockLength)+codecs.VarLengthPrefixSize)

		wireEnd, ok := codecs.EncodeSessionEvent(buf, 0, actingVersion, blockLength, fields)

		require.False(t, ok)
		require.Zero(t, wireEnd)
	})
}
