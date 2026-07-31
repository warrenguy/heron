package codecs_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func TestTryDecodeByTemplate_rejects(t *testing.T) {
	validWire := encodeArchiveWire(t, testControlResponseBoom, encodeControlResponseAt)

	authConnect := codecs.AuthConnectRequestFields{
		CorrelationID:      1,
		ResponseStreamID:   2,
		Version:            3,
		ResponseChannel:    []byte("aeron:ipc"),
		EncodedCredentials: []byte{1, 2, 3},
		ClientInfo:         []byte("client"),
	}
	authBuf := make([]byte, fuzzArchiveCodecMaxLen)
	authWireEnd, ok := codecs.EncodeAuthConnectRequest(authBuf, 0, archiveSchemaVersion, authConnect)
	require.True(t, ok)
	authConnectWire := authBuf[:authWireEnd]

	cases := []struct {
		name   string
		buf    func() []byte
		offset int
		maxLen int
	}{
		{
			name: "truncated_header",
			buf:  func() []byte { return validWire[:4] },
		},
		{
			name:   "offset_out_of_bounds",
			buf:    func() []byte { return validWire },
			offset: 10000,
		},
		{
			name:   "offset_near_maxint",
			buf:    func() []byte { return validWire },
			offset: math.MaxInt,
		},
		{
			name: "schema_mismatch",
			buf: func() []byte {
				return cloneAndMutate(validWire, func(w []byte) { w[4], w[5] = 0, 0 })
			},
		},
		{
			name: "unknown_template_id",
			buf: func() []byte {
				return cloneAndMutate(validWire, func(w []byte) { w[2], w[3] = 0xFF, 0x7F })
			},
		},
		{
			name: "block_length_below_min",
			buf: func() []byte {
				return cloneAndMutate(validWire, func(w []byte) {
					w[0] = byte(archivewire.ControlResponseSbeBlockLength - 1)
					w[1] = 0
				})
			},
		},
		{
			name: "block_length_above_max",
			buf: func() []byte {
				return cloneAndMutate(validWire, func(w []byte) {
					w[0] = byte(archivewire.ControlResponseSbeBlockLength + 1)
					w[1] = 0
				})
			},
		},
		{
			name: "truncated_fixed_block",
			buf:  func() []byte { return validWire[:messageHeaderLen+10] },
		},
		{
			name: "truncated_var_field",
			buf:  func() []byte { return validWire[:len(validWire)-2] },
		},
		{
			name: "auth_connect_truncated_client_info",
			buf:  func() []byte { return authConnectWire[:len(authConnectWire)-1] },
		},
		{
			name:   "var_field_exceeds_max_var_len",
			buf:    func() []byte { return validWire },
			maxLen: 1,
		},
		{
			name:   "negative_max_var_len",
			buf:    func() []byte { return validWire },
			maxLen: -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxLen := tc.maxLen
			if maxLen == 0 {
				maxLen = codecTestMaxVarLen
			}
			_, ok := codecs.TryDecodeByTemplate(tc.buf(), tc.offset, maxLen)
			require.False(t, ok)
		})
	}
}

func TestTryDecodeAt_rejects(t *testing.T) {
	type decodeAtFn func(buf []byte, offset, maxVarLen int) bool

	type hotTemplate struct {
		name      string
		validWire func(t *testing.T) []byte
		decode    decodeAtFn
		hasVar    bool
		wrongID   uint16
	}

	templates := []hotTemplate{
		{
			name: "control_response",
			validWire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testControlResponseBoom, encodeControlResponseAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) bool {
				_, ok := codecs.TryDecodeControlResponseAt(buf, offset, maxVarLen)
				return ok
			},
			hasVar:  true,
			wrongID: archivewire.ConnectRequestSbeTemplateID,
		},
		{
			name: "recording_signal_event",
			validWire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testRecordingSignalEventStart, encodeRecordingSignalEventAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) bool {
				_, ok := codecs.TryDecodeRecordingSignalEventAt(buf, offset, maxVarLen)
				return ok
			},
			hasVar:  false,
			wrongID: archivewire.ControlResponseSbeTemplateID,
		},
		{
			name: "recording_descriptor",
			validWire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testRecordingDescriptorChannels, encodeRecordingDescriptorAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) bool {
				_, ok := codecs.TryDecodeRecordingDescriptorAt(buf, offset, maxVarLen)
				return ok
			},
			hasVar:  true,
			wrongID: archivewire.RecordingSignalEventSbeTemplateID,
		},
		{
			name: "challenge",
			validWire: func(t *testing.T) []byte {
				return encodeArchiveWire(t, testChallengeWithBytes, encodeChallengeAt)
			},
			decode: func(buf []byte, offset, maxVarLen int) bool {
				_, ok := codecs.TryDecodeChallengeAt(buf, offset, maxVarLen)
				return ok
			},
			hasVar:  true,
			wrongID: archivewire.ChallengeResponseSbeTemplateID,
		},
	}

	for _, tmpl := range templates {
		t.Run(tmpl.name, func(t *testing.T) {
			validWire := tmpl.validWire(t)

			cases := []struct {
				name    string
				buf     func() []byte
				offset  int
				maxLen  int
				skipVar bool
			}{
				{
					name: "truncated_header",
					buf:  func() []byte { return validWire[:4] },
				},
				{
					name:   "offset_out_of_bounds",
					buf:    func() []byte { return validWire },
					offset: 10000,
				},
				{
					name: "schema_mismatch",
					buf: func() []byte {
						return cloneAndMutate(validWire, func(w []byte) { w[4], w[5] = 0, 0 })
					},
				},
				{
					name: "wrong_template_id",
					buf: func() []byte {
						return cloneAndMutate(validWire, func(w []byte) {
							w[2] = byte(tmpl.wrongID)
							w[3] = byte(tmpl.wrongID >> 8)
						})
					},
				},
				{
					name: "truncated_fixed_block",
					buf:  func() []byte { return validWire[:messageHeaderLen+10] },
				},
				{
					name:    "truncated_var_field",
					buf:     func() []byte { return validWire[:len(validWire)-2] },
					skipVar: !tmpl.hasVar,
				},
				{
					name:    "var_field_exceeds_max_var_len",
					buf:     func() []byte { return validWire },
					maxLen:  1,
					skipVar: !tmpl.hasVar,
				},
				{
					name:   "negative_max_var_len",
					buf:    func() []byte { return validWire },
					maxLen: -1,
				},
			}

			for _, tc := range cases {
				if tc.skipVar {
					continue
				}
				t.Run(tc.name, func(t *testing.T) {
					maxLen := tc.maxLen
					if maxLen == 0 {
						maxLen = codecTestMaxVarLen
					}
					ok := tmpl.decode(tc.buf(), tc.offset, maxLen)
					require.False(t, ok)
				})
			}
		})
	}
}

func TestArchiveMessageWireEnd_rejects_negative_max_var_len(t *testing.T) {
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	wireEnd, ok := encodeControlResponseAt(buf, 0, codecs.ControlResponseFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RelevantID:       3,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          7,
		ErrorMessage:     []byte("x"),
	})
	require.True(t, ok)

	_, ok = codecs.ArchiveMessageWireEnd(buf[:wireEnd], 0, -1)
	require.False(t, ok)
}

func TestTryDecodeByTemplate_allows_trailing_bytes(t *testing.T) {
	fields := testStartRecordingRequestIPC
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	wireEnd, ok := codecs.EncodeStartRecordingRequest(buf, 0, archiveSchemaVersion, fields)
	require.True(t, ok)

	buf[wireEnd] = 0xDE
	buf[wireEnd+1] = 0xAD
	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd+8], 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, archivewire.StartRecordingRequestSbeTemplateID, decoded.TemplateID)
	requireDecodedArchiveFields(t, fields, decoded.Fields)
}

func TestTryDecodeByTemplate_truncated_request_var_field(t *testing.T) {
	fields := codecs.StartRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		StreamID:         3,
		SourceLocation:   archivewire.SourceLocation_REMOTE,
		Channel:          bytes.Repeat([]byte{0x41}, 16),
	}
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	wireEnd, ok := codecs.EncodeStartRecordingRequest(buf, 0, archiveSchemaVersion, fields)
	require.True(t, ok)

	_, ok = codecs.TryDecodeByTemplate(buf[:wireEnd-4], 0, codecTestMaxVarLen)
	require.False(t, ok)
}

func TestTryDecodeByTemplate_truncated_extend_request_var_field(t *testing.T) {
	fields := codecs.ExtendRecordingRequest2Fields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
		StreamID:         4,
		SourceLocation:   archivewire.SourceLocation_REMOTE,
		AutoStop:         archivewire.BooleanType_TRUE,
		Channel:          bytes.Repeat([]byte{0x41}, 16),
	}
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	wireEnd, ok := codecs.EncodeExtendRecordingRequest2(buf, 0, archiveSchemaVersion, fields)
	require.True(t, ok)

	_, ok = codecs.TryDecodeByTemplate(buf[:wireEnd-1], 0, codecTestMaxVarLen)
	require.False(t, ok)
}

func TestEncodeArchiveWire_bufferBounds(t *testing.T) {
	fields := testStartRecordingRequestIPC
	wireLen, ok := codecs.ArchiveWireMessageLen(
		archivewire.StartRecordingRequestSbeBlockLength,
		len(fields.Channel),
	)
	require.True(t, ok)

	cases := []struct {
		name        string
		bufLen      int
		offset      int
		wantOK      bool
		wantWireEnd int
	}{
		{name: "header_only", bufLen: messageHeaderLen, wantOK: false},
		{name: "one_byte_short", bufLen: wireLen - 1, wantOK: false},
		{name: "exact_fit", bufLen: wireLen, wantOK: true, wantWireEnd: wireLen},
		{name: "negative_offset", bufLen: wireLen, offset: -1, wantOK: false},
		{name: "offset_past_header_room", bufLen: messageHeaderLen - 1, wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			wireEnd, ok := codecs.EncodeStartRecordingRequest(buf, tc.offset, archiveSchemaVersion, fields)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantWireEnd, wireEnd)
				return
			}
			require.Zero(t, wireEnd)
		})
	}
}

func TestEncodeArchiveWire_oversizeVarField(t *testing.T) {
	t.Run("wire_message_len_rejects_per_field_over_encode_max", func(t *testing.T) {
		wireLen, ok := codecs.ArchiveWireMessageLen(
			archivewire.StartRecordingRequestSbeBlockLength,
			codecs.ArchiveEncodeMaxVarLen+1,
		)
		require.False(t, ok)
		require.Zero(t, wireLen)
	})

	t.Run("encode_rejects_per_field_over_encode_max", func(t *testing.T) {
		fields := codecs.StartRecordingRequestFields{
			ControlSessionID: 1,
			CorrelationID:    2,
			StreamID:         3,
			SourceLocation:   archivewire.SourceLocation_LOCAL,
			Channel:          make([]byte, codecs.ArchiveEncodeMaxVarLen+1),
		}
		bufLen := messageHeaderLen +
			int(archivewire.StartRecordingRequestSbeBlockLength) +
			codecs.VarLengthPrefixSize
		buf := make([]byte, bufLen)
		wireEnd, ok := codecs.EncodeStartRecordingRequest(buf, 0, archiveSchemaVersion, fields)
		require.False(t, ok)
		require.Zero(t, wireEnd)
	})
}
