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

func TestTryDecodeByTemplate_rejects(t *testing.T) {
	actingVersion := clusterwire.SessionEventSbeSchemaVersion
	validEvent := codecs.SessionEventFields{
		ClusterSessionID:         1,
		CorrelationID:            2,
		LeadershipTermID:         3,
		LeaderMemberID:           4,
		Code:                     clusterwire.EventCode_OK,
		Detail:                   []byte("x"),
		LeaderHeartbeatTimeoutNs: 0,
	}
	validConnect := codecs.SessionConnectRequestFields{
		CorrelationID:      10,
		ResponseStreamID:   20,
		ResponseChannel:    []byte("aeron:ipc"),
		EncodedCredentials: []byte{1, 2},
		ClientInfo:         []byte("client"),
	}
	validEventWire := encodeSessionEventWire(t, actingVersion, validEvent)
	validConnectWire := encodeSessionConnectRequestWire(
		t, sessionConnectClientInfoMinVersion, validConnect,
	)

	cases := []tryDecodeRejectCase{}
	cases = append(cases, wireHeaderRejectCases(validEventWire)...)
	cases = append(
		cases,
		tryDecodeRejectCase{
			name:   "offset_out_of_bounds",
			buf:    func() []byte { return validEventWire },
			offset: 10000,
		},
		tryDecodeRejectCase{
			name:   "offset_near_maxint",
			buf:    func() []byte { return validEventWire },
			offset: math.MaxInt,
		},
	)
	cases = append(cases, fixedBlockRejectCases(
		"", validEventWire, clusterwire.SessionEventSbeBlockLength, 31, 10,
	)...)
	cases = append(
		cases,
		tryDecodeRejectCase{
			name: "truncated_var_field",
			buf:  func() []byte { return validEventWire[:len(validEventWire)-2] },
		},
		tryDecodeRejectCase{
			name: "var_field_exceeds_max_var_len",
			buf: func() []byte {
				fields := validEvent
				fields.Detail = bytes.Repeat([]byte{0x41}, 4)
				return encodeSessionEventWire(t, actingVersion, fields)
			},
			maxLen: 1,
		},
		tryDecodeRejectCase{
			name: "connect_truncated_client_info",
			buf:  func() []byte { return validConnectWire[:len(validConnectWire)-1] },
		},
		tryDecodeRejectCase{
			name:   "negative_max_var_len",
			buf:    func() []byte { return validEventWire },
			maxLen: -1,
		},
	)

	runTryDecodeRejectCases(t, cases)
}

func TestTryDecodeByTemplate_rejects_newLeadershipTermEvent(t *testing.T) {
	v16 := uint16(clusterwire.NewLeadershipTermEventSbeSchemaVersion)

	validEventV16Wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeNewLeadershipTermEvent(
			buf, 0, v16, newLeadershipTermEventSampleFields(),
		)
	})

	cases := []tryDecodeRejectCase{}
	cases = append(cases, fixedBlockRejectCases(
		"nlt_event",
		validEventV16Wire,
		clusterwire.NewLeadershipTermEventSbeBlockLength,
		47,
		39,
	)...)
	// v3 min root is 40; v16 suite already covers above-max and truncate at max block 48.
	cases = append(cases, tryDecodeRejectCase{
		name: rejectCaseName("nlt_event_v3", "block_length_below_min"),
		buf: func() []byte {
			return cloneMutatedWire(validEventV16Wire, func(w []byte) {
				w[0] = 39
				w[6], w[7] = 3, 0
			})
		},
	})

	runTryDecodeRejectCases(t, cases)

	// A genuine template-53 wire passes template-53 layout bounds, so the
	// reject can only come from the template-id guard.
	t.Run("wrong_template_53_into_event_try_decode", func(t *testing.T) {
		_, ok := codecs.TryDecodeNewLeadershipTermEventAt(encodeNewLeadershipTermWire(t), 0)
		require.False(t, ok)
	})
	t.Run("wrong_template_24_into_consensus_try_decode", func(t *testing.T) {
		_, ok := codecs.TryDecodeNewLeadershipTermAt(validEventV16Wire, 0)
		require.False(t, ok)
	})
}

func TestTryDecodeByTemplate_rejects_snapshot(t *testing.T) {
	v16 := uint16(clusterwire.SnapshotMarkerSbeSchemaVersion)

	markerWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeSnapshotMarker(buf, 0, v16, snapshotMarkerSampleFields())
	})
	sessionWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeClientSession(buf, 0, v16, clientSessionSampleFields())
	})
	timerWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeTimer(buf, 0, v16, timerSampleFields())
	})
	moduleWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeConsensusModule(buf, 0, v16, consensusModuleSampleFields())
	})

	cases := []tryDecodeRejectCase{}
	cases = append(cases, fixedBlockRejectCases(
		"snapshot_marker", markerWire, clusterwire.SnapshotMarkerSbeBlockLength, 39, 10,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"client_session", sessionWire, clusterwire.ClientSessionSbeBlockLength, 11, 8,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"timer", timerWire, clusterwire.TimerSbeBlockLength, 15, 8,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"consensus_module", moduleWire, clusterwire.ConsensusModuleSbeBlockLength, 27, 10,
	)...)
	cases = append(
		cases,
		// The v3 min root is 32. The v16 suite covers above-max and truncate.
		tryDecodeRejectCase{
			name: rejectCaseName("snapshot_marker_v3", "block_length_below_min"),
			buf: func() []byte {
				return cloneMutatedWire(markerWire, func(w []byte) {
					w[0] = 31
					w[6], w[7] = 3, 0
				})
			},
		},
		// The v2 min root is 8. The v16 suite covers above-max and truncate.
		tryDecodeRejectCase{
			name: rejectCaseName("consensus_module_v2", "block_length_below_min"),
			buf: func() []byte {
				return cloneMutatedWire(moduleWire, func(w []byte) {
					w[0] = 7
					w[6], w[7] = 2, 0
				})
			},
		},
		tryDecodeRejectCase{
			name:   "client_session_var_field_exceeds_max_var_len",
			buf:    func() []byte { return sessionWire },
			maxLen: 1,
		},
		tryDecodeRejectCase{
			name: "client_session_truncated_var_field",
			buf:  func() []byte { return sessionWire[:len(sessionWire)-1] },
		},
	)
	cases = append(cases, wireHeaderRejectCases(markerWire)...)

	runTryDecodeRejectCases(t, cases)

	t.Run("client_session_try_decode_var_over_cap", func(t *testing.T) {
		// The buffer holds the claimed payload, so ReadVarSlice hits the
		// maxLen path, not the length-vs-remaining bounds path.
		headerAndBlock := messageHeaderLen + int(clusterwire.ClientSessionSbeBlockLength)
		overLen := codecs.ClusterDecodeMaxVarLen + 1
		wire := make([]byte, headerAndBlock+2*codecs.VarLengthPrefixSize+overLen)
		copy(wire, sessionWire[:headerAndBlock])
		binary.LittleEndian.PutUint32(wire[headerAndBlock:], uint32(overLen))
		_, ok := codecs.TryDecodeClientSessionAt(wire, 0)
		require.False(t, ok)
	})
	t.Run("client_session_try_decode_truncated_var", func(t *testing.T) {
		_, ok := codecs.TryDecodeClientSessionAt(sessionWire[:len(sessionWire)-1], 0)
		require.False(t, ok)
	})

	// ScheduleTimer and Timer share a 16-byte block. Only the template-id
	// check rejects each wire in the other typed decoder.
	scheduleTimerWire := encodeScheduleTimerWire(t, v16, scheduleTimerSampleFields())
	t.Run("wrong_template_31_into_timer_try_decode", func(t *testing.T) {
		_, ok := codecs.TryDecodeTimerAt(scheduleTimerWire, 0)
		require.False(t, ok)
	})
	t.Run("wrong_template_104_into_schedule_timer_try_decode", func(t *testing.T) {
		_, ok := codecs.TryDecodeScheduleTimerAt(timerWire, 0)
		require.False(t, ok)
	})

	t.Run("snapshot_marker_try_decode_at", func(t *testing.T) {
		requireTryDecodeAtRejects(t, markerWire, moduleWire, codecs.TryDecodeSnapshotMarkerAt)
	})
	t.Run("client_session_try_decode_at", func(t *testing.T) {
		requireTryDecodeAtRejects(t, sessionWire, timerWire, codecs.TryDecodeClientSessionAt)
	})
	t.Run("timer_try_decode_at", func(t *testing.T) {
		requireTryDecodeAtRejects(t, timerWire, scheduleTimerWire, codecs.TryDecodeTimerAt)
	})
	t.Run("consensus_module_try_decode_at", func(t *testing.T) {
		requireTryDecodeAtRejects(t, moduleWire, markerWire, codecs.TryDecodeConsensusModuleAt)
	})
}

func encodeNewLeadershipTermWire(t *testing.T) []byte {
	t.Helper()
	return encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeNewLeadershipTerm(
			buf, 0, clusterwire.NewLeadershipTermSbeSchemaVersion,
			codecs.NewLeadershipTermFields{
				LogLeadershipTermID:     1,
				NextLeadershipTermID:    2,
				NextTermBaseLogPosition: 3,
				NextLogPosition:         4,
				LeadershipTermID:        5,
				TermBaseLogPosition:     6,
				LogPosition:             7,
				LeaderRecordingID:       8,
				Timestamp:               9,
				LeaderMemberID:          10,
				LogSessionID:            11,
				IsStartup:               clusterwire.BooleanType_FALSE,
				CommitPosition:          12,
			},
		)
	})
}

func TestTryDecodeByTemplate_rejects_consensus(t *testing.T) {
	v16 := uint16(clusterwire.NewLeadershipTermSbeSchemaVersion)
	v8 := uint16(8)
	v7 := uint16(7)

	validCanvassWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeCanvassPosition(buf, 0, v16, codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
			ProtocolVersion:     5,
		})
	})
	validRequestVoteWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeRequestVote(buf, 0, v16, codecs.RequestVoteFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			CandidateTermID:     3,
			CandidateMemberID:   4,
			ProtocolVersion:     5,
		})
	})
	validAppendV16Wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeAppendPosition(buf, 0, v16, codecs.AppendPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			FollowerMemberID: 3,
			Flags:            4,
		})
	})
	validAppendV7Wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeAppendPosition(buf, 0, v7, codecs.AppendPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			FollowerMemberID: 3,
		})
	})
	validVoteWire := encodeClusterWire(t, func(buf []byte) (int, bool) {
		return codecs.EncodeVote(buf, 0, v16, codecs.VoteFields{
			CandidateTermID:     1,
			LogLeadershipTermID: 2,
			LogPosition:         3,
			CandidateMemberID:   4,
			FollowerMemberID:    5,
			Vote:                clusterwire.BooleanType_TRUE,
		})
	})
	validLeadershipWire := encodeNewLeadershipTermWire(t)

	cases := []tryDecodeRejectCase{}
	cases = append(cases, fixedBlockRejectCases(
		"canvass", validCanvassWire, clusterwire.CanvassPositionSbeBlockLength, 27, 10,
	)...)
	cases = append(cases, fixedBlockRejectAboveMaxAtVersion(
		"canvass", validCanvassWire, v8, clusterwire.CanvassPositionSbeBlockLength, "v8",
	))
	cases = append(cases, fixedBlockRejectCases(
		"request_vote", validRequestVoteWire, clusterwire.RequestVoteSbeBlockLength, 27, 10,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"append_position", validAppendV16Wire, clusterwire.AppendPositionSbeBlockLength, 19, 10,
	)...)
	cases = append(cases, fixedBlockRejectAboveMaxAtVersion(
		"append_position", validAppendV7Wire, v7, clusterwire.AppendPositionSbeBlockLength, "v7",
	))
	cases = append(cases, fixedBlockRejectCases(
		"vote", validVoteWire, clusterwire.VoteSbeBlockLength, 35, 10,
	)...)
	cases = append(cases, fixedBlockRejectCases(
		"new_leadership", validLeadershipWire, clusterwire.NewLeadershipTermSbeBlockLength, 87, 40,
	)...)

	runTryDecodeRejectCases(t, cases)
}

func TestTryDecodeByTemplate_catchupTemplates(t *testing.T) {
	t.Run("catchup_position", func(t *testing.T) {
		want := codecs.CatchupPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			FollowerMemberID: 3,
			CatchupEndpoint:  "localhost:0",
		}
		wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
			return codecs.EncodeCatchupPosition(
				buf, 0, clusterwire.CatchupPositionSbeSchemaVersion, want,
			)
		})
		decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecTestMaxVarLen)
		require.True(t, ok)
		require.Equal(t, clusterwire.CatchupPositionSbeTemplateID, decoded.TemplateID)
		fields, ok := decoded.Fields.(codecs.CatchupPositionFields)
		require.True(t, ok)
		require.Equal(t, want, fields)
	})
	t.Run("stop_catchup", func(t *testing.T) {
		want := codecs.StopCatchupFields{
			LeadershipTermID: 1,
			FollowerMemberID: 2,
		}
		wire := encodeClusterWire(t, func(buf []byte) (int, bool) {
			return codecs.EncodeStopCatchup(
				buf, 0, clusterwire.StopCatchupSbeSchemaVersion, want,
			)
		})
		decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecTestMaxVarLen)
		require.True(t, ok)
		require.Equal(t, clusterwire.StopCatchupSbeTemplateID, decoded.TemplateID)
		fields, ok := decoded.Fields.(codecs.StopCatchupFields)
		require.True(t, ok)
		require.Equal(t, want, fields)
	})
}

func TestClusterMessageWireEnd_rejects_negative_max_var_len(t *testing.T) {
	fields := codecs.SessionEventFields{
		ClusterSessionID: 1,
		CorrelationID:    2,
		LeadershipTermID: 3,
		LeaderMemberID:   4,
		Code:             clusterwire.EventCode_OK,
		Detail:           []byte("x"),
	}
	wire := encodeSessionEventWire(t, clusterwire.SessionEventSbeSchemaVersion, fields)

	_, ok := codecs.ClusterMessageWireEnd(wire, 0, -1)
	require.False(t, ok)
}

func TestTryDecodeByTemplate_allows_trailing_bytes(t *testing.T) {
	fields := codecs.SessionEventFields{
		ClusterSessionID:         1,
		CorrelationID:            2,
		LeadershipTermID:         3,
		LeaderMemberID:           4,
		Code:                     clusterwire.EventCode_OK,
		Detail:                   []byte("tail"),
		LeaderHeartbeatTimeoutNs: 0,
	}
	buf, wireEnd := encodeSessionEventInto(t, clusterwire.SessionEventSbeSchemaVersion, fields)

	// Simulate Aeron fragment buffer with trailing garbage after the message.
	buf[wireEnd] = 0xDE
	buf[wireEnd+1] = 0xAD
	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd+8], 0, codecTestMaxVarLen)
	require.True(t, ok)
	require.Equal(t, clusterwire.SessionEventSbeTemplateID, decoded.TemplateID)
	requireDecodedClusterFields(
		t,
		codecs.SessionEventOracleFields(fields, clusterwire.SessionEventSbeSchemaVersion),
		decoded.Fields,
	)
}
