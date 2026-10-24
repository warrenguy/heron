package cluster

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestLogChannelWithInitialPosition(t *testing.T) {
	t.Parallel()

	const (
		baseChannel = "aeron:udp?term-length=64k|endpoint=239.255.12.1:9004"
		termLen     = int32(logbuffer.TermMinLength)
		mtu         = int32(1408)
		initTermID  = int32(7)
	)

	tests := []struct {
		name            string
		channel         string
		seed            *LogPublicationInitialPosition
		wantChannel     string
		wantErrContains string
	}{
		{
			name:        "nil seed leaves channel unchanged",
			channel:     baseChannel,
			seed:        nil,
			wantChannel: baseChannel,
		},
		{
			name:    "position zero seeds geometry",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         0,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
			wantChannel: "aeron:udp?term-length=" + strconv.Itoa(int(termLen)) +
				"|endpoint=239.255.12.1:9004" +
				"|init-term-id=7|term-id=7|term-offset=0|mtu=1408",
		},
		{
			name:    "negative position errors",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         -1,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
			wantErrContains: "is invalid",
		},
		{
			name:    "seeds aligned position and replaces term-length",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         128,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
			wantChannel: "aeron:udp?term-length=" + strconv.Itoa(int(termLen)) +
				"|endpoint=239.255.12.1:9004" +
				"|init-term-id=7|term-id=7|term-offset=128|mtu=1408",
		},
		{
			name:    "term boundary advances term-id",
			channel: "aeron:udp?endpoint=239.255.12.1:9004",
			seed: &LogPublicationInitialPosition{
				Position:         int64(termLen),
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
			wantChannel: "aeron:udp?endpoint=239.255.12.1:9004" +
				"|init-term-id=7|term-id=8|term-offset=0" +
				"|term-length=" + strconv.Itoa(int(termLen)) +
				"|mtu=1408",
		},
		{
			name:    "unaligned position errors",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         17,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
			wantErrContains: "not frame-aligned",
		},
		{
			name:    "invalid term length errors",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         32,
				InitialTermID:    initTermID,
				TermBufferLength: 1000,
				MtuLength:        mtu,
			},
			wantErrContains: "term length",
		},
		{
			name:    "non-positive mtu errors",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         32,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        0,
			},
			wantErrContains: "mtu",
		},
		{
			name:    "unaligned mtu errors",
			channel: baseChannel,
			seed: &LogPublicationInitialPosition{
				Position:         32,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        1400,
			},
			wantErrContains: "mtu",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := logChannelWithInitialPosition(tc.channel, tc.seed)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantErrContains)
				require.Empty(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantChannel, got)
		})
	}
}

func TestLogChannelWithInitialPosition_roundtripParams(t *testing.T) {
	t.Parallel()

	const (
		channel = "aeron:udp?term-length=64k|endpoint=239.255.12.1:9004"
		termLen = logbuffer.TermMinLength
		pos     = int64(96)
		initID  = int32(3)
		mtu     = int32(1408)
	)

	seed := &LogPublicationInitialPosition{
		Position:         pos,
		InitialTermID:    initID,
		TermBufferLength: int32(termLen),
		MtuLength:        mtu,
	}
	got, err := logChannelWithInitialPosition(channel, seed)
	require.NoError(t, err)

	shift := logbuffer.PositionBitsToShift(termLen)
	wantTermID := logbuffer.ComputeTermIDFromPosition(pos, shift, initID)
	wantOffset := logbuffer.ComputeTermOffsetFromPosition(pos, shift)

	require.Equal(t, strconv.Itoa(int(initID)), channelParamValue(got, channelInitTermIDParam))
	require.Equal(t, strconv.Itoa(int(wantTermID)), channelParamValue(got, channelTermIDParam))
	require.Equal(t, strconv.Itoa(int(wantOffset)), channelParamValue(got, channelTermOffsetParam))
	require.Equal(t, strconv.Itoa(termLen), channelParamValue(got, channelTermLengthParam))
	require.Equal(t, strconv.Itoa(int(mtu)), channelParamValue(got, channelMTUParam))

	// Existing term-length replaced; no duplicate.
	require.Equal(t, 1, strings.Count(got, "term-length="))
}

func TestLogPublicationSeedFromPlan(t *testing.T) {
	t.Parallel()

	const (
		initTermID = int32(5)
		termLen    = int32(128 * 1024)
		mtu        = int32(1408)
	)

	extent := recordinglog.LogRecordingExtents{
		InitialTermID:    initTermID,
		TermBufferLength: termLen,
		MtuLength:        mtu,
	}
	log := &recordinglog.RecoveryPlanLog{
		Term:   recordinglog.TermRef{RecordingID: 9},
		Extent: extent,
	}

	tests := []struct {
		name string
		plan recordinglog.RecoveryPlan
		want *LogPublicationInitialPosition
	}{
		{
			name: "nil log is cold start",
			plan: recordinglog.RecoveryPlan{},
			want: nil,
		},
		{
			name: "null appended floors to committed and seeds",
			plan: recordinglog.RecoveryPlan{
				AppendedLogPosition:  recordinglog.NullPosition,
				CommittedLogPosition: 320,
				Log:                  log,
			},
			want: &LogPublicationInitialPosition{
				Position:         320,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
		},
		{
			name: "position zero seeds",
			plan: recordinglog.RecoveryPlan{
				AppendedLogPosition: 0,
				Log:                 log,
			},
			want: &LogPublicationInitialPosition{
				Position:         0,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
		},
		{
			name: "positive position seeds",
			plan: recordinglog.RecoveryPlan{
				AppendedLogPosition: 480,
				Log:                 log,
			},
			want: &LogPublicationInitialPosition{
				Position:         480,
				InitialTermID:    initTermID,
				TermBufferLength: termLen,
				MtuLength:        mtu,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := logPublicationSeedFromPlan(tc.plan)
			require.Equal(t, tc.want, got)
		})
	}
}

func channelParamValue(channel, param string) string {
	idx := aeron.ChannelParamIndex(channel, param, true)
	if idx < 0 {
		return ""
	}
	return aeron.ChannelParamValue(channel, idx, param)
}
