//go:build testhooks

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const moduleSnapshotReplaySessionID int64 = 1002

func encodeModuleSnapshotMarker(t *testing.T, fields codecs.SnapshotMarkerFields) []byte {
	t.Helper()
	buf := make([]byte, codecs.SnapshotMarkerEncodedLen())
	end, ok := codecs.EncodeSnapshotMarker(
		buf, 0, clusterwire.SnapshotMarkerSbeSchemaVersion, fields,
	)
	require.True(t, ok)
	return buf[:end]
}

func encodeModuleSnapshotConsensus(t *testing.T, fields codecs.ConsensusModuleFields) []byte {
	t.Helper()
	buf := make([]byte, codecs.ConsensusModuleEncodedLen())
	end, ok := codecs.EncodeConsensusModule(
		buf, 0, clusterwire.ConsensusModuleSbeSchemaVersion, fields,
	)
	require.True(t, ok)
	return buf[:end]
}

func moduleSnapshotMarker(
	logPosition int64,
	mark clusterwire.SnapshotMark,
) codecs.SnapshotMarkerFields {
	return codecs.SnapshotMarkerFields{
		TypeID:           codecs.ConsensusModuleSnapshotTypeID,
		LogPosition:      logPosition,
		LeadershipTermID: 3,
		Mark:             mark,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       0,
	}
}

type fakeSnapshotControl struct {
	fakeReplayControl
}

func (f *fakeSnapshotControl) Poll() int { return 0 }

func TestModuleSnapshotLoad_run(t *testing.T) {
	t.Parallel()

	const (
		recordingID int64 = 55
		logPosition int64 = 128
		timeoutNs   int64 = 1_000_000_000
	)
	snapshot := recordinglog.Snapshot{
		RecordingID:      recordingID,
		LeadershipTermID: 3,
		LogPosition:      logPosition,
	}
	prefix := "cluster: load module snapshot recording 55"

	type row struct {
		name      string
		startErr  error
		image     bool
		payloads  [][]byte
		endAtZero bool
		jump      bool
		cancel    bool
		wantErr   string
		wantStop  int
		wantNext  int64
		wantCode  int32
	}

	rows := []row{
		{
			name:     "unknown recording",
			startErr: &archive.Exception{ErrorCode: archive.ErrorUnknownRecording},
			wantErr:  "start replay",
			wantStop: 0,
			wantCode: archive.ErrorUnknownRecording,
		},
		{
			name:  "corrupt frame",
			image: true,
			payloads: [][]byte{encodeModuleSnapshotConsensus(t, codecs.ConsensusModuleFields{
				NextSessionID: 11,
			})},
			jump:     true,
			wantErr:  "consensus: load snapshot",
			wantStop: 1,
		},
		{
			name:      "image ends before the end marker",
			image:     true,
			endAtZero: true,
			jump:      true,
			wantErr:   "replay ended before the end marker",
			wantStop:  1,
		},
		{
			name:     "image never appears",
			jump:     true,
			wantErr:  "replay image did not appear within one leader heartbeat",
			wantStop: 1,
		},
		{
			name:     "ctx ends before the image appears",
			cancel:   true,
			wantErr:  context.Canceled.Error(),
			wantStop: 1,
		},
		{
			name:     "ctx ends before the end marker",
			image:    true,
			cancel:   true,
			wantErr:  context.Canceled.Error(),
			wantStop: 1,
		},
		{
			name:     "fragment timeout",
			image:    true,
			jump:     true,
			wantErr:  "no replay fragment within one leader heartbeat",
			wantStop: 1,
		},
		{
			name:  "full snapshot",
			image: true,
			payloads: [][]byte{
				encodeModuleSnapshotMarker(t, moduleSnapshotMarker(
					logPosition, clusterwire.SnapshotMark_BEGIN,
				)),
				encodeModuleSnapshotConsensus(t, codecs.ConsensusModuleFields{
					NextSessionID: 11,
				}),
				encodeModuleSnapshotMarker(t, moduleSnapshotMarker(
					logPosition, clusterwire.SnapshotMark_END,
				)),
			},
			wantStop: 1,
			wantNext: 11,
		},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clock := int64(1_000)
			control := &fakeSnapshotControl{
				fakeReplayControl: fakeReplayControl{
					sessionID: moduleSnapshotReplaySessionID,
					startErr:  tc.startErr,
				},
			}
			sub := aeron.NewTestSubscriptionForHooks(t)
			if tc.image {
				image := attachReplayImage(t, sub, moduleSnapshotReplaySessionID, 0)
				var offset int64
				for _, payload := range tc.payloads {
					offset = writeReplayFrame(t, image, offset, payload)
				}
				if tc.endAtZero {
					aeron.ImageSetEndOfStreamPositionForHooks(image, 0)
				}
			}
			adder := &fakeSubscriptionAdder{result: sub}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			agent := consensus.NewAgent(consensus.AgentConfig{})
			err := moduleSnapshotLoad{
				ctx:     ctx,
				control: control,
				adder:   adder,
				pump: func() int {
					if tc.cancel {
						cancel()
					}
					if tc.jump {
						clock += timeoutNs + 1
					}
					return 0
				},
				nanoClock: func() int64 { return clock },
				channel:   "aeron:ipc",
				streamID:  103,
				timeoutNs: timeoutNs,
			}.run(agent, snapshot)

			require.Len(t, control.startCalls, 1)
			require.Len(t, control.stopCalls, tc.wantStop)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.wantNext, agent.NextSessionIDForTest())
			} else {
				require.ErrorContains(t, err, prefix)
				require.ErrorContains(t, err, tc.wantErr)
				if tc.cancel {
					require.ErrorIs(t, err, context.Canceled)
				}
				if tc.wantCode != 0 {
					code, ok := archive.ExceptionCode(err)
					require.True(t, ok, "%v", err)
					require.Equal(t, tc.wantCode, code)
				}
				if tc.wantStop == 0 {
					require.Empty(t, adder.calls)
					return
				}
			}
			require.Len(t, adder.calls, 1)
			require.True(t, sub.IsClosed())
		})
	}
}
