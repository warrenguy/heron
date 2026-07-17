package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireEffectiveControl(
	t *testing.T,
	archiveCtx Context,
	wantReqCh, wantRespCh string,
	wantReqStream, wantRespStream int32,
) {
	t.Helper()
	require.Equal(t, wantReqCh, archiveCtx.EffectiveControlRequestChannel())
	require.Equal(t, wantRespCh, archiveCtx.EffectiveControlResponseChannel())
	require.Equal(t, wantReqStream, archiveCtx.EffectiveControlRequestStreamID())
	require.Equal(t, wantRespStream, archiveCtx.EffectiveControlResponseStreamID())
}

func requireEffectiveStreamIDs(t *testing.T, archiveCtx Context, want int32) {
	t.Helper()
	require.Equal(t, want, archiveCtx.EffectiveControlRequestStreamID())
	require.Equal(t, want, archiveCtx.EffectiveControlResponseStreamID())
}

func TestEffectiveChannels(t *testing.T) {
	tests := []struct {
		name       string
		archiveCtx Context
		wantReqCh  string
		wantRespCh string
		wantStream int32
	}{
		{
			name:       "ipc default",
			archiveCtx: Context{}.Normalize(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
		{
			name:       "remote",
			archiveCtx: Context{}.WithRemoteControl().Normalize(),
			wantReqCh:  DefaultControlRequestChannel,
			wantRespCh: DefaultControlResponseChannel,
			wantStream: DefaultControlRequestStreamID,
		},
		{
			name: "explicit local after remote opt out",
			archiveCtx: func() Context {
				archiveCtx := Context{}.WithRemoteControl().Normalize()
				archiveCtx.LocalControlChannel = DefaultLocalControlChannel
				return archiveCtx
			}(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			archiveCtx := applyEffectiveChannels(tc.archiveCtx)
			requireEffectiveControl(t, archiveCtx, tc.wantReqCh, tc.wantRespCh, tc.wantStream, tc.wantStream)
		})
	}
}

func TestEffectiveChannels_ipcStreamID(t *testing.T) {
	tests := []struct {
		name      string
		input     Context
		normalize bool
		want      int32
	}{
		{
			name: "explicit",
			input: Context{
				LocalControlChannel:  DefaultLocalControlChannel,
				LocalControlStreamID: 77,
			},
			normalize: true,
			want:      77,
		},
		{
			name: "fallback",
			input: Context{
				LocalControlChannel:    DefaultLocalControlChannel,
				LocalControlStreamID:   0,
				ControlRequestStreamID: 42,
			},
			want: 42,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			archiveCtx := tc.input
			if tc.normalize {
				archiveCtx = archiveCtx.Normalize()
			}
			archiveCtx = applyEffectiveChannels(archiveCtx)
			requireEffectiveStreamIDs(t, archiveCtx, tc.want)
		})
	}
}
