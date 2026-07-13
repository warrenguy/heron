package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireEffectiveControl(
	t *testing.T,
	ctx Context,
	wantReqCh, wantRespCh string,
	wantReqStream, wantRespStream int32,
) {
	t.Helper()
	require.Equal(t, wantReqCh, ctx.EffectiveControlRequestChannel())
	require.Equal(t, wantRespCh, ctx.EffectiveControlResponseChannel())
	require.Equal(t, wantReqStream, ctx.EffectiveControlRequestStreamID())
	require.Equal(t, wantRespStream, ctx.EffectiveControlResponseStreamID())
}

func requireEffectiveStreamIDs(t *testing.T, ctx Context, want int32) {
	t.Helper()
	require.Equal(t, want, ctx.EffectiveControlRequestStreamID())
	require.Equal(t, want, ctx.EffectiveControlResponseStreamID())
}

func TestEffectiveChannels(t *testing.T) {
	tests := []struct {
		name       string
		ctx        Context
		wantReqCh  string
		wantRespCh string
		wantStream int32
	}{
		{
			name:       "ipc default",
			ctx:        Context{}.Normalize(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
		{
			name:       "remote",
			ctx:        Context{}.WithRemoteControl().Normalize(),
			wantReqCh:  DefaultControlRequestChannel,
			wantRespCh: DefaultControlResponseChannel,
			wantStream: DefaultControlRequestStreamID,
		},
		{
			name: "explicit local after remote opt out",
			ctx: func() Context {
				ctx := Context{}.WithRemoteControl().Normalize()
				ctx.LocalControlChannel = DefaultLocalControlChannel
				return ctx
			}(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := applyEffectiveChannels(tc.ctx)
			requireEffectiveControl(t, ctx, tc.wantReqCh, tc.wantRespCh, tc.wantStream, tc.wantStream)
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
			ctx := tc.input
			if tc.normalize {
				ctx = ctx.Normalize()
			}
			ctx = applyEffectiveChannels(ctx)
			requireEffectiveStreamIDs(t, ctx, tc.want)
		})
	}
}
