package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireEffectiveControl(
	t *testing.T,
	cfg Config,
	wantReqCh, wantRespCh string,
	wantReqStream, wantRespStream int32,
) {
	t.Helper()
	require.Equal(t, wantReqCh, cfg.EffectiveControlRequestChannel())
	require.Equal(t, wantRespCh, cfg.EffectiveControlResponseChannel())
	require.Equal(t, wantReqStream, cfg.EffectiveControlRequestStreamID())
	require.Equal(t, wantRespStream, cfg.EffectiveControlResponseStreamID())
}

func requireEffectiveStreamIDs(t *testing.T, cfg Config, want int32) {
	t.Helper()
	require.Equal(t, want, cfg.EffectiveControlRequestStreamID())
	require.Equal(t, want, cfg.EffectiveControlResponseStreamID())
}

func TestEffectiveChannels(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantReqCh  string
		wantRespCh string
		wantStream int32
	}{
		{
			name:       "ipc default",
			cfg:        Config{}.Normalize(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
		{
			name:       "remote",
			cfg:        Config{}.WithRemoteControl().Normalize(),
			wantReqCh:  DefaultControlRequestChannel,
			wantRespCh: DefaultControlResponseChannel,
			wantStream: DefaultControlRequestStreamID,
		},
		{
			name: "explicit local after remote opt out",
			cfg: func() Config {
				cfg := Config{}.WithRemoteControl().Normalize()
				cfg.LocalControlChannel = DefaultLocalControlChannel
				return cfg
			}(),
			wantReqCh:  DefaultLocalControlChannel,
			wantRespCh: DefaultLocalControlChannel,
			wantStream: DefaultLocalControlStreamID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := applyEffectiveChannels(tc.cfg)
			requireEffectiveControl(t, cfg, tc.wantReqCh, tc.wantRespCh, tc.wantStream, tc.wantStream)
		})
	}
}

func TestEffectiveChannels_ipcStreamID(t *testing.T) {
	tests := []struct {
		name      string
		input     Config
		normalize bool
		want      int32
	}{
		{
			name: "explicit",
			input: Config{
				LocalControlChannel:  DefaultLocalControlChannel,
				LocalControlStreamID: 77,
			},
			normalize: true,
			want:      77,
		},
		{
			name: "fallback",
			input: Config{
				LocalControlChannel:    DefaultLocalControlChannel,
				LocalControlStreamID:   0,
				ControlRequestStreamID: 42,
			},
			want: 42,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.input
			if tc.normalize {
				cfg = cfg.Normalize()
			}
			cfg = applyEffectiveChannels(cfg)
			requireEffectiveStreamIDs(t, cfg, tc.want)
		})
	}
}
