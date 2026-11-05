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
	requireEffectiveStreamIDs(t, cfg, wantReqStream, wantRespStream)
}

func requireEffectiveStreamIDs(t *testing.T, cfg Config, wantReq, wantResp int32) {
	t.Helper()
	require.Equal(t, wantReq, cfg.EffectiveControlRequestStreamID())
	require.Equal(t, wantResp, cfg.EffectiveControlResponseStreamID())
}

func TestEffectiveChannels(t *testing.T) {
	tests := []struct {
		name          string
		cfg           Config
		wantReqCh     string
		wantRespCh    string
		wantReqStream int32
	}{
		{
			name:          "ipc default",
			cfg:           Config{}.Normalize(),
			wantReqCh:     DefaultLocalControlChannel,
			wantRespCh:    DefaultLocalControlChannel,
			wantReqStream: DefaultLocalControlStreamID,
		},
		{
			name:          "remote",
			cfg:           Config{}.WithRemoteControl().Normalize(),
			wantReqCh:     DefaultControlRequestChannel,
			wantRespCh:    DefaultControlResponseChannel,
			wantReqStream: DefaultControlRequestStreamID,
		},
		{
			name: "explicit local after remote opt out",
			cfg: func() Config {
				cfg := Config{}.WithRemoteControl().Normalize()
				cfg.LocalControlChannel = DefaultLocalControlChannel
				return cfg
			}(),
			wantReqCh:     DefaultLocalControlChannel,
			wantRespCh:    DefaultLocalControlChannel,
			wantReqStream: DefaultLocalControlStreamID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := applyEffectiveChannels(tc.cfg)
			requireEffectiveControl(
				t,
				cfg,
				tc.wantReqCh,
				tc.wantRespCh,
				tc.wantReqStream,
				DefaultControlResponseStreamID,
			)
		})
	}
}

func TestEffectiveChannels_ipcStreamID(t *testing.T) {
	tests := []struct {
		name      string
		input     Config
		normalize bool
		wantReq   int32
		wantResp  int32
	}{
		{
			name: "explicit",
			input: Config{
				LocalControlChannel:  DefaultLocalControlChannel,
				LocalControlStreamID: 77,
			},
			normalize: true,
			wantReq:   77,
			wantResp:  DefaultControlResponseStreamID,
		},
		{
			name: "fallback",
			input: Config{
				LocalControlChannel:    DefaultLocalControlChannel,
				LocalControlStreamID:   0,
				ControlRequestStreamID: 42,
			},
			// Without Normalize, the response stream stays zero.
			wantReq:  42,
			wantResp: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.input
			if tc.normalize {
				cfg = cfg.Normalize()
			}
			cfg = applyEffectiveChannels(cfg)
			requireEffectiveStreamIDs(t, cfg, tc.wantReq, tc.wantResp)
		})
	}
}
