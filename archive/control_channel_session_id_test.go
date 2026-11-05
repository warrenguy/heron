package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyControlChannelSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestChannel  string
		responseChannel string
		sessionID       int32
		wantRequest     string
		wantResponse    string
	}{
		{
			name:            "udp_caller_response_session_id",
			requestChannel:  "aeron:udp?endpoint=localhost:3388|mtu=2048",
			responseChannel: "aeron:udp?session-id=5|endpoint=localhost:0|sparse=true|mtu=1024",
			sessionID:       7,
			wantRequest:     "aeron:udp?endpoint=localhost:3388|mtu=2048|session-id=7",
			wantResponse:    "aeron:udp?session-id=7|endpoint=localhost:0|sparse=true|mtu=1024",
		},
		{
			name:            "udp_dynamic_control",
			requestChannel:  "aeron:udp?endpoint=localhost:3388",
			responseChannel: "aeron:udp?control=localhost:10000|control-mode=dynamic",
			sessionID:       7,
			wantRequest:     "aeron:udp?endpoint=localhost:3388|session-id=7",
			wantResponse:    "aeron:udp?control=localhost:10000|control-mode=dynamic|session-id=7",
		},
		{
			name:            "udp_manual_control",
			requestChannel:  "aeron:udp?endpoint=localhost:3388",
			responseChannel: "aeron:udp?control-mode=manual",
			sessionID:       7,
			wantRequest:     "aeron:udp?endpoint=localhost:3388|session-id=7",
			wantResponse:    "aeron:udp?control-mode=manual|session-id=7",
		},
		{
			name:            "ipc_caller_request_session_id",
			requestChannel:  "aeron:ipc?alias=request|ssc=false|linger=0|session-id=42|sparse=false",
			responseChannel: "aeron:ipc?term-length=64k|alias=response",
			sessionID:       7,
			wantRequest:     "aeron:ipc?alias=request|ssc=false|linger=0|session-id=7|sparse=false",
			wantResponse:    "aeron:ipc?term-length=64k|alias=response|session-id=7",
		},
		{
			name:            "no_query",
			requestChannel:  "aeron:ipc",
			responseChannel: "aeron:ipc",
			sessionID:       7,
			wantRequest:     "aeron:ipc?session-id=7",
			wantResponse:    "aeron:ipc?session-id=7",
		},
		{
			name:            "trailing_query_mark",
			requestChannel:  "aeron:ipc?",
			responseChannel: "aeron:ipc?",
			sessionID:       7,
			wantRequest:     "aeron:ipc?session-id=7",
			wantResponse:    "aeron:ipc?session-id=7",
		},
		{
			name:            "session_id_mid_query",
			requestChannel:  "aeron:ipc?alias=a|session-id=1|term-length=64k",
			responseChannel: "aeron:ipc?alias=b|session-id=2|term-length=64k",
			sessionID:       7,
			wantRequest:     "aeron:ipc?alias=a|session-id=7|term-length=64k",
			wantResponse:    "aeron:ipc?alias=b|session-id=7|term-length=64k",
		},
		{
			name:            "negative_session_id",
			requestChannel:  "aeron:ipc",
			responseChannel: "aeron:ipc?term-length=64k",
			sessionID:       -5,
			wantRequest:     "aeron:ipc?session-id=-5",
			wantResponse:    "aeron:ipc?term-length=64k|session-id=-5",
		},
		{
			name:            "alias_holds_session_id_text",
			requestChannel:  "aeron:ipc?alias=x-session-id=1",
			responseChannel: "aeron:ipc?alias=x-session-id=1",
			sessionID:       7,
			wantRequest:     "aeron:ipc?alias=x-session-id=1|session-id=7",
			wantResponse:    "aeron:ipc?alias=x-session-id=1|session-id=7",
		},
		{
			name:            "tagged_session_id",
			requestChannel:  "aeron:ipc?session-id=tag:1",
			responseChannel: "aeron:ipc?term-length=64k|session-id=tag:1",
			sessionID:       7,
			wantRequest:     "aeron:ipc?session-id=7",
			wantResponse:    "aeron:ipc?term-length=64k|session-id=7",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{
				effectiveControlRequestChannel:  tc.requestChannel,
				effectiveControlResponseChannel: tc.responseChannel,
			}
			got := applyControlChannelSessionID(cfg, tc.sessionID)
			require.Equal(t, tc.wantRequest, got.EffectiveControlRequestChannel())
			require.Equal(t, tc.wantResponse, got.EffectiveControlResponseChannel())
		})
	}
}

func TestUsesControlChannelSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestChannel  string
		responseChannel string
		want            bool
	}{
		{
			name:            "ipc_response_mode",
			requestChannel:  "aeron:ipc",
			responseChannel: "aeron:ipc?control-mode=response",
		},
		{
			name:            "no_mode",
			requestChannel:  "aeron:udp?endpoint=localhost:8010",
			responseChannel: "aeron:udp?endpoint=localhost:0",
			want:            true,
		},
		{
			name:            "ipc_request_response_mode",
			requestChannel:  "aeron:ipc?control-mode=response",
			responseChannel: "aeron:ipc",
			want:            true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{
				effectiveControlRequestChannel:  tc.requestChannel,
				effectiveControlResponseChannel: tc.responseChannel,
			}
			require.Equal(t, tc.want, usesControlChannelSessionID(cfg))
		})
	}
}
