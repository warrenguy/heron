package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngressChannelMedia(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		want    string
		ok      bool
	}{
		{name: "ipc", channel: "aeron:ipc", want: "ipc", ok: true},
		{name: "ipcx", channel: "aeron:ipcx?alias=ingress", want: "ipcx", ok: true},
		{name: "udp", channel: "aeron:udp?endpoint=localhost:0", want: "udp", ok: true},
		{
			name:    "spy udp",
			channel: "aeron-spy:aeron:udp?endpoint=localhost:0",
			want:    "udp",
			ok:      true,
		},
		{name: "spy ipc", channel: "aeron-spy:aeron:ipc", want: "ipc", ok: true},
		{name: "unknown media", channel: "aeron:unknown", want: "unknown", ok: true},
		{name: "empty", channel: "", ok: false},
		{name: "not a channel", channel: "not-a-channel", ok: false},
		{name: "empty media", channel: "aeron:", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			media, ok := ingressChannelMedia(tc.channel)
			require.Equal(t, tc.ok, ok, tc.channel)
			if tc.ok {
				require.Equal(t, tc.want, media)
			}
		})
	}
}

func TestIngressChannelPredicates(t *testing.T) {
	tests := []struct {
		name         string
		channel      string
		wantUDP      bool
		wantPassThru bool
	}{
		{name: "spy udp", channel: "aeron-spy:aeron:udp", wantUDP: true},
		{name: "ipcx", channel: "aeron:ipcx", wantPassThru: true},
		{name: "ipcx with query", channel: "aeron:ipcx?alias=x", wantPassThru: true},
		{name: "ipc", channel: "aeron:ipc", wantPassThru: true},
		{name: "udp", channel: "aeron:udp?endpoint=localhost:0", wantUDP: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantUDP, ingressChannelIsUDP(tc.channel))
			require.Equal(t, tc.wantPassThru, ingressChannelPassthrough(tc.channel))
		})
	}
}

func TestIngressChannelHasEndpointParam(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{name: "udp endpoint", channel: "aeron:udp?endpoint=localhost:0", want: true},
		{
			name:    "spy udp endpoint",
			channel: "aeron-spy:aeron:udp?endpoint=localhost:0",
			want:    true,
		},
		{
			name:    "piped endpoint",
			channel: "aeron:udp?ttl=16|endpoint=localhost:0",
			want:    true,
		},
		{name: "udp ttl only", channel: "aeron:udp?ttl=16", want: false},
		{name: "ipc", channel: "aeron:ipc", want: false},
		{name: "ipcx alias", channel: "aeron:ipcx?alias=ingress", want: false},
		{
			name:    "alias false positive",
			channel: "aeron:udp?alias=endpoint=localhost:0",
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ingressChannelEndpointParamIndex(tc.channel) >= 0
			require.Equal(t, tc.want, got, tc.channel)
		})
	}
}

func TestIngressChannelQueryEndpointParamIndex(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{name: "udp endpoint", channel: "aeron:udp?endpoint=localhost:0", want: true},
		{
			name:    "piped endpoint",
			channel: "aeron:udp?ttl=16|endpoint=localhost:0",
			want:    false,
		},
		{
			name:    "alias false positive",
			channel: "aeron:udp?alias=endpoint=localhost:0",
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ingressChannelQueryEndpointParamIndex(tc.channel) >= 0
			require.Equal(t, tc.want, got, tc.channel)
		})
	}
}

func TestBuildIngressChannel(t *testing.T) {
	const passthroughEndpoint = "localhost:20110"

	t.Run("passthrough", func(t *testing.T) {
		tests := []struct {
			name    string
			channel string
		}{
			{name: "ipc", channel: "aeron:ipc"},
			{name: "ipcx", channel: "aeron:ipcx"},
			{name: "ipcx with query", channel: "aeron:ipcx?alias=ingress"},
			{name: "spy ipc", channel: "aeron-spy:aeron:ipc"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := BuildIngressChannel(tc.channel, passthroughEndpoint)
				require.NoError(t, err)
				require.Equal(t, tc.channel, got)
			})
		}
	})

	t.Run("udp", func(t *testing.T) {
		tests := []struct {
			name     string
			channel  string
			endpoint string
			want     string
		}{
			{
				name:     "replace",
				channel:  "aeron:udp?endpoint=localhost:0",
				endpoint: "192.168.1.10:20110",
				want:     "aeron:udp?endpoint=192.168.1.10:20110",
			},
			{
				name:     "append",
				channel:  "aeron:udp?ttl=16",
				endpoint: "host:20110",
				want:     "aeron:udp?ttl=16|endpoint=host:20110",
			},
			{
				name:     "bare append",
				channel:  "aeron:udp",
				endpoint: "host:20110",
				want:     "aeron:udp?endpoint=host:20110",
			},
			{
				name:     "bare query delimiter append",
				channel:  "aeron:udp?",
				endpoint: "host:20110",
				want:     "aeron:udp?endpoint=host:20110",
			},
			{
				name:     "alias false positive append",
				channel:  "aeron:udp?alias=endpoint=localhost:0",
				endpoint: "host:20110",
				want:     "aeron:udp?alias=endpoint=localhost:0|endpoint=host:20110",
			},
			{
				name:     "spy replace",
				channel:  "aeron-spy:aeron:udp?endpoint=localhost:0",
				endpoint: "host:20110",
				want:     "aeron-spy:aeron:udp?endpoint=host:20110",
			},
			{
				name:     "replace piped params",
				channel:  "aeron:udp?ttl=16|endpoint=localhost:0",
				endpoint: "host:20110",
				want:     "aeron:udp?ttl=16|endpoint=host:20110",
			},
			{
				name:     "replace endpoint before piped ttl",
				channel:  "aeron:udp?endpoint=localhost:0|ttl=16",
				endpoint: "host:20110",
				want:     "aeron:udp?endpoint=host:20110|ttl=16",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := BuildIngressChannel(tc.channel, tc.endpoint)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			name    string
			channel string
		}{
			{name: "unknown media", channel: "aeron:unknown"},
			{name: "missing aeron prefix", channel: "not-a-channel"},
			{name: "empty media", channel: "aeron:"},
		}

		const endpoint = "host:20110"
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := BuildIngressChannel(tc.channel, endpoint)
				require.Empty(t, got)
				require.EqualError(
					t,
					err,
					"cluster client: invalid ingress channel: "+tc.channel,
				)
			})
		}
	})
}
