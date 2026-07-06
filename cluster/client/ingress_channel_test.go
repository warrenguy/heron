package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type ingressParamPresentCase struct {
	name    string
	channel string
	want    bool
}

func runIngressParamPresent(
	t *testing.T,
	presentFn func(string) bool,
	tests []ingressParamPresentCase,
) {
	t.Helper()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, presentFn(tc.channel), tc.channel)
		})
	}
}

type buildIngressCase struct {
	name     string
	channel  string
	endpoint string
	want     string
	wantErr  string
}

func runBuildIngress(t *testing.T, tests []buildIngressCase) {
	t.Helper()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.False(
				t,
				tc.want != "" && tc.wantErr != "",
				"test case %q: want and wantErr are mutually exclusive",
				tc.name,
			)

			got, err := BuildIngressChannel(tc.channel, tc.endpoint)
			if tc.wantErr != "" {
				require.Empty(t, got)
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.want != "" {
				require.Equal(t, tc.want, got)
				return
			}
			require.Equal(t, tc.channel, got)
		})
	}
}

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
			require.Equal(t, tc.wantUDP, IngressChannelIsUDP(tc.channel))
			require.Equal(t, tc.wantPassThru, IngressChannelPassthrough(tc.channel))
		})
	}
}

func TestIngressChannelHasEndpointParam(t *testing.T) {
	runIngressParamPresent(t, IngressChannelHasEndpointParam, []ingressParamPresentCase{
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
	})
}

func TestIngressChannelQueryHasEndpointParam(t *testing.T) {
	runIngressParamPresent(t, ingressChannelQueryHasEndpointParam, []ingressParamPresentCase{
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
	})
}

func TestBuildIngressChannel(t *testing.T) {
	const passthroughEndpoint = "localhost:20110"

	t.Run("passthrough", func(t *testing.T) {
		runBuildIngress(t, []buildIngressCase{
			{name: "ipc", channel: "aeron:ipc", endpoint: passthroughEndpoint},
			{name: "ipcx", channel: "aeron:ipcx", endpoint: passthroughEndpoint},
			{
				name:     "ipcx with query",
				channel:  "aeron:ipcx?alias=ingress",
				endpoint: passthroughEndpoint,
			},
			{name: "spy ipc", channel: "aeron-spy:aeron:ipc", endpoint: passthroughEndpoint},
			{name: "ipc empty endpoint", channel: "aeron:ipc", endpoint: ""},
			{name: "ipcx pipe in endpoint", channel: "aeron:ipcx", endpoint: "host|20110"},
		})
	})

	t.Run("udp", func(t *testing.T) {
		runBuildIngress(t, []buildIngressCase{
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
		})
	})

	t.Run("invalid member endpoint", func(t *testing.T) {
		runBuildIngress(t, []buildIngressCase{
			{
				name:     "empty",
				channel:  "aeron:udp?ttl=16",
				endpoint: "",
				wantErr:  "cluster client: ingress member endpoint is empty",
			},
			{
				name:     "pipe",
				channel:  "aeron:udp?ttl=16",
				endpoint: "host|20110",
				wantErr:  "cluster client: invalid ingress member endpoint: host|20110",
			},
			{
				name:     "question mark",
				channel:  "aeron:udp?ttl=16",
				endpoint: "host?20110",
				wantErr:  "cluster client: invalid ingress member endpoint: host?20110",
			},
			{
				name:     "equals",
				channel:  "aeron:udp?ttl=16",
				endpoint: "host=20110",
				wantErr:  "cluster client: invalid ingress member endpoint: host=20110",
			},
		})
	})

	t.Run("invalid", func(t *testing.T) {
		runBuildIngress(t, []buildIngressCase{
			{
				name:     "unknown media",
				channel:  "aeron:unknown",
				endpoint: "host:20110",
				wantErr:  "cluster client: invalid ingress channel: aeron:unknown",
			},
			{
				name:     "missing aeron prefix",
				channel:  "not-a-channel",
				endpoint: "host:20110",
				wantErr:  "cluster client: invalid ingress channel: not-a-channel",
			},
			{
				name:     "empty media",
				channel:  "aeron:",
				endpoint: "host:20110",
				wantErr:  "cluster client: invalid ingress channel: aeron:",
			},
		})
	})
}
