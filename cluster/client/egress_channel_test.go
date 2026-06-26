package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type stubResponseChannelSub struct {
	channel string
}

func (s stubResponseChannelSub) Channel() string { return s.channel }

func (s stubResponseChannelSub) TryResolveChannelEndpointPort() (string, bool) {
	return s.channel, true
}

func TestResolveResponseChannel(t *testing.T) {
	t.Run("nil sub", func(t *testing.T) {
		got, ok := resolveResponseChannel(nil)
		require.Empty(t, got)
		require.False(t, ok)
	})

	tests := []struct {
		name   string
		sub    stubResponseChannelSub
		want   string
		wantOK bool
	}{
		{name: "empty channel rejected", sub: stubResponseChannelSub{}, wantOK: false},
		{
			name:   "ipc",
			sub:    stubResponseChannelSub{channel: "aeron:ipc"},
			want:   "aeron:ipc",
			wantOK: true,
		},
		{
			name:   "spy ipc strips prefix",
			sub:    stubResponseChannelSub{channel: "aeron-spy:aeron:ipc"},
			want:   "aeron:ipc",
			wantOK: true,
		},
		{
			name:   "ipcx",
			sub:    stubResponseChannelSub{channel: "aeron:ipcx?alias=egress"},
			want:   "aeron:ipcx?alias=egress",
			wantOK: true,
		},
		{
			name:   "spy ipcx with params",
			sub:    stubResponseChannelSub{channel: "aeron-spy:aeron:ipcx?alias=egress"},
			want:   "aeron:ipcx?alias=egress",
			wantOK: true,
		},
		{
			name:   "unknown media",
			sub:    stubResponseChannelSub{channel: "aeron:unknown"},
			wantOK: false,
		},
		{
			name:   "spy udp fixed endpoint strips prefix",
			sub:    stubResponseChannelSub{channel: "aeron-spy:aeron:udp?endpoint=localhost:20111"},
			want:   "aeron:udp?endpoint=localhost:20111",
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveResponseChannel(tc.sub)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

type applyUDPRejoinFalseWantCase struct {
	name    string
	channel string
	want    string
}

func TestValidateClusterEgressChannel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		channel string
	}{
		{name: "ipc", channel: "aeron:ipc"},
		{name: "ipcx", channel: "aeron:ipcx?alias=egress"},
		{name: "udp", channel: "aeron:udp?endpoint=localhost:0"},
		{name: "spy udp", channel: "aeron-spy:aeron:udp?endpoint=localhost:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, validateClusterEgressChannel(tc.channel))
		})
	}

	t.Run("unknown media", func(t *testing.T) {
		err := validateClusterEgressChannel("aeron:unknown")
		require.EqualError(
			t, err,
			"cluster client: unknown egress response channel not supported; use ipc, ipcx, or udp",
		)
	})

	t.Run("invalid channel", func(t *testing.T) {
		const channel = "not-a-channel"
		err := validateClusterEgressChannel(channel)
		require.EqualError(t, err, "cluster client: invalid egress channel: "+channel)
	})
}

func TestApplyUDPRejoinFalse(t *testing.T) {
	runWant := func(t *testing.T, tests []applyUDPRejoinFalseWantCase) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := applyUDPRejoinFalse(tc.channel)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	}

	t.Run("udp", func(t *testing.T) {
		runWant(t, []applyUDPRejoinFalseWantCase{
			{name: "bare append", channel: "aeron:udp", want: "aeron:udp?rejoin=false"},
			{name: "bare query delimiter append", channel: "aeron:udp?", want: "aeron:udp?rejoin=false"},
			{
				name:    "append after query param",
				channel: "aeron:udp?endpoint=localhost:0",
				want:    "aeron:udp?endpoint=localhost:0|rejoin=false",
			},
			{name: "replace query rejoin", channel: "aeron:udp?rejoin=true", want: "aeron:udp?rejoin=false"},
			{
				name:    "replace piped rejoin",
				channel: "aeron:udp?ttl=16|rejoin=true",
				want:    "aeron:udp?ttl=16|rejoin=false",
			},
			{
				name:    "replace rejoin before piped param",
				channel: "aeron:udp?rejoin=true|ttl=16",
				want:    "aeron:udp?rejoin=false|ttl=16",
			},
			{
				name:    "replace empty rejoin value",
				channel: "aeron:udp?rejoin=",
				want:    "aeron:udp?rejoin=false",
			},
			{
				name:    "alias false positive append",
				channel: "aeron:udp?alias=rejoin=true",
				want:    "aeron:udp?alias=rejoin=true|rejoin=false",
			},
			{
				name:    "spy replace",
				channel: "aeron-spy:aeron:udp?rejoin=true",
				want:    "aeron-spy:aeron:udp?rejoin=false",
			},
			{
				name:    "spy bare append",
				channel: "aeron-spy:aeron:udp",
				want:    "aeron-spy:aeron:udp?rejoin=false",
			},
			{
				name:    "idempotent query rejoin false",
				channel: "aeron:udp?rejoin=false",
				want:    "aeron:udp?rejoin=false",
			},
			{
				name:    "idempotent piped rejoin false",
				channel: "aeron:udp?ttl=16|rejoin=false",
				want:    "aeron:udp?ttl=16|rejoin=false",
			},
		})
	})

	t.Run("passthrough", func(t *testing.T) {
		runWant(t, []applyUDPRejoinFalseWantCase{
			{name: "ipc", channel: "aeron:ipc", want: "aeron:ipc"},
			{name: "ipcx", channel: "aeron:ipcx?alias=egress", want: "aeron:ipcx?alias=egress"},
			{name: "spy ipc", channel: "aeron-spy:aeron:ipc", want: "aeron-spy:aeron:ipc"},
			{name: "unknown media", channel: "aeron:unknown", want: "aeron:unknown"},
		})
	})

	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			name    string
			channel string
		}{
			{name: "missing aeron prefix", channel: "not-a-channel"},
			{name: "empty media", channel: "aeron:"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := applyUDPRejoinFalse(tc.channel)
				require.Empty(t, got)
				require.EqualError(t, err, "cluster client: invalid egress channel: "+tc.channel)
			})
		}
	})
}
