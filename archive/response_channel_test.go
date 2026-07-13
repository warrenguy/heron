package archive

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

func requireResolveResponseChannel(
	t *testing.T,
	sub responseChannelResolver,
	want string,
	wantOK bool,
) {
	t.Helper()
	got, ok := resolveResponseChannel(sub)
	require.Equal(t, wantOK, ok)
	if !wantOK {
		require.Empty(t, got)
		return
	}
	require.Equal(t, want, got)
}

func requireApplyUDPRejoinFalse(t *testing.T, channel, want string) {
	t.Helper()
	got, err := applyUDPRejoinFalse(channel)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func requireApplyUDPRejoinFalseError(t *testing.T, channel string) {
	t.Helper()
	got, err := applyUDPRejoinFalse(channel)
	require.Empty(t, got)
	require.EqualError(t, err, "archive: invalid control response channel: "+channel)
}

func TestResolveResponseChannel(t *testing.T) {
	t.Run("nil sub", func(t *testing.T) {
		t.Parallel()
		requireResolveResponseChannel(t, nil, "", false)
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
			sub:    stubResponseChannelSub{channel: "aeron:ipcx?alias=response"},
			want:   "aeron:ipcx?alias=response",
			wantOK: true,
		},
		{
			name:   "spy ipcx with params",
			sub:    stubResponseChannelSub{channel: "aeron-spy:aeron:ipcx?alias=response"},
			want:   "aeron:ipcx?alias=response",
			wantOK: true,
		},
		{
			name:   "unknown media",
			sub:    stubResponseChannelSub{channel: "aeron:unknown"},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireResolveResponseChannel(t, tc.sub, tc.want, tc.wantOK)
		})
	}
}

func TestValidateIngressChannel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		role    string
		channel string
	}{
		{name: "request ipc", role: "request", channel: "aeron:ipc"},
		{name: "request udp", role: "request", channel: "aeron:udp?endpoint=localhost:8010"},
		{name: "response ipc", role: "response", channel: "aeron:ipc"},
		{name: "response ipcx", role: "response", channel: "aeron:ipcx?alias=response"},
		{name: "response udp", role: "response", channel: "aeron:udp?endpoint=localhost:0"},
		{name: "response spy udp", role: "response", channel: "aeron-spy:aeron:udp?endpoint=localhost:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, validateArchiveIngressChannel(tc.channel, tc.role))
		})
	}

	t.Run("unsupported response media", func(t *testing.T) {
		t.Parallel()
		err := validateArchiveIngressChannel("aeron:unknown", "response")
		require.EqualError(
			t, err,
			"archive: unknown control response channel not supported; use ipc, ipcx, or udp",
		)
	})

	for _, tc := range []struct {
		name string
		role string
	}{
		{name: "invalid request channel", role: "request"},
		{name: "invalid response channel", role: "response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const channel = "not-a-channel"
			err := validateArchiveIngressChannel(channel, tc.role)
			require.EqualError(t, err, "archive: invalid control "+tc.role+" channel: "+channel)
		})
	}
}

func TestApplyUDPRejoinFalse(t *testing.T) {
	t.Run("udp", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			channel string
			want    string
		}{
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
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireApplyUDPRejoinFalse(t, tc.channel, tc.want)
			})
		}
	})

	t.Run("passthrough", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			channel string
			want    string
		}{
			{name: "ipc", channel: "aeron:ipc", want: "aeron:ipc"},
			{name: "ipcx", channel: "aeron:ipcx?alias=response", want: "aeron:ipcx?alias=response"},
			{name: "spy ipc", channel: "aeron-spy:aeron:ipc", want: "aeron-spy:aeron:ipc"},
			{name: "unknown media", channel: "aeron:unknown", want: "aeron:unknown"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireApplyUDPRejoinFalse(t, tc.channel, tc.want)
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			channel string
		}{
			{name: "missing aeron prefix", channel: "not-a-channel"},
			{name: "empty media", channel: "aeron:"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireApplyUDPRejoinFalseError(t, tc.channel)
			})
		}
	})
}
