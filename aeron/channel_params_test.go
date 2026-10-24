package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestChannelEndpointNeedsWildcardPortResolution(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{name: "no endpoint", channel: "aeron:udp?ttl=16", want: false},
		{name: "fixed endpoint", channel: "aeron:udp?endpoint=localhost:20111", want: false},
		{name: "wildcard endpoint", channel: "aeron:udp?endpoint=localhost:0", want: true},
		{name: "piped wildcard", channel: "aeron:udp?ttl=16|endpoint=localhost:0", want: true},
		{name: "alias false positive", channel: "aeron:udp?alias=endpoint=localhost:0", want: false},
		{name: "spy wildcard", channel: "aeron-spy:aeron:udp?endpoint=localhost:0", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.ChannelEndpointNeedsWildcardPortResolution(tc.channel))
		})
	}
}

func TestIsControlModeResponse(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{
			name:    "response first",
			channel: "aeron:udp?control-mode=response|control=localhost:10002",
			want:    true,
		},
		{
			name:    "piped response",
			channel: "aeron:udp?control=localhost:10002|control-mode=response",
			want:    true,
		},
		{name: "manual", channel: "aeron:udp?control-mode=manual", want: false},
		{name: "absent", channel: "aeron:udp?endpoint=localhost:8010", want: false},
		{name: "value prefix", channel: "aeron:udp?control-mode=responsex", want: false},
		{name: "alias false positive", channel: "aeron:udp?alias=control-mode=response", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.IsControlModeResponse(tc.channel))
		})
	}
}

func TestSetChannelParam(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		param   string
		value   string
		want    string
	}{
		{
			name:    "append to bare media",
			channel: "aeron:udp",
			param:   "mtu=",
			value:   "1408",
			want:    "aeron:udp?mtu=1408",
		},
		{
			name:    "append after existing query",
			channel: "aeron:udp?endpoint=127.0.0.1:1",
			param:   "mtu=",
			value:   "1408",
			want:    "aeron:udp?endpoint=127.0.0.1:1|mtu=1408",
		},
		{
			name:    "replace first param",
			channel: "aeron:udp?term-length=64k|endpoint=127.0.0.1:1",
			param:   "term-length=",
			value:   "65536",
			want:    "aeron:udp?term-length=65536|endpoint=127.0.0.1:1",
		},
		{
			name:    "replace middle param",
			channel: "aeron:udp?endpoint=127.0.0.1:1|mtu=1200|ttl=16",
			param:   "mtu=",
			value:   "1408",
			want:    "aeron:udp?endpoint=127.0.0.1:1|mtu=1408|ttl=16",
		},
		{
			name:    "replace trailing param",
			channel: "aeron:udp?endpoint=127.0.0.1:1|mtu=1200",
			param:   "mtu=",
			value:   "1408",
			want:    "aeron:udp?endpoint=127.0.0.1:1|mtu=1408",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.SetChannelParam(tc.channel, tc.param, tc.value))
		})
	}
}

func TestReplaceChannelEndpoint(t *testing.T) {
	const resolvedEndpoint = "127.0.0.1:54321"

	tests := []struct {
		name     string
		channel  string
		endpoint string
		want     string
	}{
		{
			name:     "replace query endpoint",
			channel:  "aeron:udp?endpoint=localhost:0",
			endpoint: resolvedEndpoint,
			want:     "aeron:udp?endpoint=127.0.0.1:54321",
		},
		{
			name:     "replace piped endpoint",
			channel:  "aeron:udp?endpoint=localhost:0|term-length=64k",
			endpoint: resolvedEndpoint,
			want:     "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k",
		},
		{
			name:     "no endpoint param",
			channel:  "aeron:udp?ttl=16",
			endpoint: resolvedEndpoint,
			want:     "aeron:udp?ttl=16",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.ReplaceChannelEndpoint(tc.channel, tc.endpoint))
		})
	}
}
