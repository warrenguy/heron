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
