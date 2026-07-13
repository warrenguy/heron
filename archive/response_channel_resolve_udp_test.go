//go:build testhooks

package archive

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
)

func TestResolveResponseChannel_udpSubscription(t *testing.T) {
	const resolvedBind = "127.0.0.1:54321"

	tests := []struct {
		name          string
		channel       string
		wireWildcard  bool
		bindAddress   string
		wildcardReady bool
		want          string
		wantOK        bool
	}{
		{
			name:    "fixed endpoint",
			channel: "aeron:udp?endpoint=localhost:20111",
			want:    "aeron:udp?endpoint=localhost:20111",
			wantOK:  true,
		},
		{
			name:         "wildcard unresolved",
			channel:      "aeron:udp?endpoint=localhost:0",
			wireWildcard: true,
			wantOK:       false,
		},
		{
			name:          "wildcard resolved",
			channel:       "aeron:udp?endpoint=localhost:0|term-length=64k",
			wireWildcard:  true,
			bindAddress:   resolvedBind,
			wildcardReady: true,
			want:          "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k",
			wantOK:        true,
		},
		{
			name:          "spy udp wildcard resolved strips prefix",
			channel:       "aeron-spy:aeron:udp?endpoint=localhost:0|term-length=64k",
			wireWildcard:  true,
			bindAddress:   resolvedBind,
			wildcardReady: true,
			want:          "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k",
			wantOK:        true,
		},
		{
			name:    "spy udp fixed endpoint strips prefix",
			channel: "aeron-spy:aeron:udp?endpoint=localhost:20111",
			want:    "aeron:udp?endpoint=localhost:20111",
			wantOK:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sub := aeron.NewSubscriptionForHooks(
				aeron.TestHookSubscriptionRegistrationID,
				aeron.TestHookSubscriptionStreamID,
				tc.channel,
			)
			if tc.wireWildcard {
				applyUDPWildcardForTest(t, sub, tc.bindAddress, tc.wildcardReady)
			}

			requireResolveResponseChannel(t, sub, tc.want, tc.wantOK)
		})
	}
}

func applyUDPWildcardForTest(
	t *testing.T,
	sub *aeron.Subscription,
	bindAddress string,
	ready bool,
) {
	t.Helper()

	aeron.WireSubscriptionUDPWildcardForHooks(
		sub,
		aeron.NewWildcardUDPReceiveCountersForHooks(
			t,
			"aeron:udp?endpoint=localhost:0",
			0,
			bindAddress,
		),
		ready,
	)
}
