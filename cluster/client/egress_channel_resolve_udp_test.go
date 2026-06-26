//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestResolveResponseChannel_udpSubscription(t *testing.T) {
	const resolvedBind = "127.0.0.1:54321"

	type setupFunc func(t *testing.T, sub *aeron.Subscription)

	tests := []struct {
		name    string
		channel string
		setup   setupFunc
		want    string
		wantOK  bool
	}{
		{
			name:    "fixed endpoint",
			channel: "aeron:udp?endpoint=localhost:20111",
			want:    "aeron:udp?endpoint=localhost:20111",
			wantOK:  true,
		},
		{
			name:    "wildcard unresolved",
			channel: "aeron:udp?endpoint=localhost:0",
			setup: func(t *testing.T, sub *aeron.Subscription) {
				aeron.WireSubscriptionUDPWildcardForHooks(
					sub,
					aeron.NewWildcardUDPReceiveCountersForHooks(
						t,
						"aeron:udp?endpoint=localhost:0",
						0,
						"",
					),
					false,
				)
			},
			wantOK: false,
		},
		{
			name:    "wildcard resolved",
			channel: "aeron:udp?endpoint=localhost:0|term-length=64k",
			setup: func(t *testing.T, sub *aeron.Subscription) {
				applyResolvedUDPWildcardForTest(t, sub, resolvedBind)
			},
			want:   "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k",
			wantOK: true,
		},
		{
			name:    "spy udp wildcard resolved strips prefix",
			channel: "aeron-spy:aeron:udp?endpoint=localhost:0|term-length=64k",
			setup: func(t *testing.T, sub *aeron.Subscription) {
				applyResolvedUDPWildcardForTest(t, sub, resolvedBind)
			},
			want:   "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k",
			wantOK: true,
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
			sub := aeron.NewSubscriptionForHooks(
				aeron.TestHookSubscriptionRegistrationID,
				aeron.TestHookSubscriptionStreamID,
				tc.channel,
			)
			if tc.setup != nil {
				tc.setup(t, sub)
			}

			got, ok := resolveResponseChannel(sub)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func applyResolvedUDPWildcardForTest(t *testing.T, sub *aeron.Subscription, bindAddress string) {
	t.Helper()

	aeron.WireSubscriptionUDPWildcardForHooks(
		sub,
		aeron.NewWildcardUDPReceiveCountersForHooks(
			t,
			"aeron:udp?endpoint=localhost:0",
			0,
			bindAddress,
		),
		true,
	)
}
