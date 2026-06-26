package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
)

const wildcardUDPChannel = "aeron:udp?endpoint=localhost:0"

func TestSubscription_TryResolveChannelEndpointPort(t *testing.T) {
	t.Run("nil subscription", func(t *testing.T) {
		var sub *aeron.Subscription
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("closed subscription", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		require.NoError(t, sub.Close())
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("fixed endpoint passthrough", func(t *testing.T) {
		const channel = "aeron:udp?endpoint=localhost:20111"
		sub := subscriptionWithChannel(channel)
		got, ok := sub.TryResolveChannelEndpointPort()
		require.True(t, ok)
		require.Equal(t, channel, got)
	})

	t.Run("wildcard counters not yet wired", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("wildcard nil counters", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		aeron.SubscriptionApplyReadyForTest(sub, 0)
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("wildcard inactive receive channel status", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		applyWildcardWithCounters(t, sub, "")
		aeron.SubscriptionSetReceiveChannelStatusForTest(sub, 0)
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("wildcard unresolved without active endpoint", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		applyWildcardWithCounters(t, sub, "")
		requireTryResolveUnresolved(t, sub)
	})

	t.Run("wildcard resolved", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel + "|term-length=64k")
		applyWildcardWithCounters(t, sub, "127.0.0.1:54321")
		aeron.SubscriptionSetReceiveChannelStatusForTest(sub, cnc.ChannelEndpointStatusActive)

		got, ok := sub.TryResolveChannelEndpointPort()
		require.True(t, ok)
		require.Equal(t, "aeron:udp?endpoint=127.0.0.1:54321|term-length=64k", got)
	})
}

func TestSubscription_ChannelStatus(t *testing.T) {
	t.Run("null counter passthrough", func(t *testing.T) {
		sub := subscriptionWithChannel("aeron:ipc")
		status, err := sub.ChannelStatus()
		require.NoError(t, err)
		require.Equal(t, cnc.ChannelEndpointStatusActive, status)
	})

	t.Run("active receive counter", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		applyWildcardWithCounters(t, sub, "")
		aeron.SubscriptionSetReceiveChannelStatusForTest(sub, cnc.ChannelEndpointStatusActive)

		status, err := sub.ChannelStatus()
		require.NoError(t, err)
		require.Equal(t, cnc.ChannelEndpointStatusActive, status)
	})

	t.Run("nil counters error", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		aeron.SubscriptionApplyReadyForTest(sub, 0)

		_, err := sub.ChannelStatus()
		require.EqualError(t, err, "aeron: subscription counters view is nil")
	})

	t.Run("closed subscription", func(t *testing.T) {
		sub := subscriptionWithChannel(wildcardUDPChannel)
		require.NoError(t, sub.Close())

		_, err := sub.ChannelStatus()
		require.EqualError(t, err, "aeron: subscription is closed")
	})
}

func subscriptionWithChannel(channel string) *aeron.Subscription {
	return aeron.NewSubscriptionForTest(
		aeron.TestSubscriptionRegistrationID,
		aeron.TestPublicationStreamID,
		channel,
	)
}

func applyWildcardWithCounters(tb testing.TB, sub *aeron.Subscription, bindAddress string) {
	tb.Helper()
	aeron.WireSubscriptionUDPWildcardForHooks(
		sub,
		aeron.NewWildcardUDPReceiveCountersForHooks(tb, wildcardUDPChannel, 0, bindAddress),
		false,
	)
}

func requireTryResolveUnresolved(tb testing.TB, sub *aeron.Subscription) {
	tb.Helper()
	got, ok := sub.TryResolveChannelEndpointPort()
	require.False(tb, ok)
	require.Empty(tb, got)
}
