package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const (
	destinationTestChannel = "aeron:udp?endpoint=127.0.0.1:24610"
	manualControlChannel   = "aeron:udp?control-mode=manual"
)

var subscriptionDestinationOps = []struct {
	name string
	fn   func(*aeron.Subscription, string) error
}{
	{"Add", (*aeron.Subscription).AddDestination},
	{"Remove", (*aeron.Subscription).RemoveDestination},
}

func TestSubscription_destination_closed(t *testing.T) {
	for _, op := range subscriptionDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			sub := subscriptionWithChannel(manualControlChannel)
			require.NoError(t, sub.Close())

			err := op.fn(sub, destinationTestChannel)
			require.EqualError(t, err, "aeron: subscription is closed")
		})
	}
}

func TestSubscription_destination_unbound(t *testing.T) {
	for _, op := range subscriptionDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			sub := subscriptionWithChannel(manualControlChannel)

			err := op.fn(sub, destinationTestChannel)
			require.ErrorIs(t, err, aeron.ErrClientNotBound)
		})
	}
}

func TestSubscription_destination_clientClosed(t *testing.T) {
	for _, op := range subscriptionDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			sub := subscriptionWithChannel(manualControlChannel)
			client := aeron.NewClientForTest(nil, func() int64 { return 0 })
			client.PublishClosedForTest()
			aeron.BindClientForTest(sub, client)

			err := op.fn(sub, destinationTestChannel)
			require.ErrorIs(t, err, aeron.ErrClientClosed)
		})
	}
}

func TestSubscription_destination_clientNotReady(t *testing.T) {
	// Conductor is nil: requireReadyWithDriverProxy returns not ready.
	for _, op := range subscriptionDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			sub := bindSubscriptionWithNotReadyClient()

			err := op.fn(sub, destinationTestChannel)
			require.ErrorIs(t, err, aeron.ErrClientNotReady)
		})
	}
}

func TestSubscription_destination_validateChannel(t *testing.T) {
	// Bind a non-ready client so channel validation runs before ready.
	cases := []struct {
		name    string
		channel string
		wantErr string
	}{
		{"empty", "", "aeron: channel is required"},
		{"non-ASCII high byte", "aeron:\x80", "aeron: channel must be ASCII"},
		{"non-ASCII utf8", "\u00fc", "aeron: channel must be ASCII"},
	}
	for _, op := range subscriptionDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			sub := bindSubscriptionWithNotReadyClient()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					err := op.fn(sub, tc.channel)
					require.EqualError(t, err, tc.wantErr)
				})
			}
		})
	}
}

func TestSubscription_TryResolveChannelEndpointPort_manualControlMode(t *testing.T) {
	sub := subscriptionWithChannel(manualControlChannel)

	got, ok := sub.TryResolveChannelEndpointPort()
	require.True(t, ok)
	require.Equal(t, manualControlChannel, got)
}

func bindSubscriptionWithNotReadyClient() *aeron.Subscription {
	sub := subscriptionWithChannel(manualControlChannel)
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })
	aeron.BindClientForTest(sub, client)
	return sub
}
