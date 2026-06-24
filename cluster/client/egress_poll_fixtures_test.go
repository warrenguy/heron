//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

func setupEgressPollFixtures(
	t testing.TB,
) (*aeron.Subscription, *aeron.Image, atomic.AtomicBuffer) {
	t.Helper()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, image, aeron.ImageTermBufferForHooks(image)
}

func setupEgressAdapterPollFixtures(t testing.TB) (*aeron.Subscription, atomic.AtomicBuffer) {
	t.Helper()

	sub, _, termBuffer := setupEgressPollFixtures(t)
	return sub, termBuffer
}
