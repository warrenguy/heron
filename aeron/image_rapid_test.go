package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

func TestImage_joinAndLivePosition_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		joinPosition := rapid.Int64().Draw(rt, "joinPosition")

		sub := testSubscription(t)
		image := aeron.NewImageForTest(t, sub, joinPosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, joinPosition, image.Position())

		livePosition := rapid.Int64().Draw(rt, "livePosition")
		aeron.ImageSetSubscriberPositionForTest(image, livePosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, livePosition, image.Position())

		aeron.ImageInternalCloseForTest(image)

		postClosePosition := rapid.Int64().Draw(rt, "postClosePosition")
		aeron.ImageSetSubscriberPositionForTest(image, postClosePosition)

		require.Equal(rt, livePosition, image.Position())
		require.Equal(rt, joinPosition, image.JoinPosition())
	})
}
