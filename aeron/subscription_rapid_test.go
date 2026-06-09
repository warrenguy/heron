package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

type subImageOp int

const (
	subImageOpAdd subImageOp = iota
	subImageOpAddDup
	subImageOpRemove
	subImageOpCloseImage
	subImageOpCloseSubscription
)

func drawSubImageOp(rt *rapid.T, step int, hasImages, subClosed bool) subImageOp {
	if !subClosed && rapid.IntRange(0, 19).Draw(rt, fmt.Sprintf("closeRoll%d", step)) == 0 {
		return subImageOpCloseSubscription
	}
	if hasImages {
		op := rapid.IntRange(0, int(subImageOpCloseImage)).Draw(rt, fmt.Sprintf("op%d", step))
		return subImageOp(op)
	}
	return subImageOpAdd
}

func subscriptionHasOpenImage(images map[int64]*aeron.Image) bool {
	for _, image := range images {
		if image != nil && !image.IsClosed() {
			return true
		}
	}
	return false
}

func pickCorrelationID(rt *rapid.T, images map[int64]*aeron.Image, name string) int64 {
	rt.Helper()

	ids := make([]int64, 0, len(images))
	for correlationID := range images {
		ids = append(ids, correlationID)
	}
	idx := rapid.IntRange(0, len(ids)-1).Draw(rt, name)
	return ids[idx]
}

func TestSubscription_imageMapInvariants_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		sub := testSubscription(t)

		opCount := rapid.IntRange(1, 12).Draw(rt, "opCount")
		images := make(map[int64]*aeron.Image)

		for step := range opCount {
			op := drawSubImageOp(rt, step, len(images) > 0, sub.IsClosed())
			countBefore := sub.ImageCount()

			switch op {
			case subImageOpAdd:
				correlationID := aeron.TestImageCorrelationID + int64(step) + 1
				joinPos := aeron.TestSubscriberPositionAtJoin + int64(step)*64
				image := aeron.NewImageForTestWithCorrelationID(t, sub, joinPos, correlationID)
				aeron.SubscriptionAddImageForTest(sub, image)
				images[correlationID] = image

			case subImageOpAddDup:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("dupCorr%d", step))
				original := images[correlationID]
				joinPos := aeron.TestSubscriberPositionAtJoin + int64(step)*64 + 1
				dup := aeron.NewImageForTestWithCorrelationID(t, sub, joinPos, correlationID)
				aeron.SubscriptionAddImageForTest(sub, dup)
				require.Equal(rt, countBefore, sub.ImageCount())
				require.Same(rt, original, images[correlationID])

			case subImageOpRemove:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("rmCorr%d", step))
				removed := aeron.SubscriptionRemoveImageForTest(sub, correlationID)
				require.Same(rt, images[correlationID], removed)
				delete(images, correlationID)

			case subImageOpCloseImage:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("closeCorr%d", step))
				aeron.ImageInternalCloseForTest(images[correlationID])

			case subImageOpCloseSubscription:
				require.NoError(rt, sub.Close())
				require.True(rt, sub.IsClosed())
			}

			require.Equal(rt, len(images), sub.ImageCount())
			wantConnected := !sub.IsClosed() && subscriptionHasOpenImage(images)
			require.Equal(rt, wantConnected, sub.IsConnected())
		}
	})
}
