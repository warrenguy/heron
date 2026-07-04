//go:build testhooks

package client

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
)

func appendClonedFragmentHandler(dst *[][]byte) aeron.FragmentHandler {
	return func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		_ *aeron.Header,
	) {
		*dst = append(
			*dst,
			slices.Clone(buffer.Bytes()[offset:offset+length]),
		)
	}
}

func addImageWithSessionID(
	t *testing.T,
	sub *aeron.Subscription,
	sessionID int32,
	correlationID int64,
) *aeron.Image {
	t.Helper()

	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		sessionID,
		correlationID,
		128,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

func TestFindReplayImage_table(t *testing.T) {
	const (
		matchSessionID   int32 = 4242
		otherSessionID   int32 = 5150
		replaySessionID  int64 = 4242
		missingSessionID int64 = 9999
	)

	tests := []struct {
		name          string
		setup         func(t *testing.T) *aeron.Subscription
		wantReplayID  int64
		wantFound     bool
		wantSessionID int32
		wantErr       string
	}{
		{
			name: "emptySubscription",
			setup: func(t *testing.T) *aeron.Subscription {
				return aeron.NewSubscriptionForHooks(2, 1201, "aeron:ipc")
			},
			wantReplayID: missingSessionID,
			wantFound:    false,
		},
		{
			name: "matchingSessionId",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(3, 1201, "aeron:ipc")
				addImageWithSessionID(t, local, matchSessionID, 100)
				return local
			},
			wantReplayID:  replaySessionID,
			wantFound:     true,
			wantSessionID: matchSessionID,
		},
		{
			name: "compositeReplaySessionId",
			setup: func(t *testing.T) *aeron.Subscription {
				const compositeSessionID int32 = 4242
				local := aeron.NewSubscriptionForHooks(11, 1201, "aeron:ipc")
				addImageWithSessionID(t, local, compositeSessionID, 108)
				return local
			},
			wantReplayID:  (int64(7) << 32) | int64(matchSessionID),
			wantFound:     true,
			wantSessionID: matchSessionID,
		},
		{
			name: "noMatchingSessionId",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(4, 1201, "aeron:ipc")
				addImageWithSessionID(t, local, otherSessionID, 101)
				return local
			},
			wantReplayID: missingSessionID,
			wantFound:    false,
		},
		{
			name: "firstMatchWins",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(5, 1201, "aeron:ipc")
				addImageWithSessionID(t, local, matchSessionID, 102)
				addImageWithSessionID(t, local, matchSessionID, 103)
				return local
			},
			wantReplayID:  replaySessionID,
			wantFound:     true,
			wantSessionID: matchSessionID,
		},
		{
			name: "skipsNilImageSlot",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(6, 1201, "aeron:ipc")
				aeron.SubscriptionInjectNilImageAtIndexForHooks(local, 0)
				addImageWithSessionID(t, local, matchSessionID, 104)
				return local
			},
			wantReplayID:  replaySessionID,
			wantFound:     true,
			wantSessionID: matchSessionID,
		},
		{
			name: "skipsClosedMatchingSessionId",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(7, 1201, "aeron:ipc")
				closed := addImageWithSessionID(t, local, matchSessionID, 105)
				aeron.ImageInternalCloseForHooks(closed)
				return local
			},
			wantReplayID: replaySessionID,
			wantFound:    false,
		},
		{
			name: "skipsClosedImageFindsOpenMatch",
			setup: func(t *testing.T) *aeron.Subscription {
				local := aeron.NewSubscriptionForHooks(10, 1201, "aeron:ipc")
				closed := addImageWithSessionID(t, local, matchSessionID, 106)
				aeron.ImageInternalCloseForHooks(closed)
				addImageWithSessionID(t, local, otherSessionID, 107)
				return local
			},
			wantReplayID:  int64(otherSessionID),
			wantFound:     true,
			wantSessionID: otherSessionID,
		},
		{
			name: "nilSubscription",
			setup: func(t *testing.T) *aeron.Subscription {
				return nil
			},
			wantReplayID: replaySessionID,
			wantErr:      "subscription is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := tt.setup(t)
			got, ok, err := FindReplayImage(local, tt.wantReplayID)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.False(t, ok)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantFound, ok)
			if tt.wantFound {
				require.NotNil(t, got)
				require.Equal(t, tt.wantSessionID, got.SessionID())
				return
			}
			require.Nil(t, got)
		})
	}
}

func TestSubscriptionInjectNilImageAtIndexForHooks_outOfRangeNoOp(t *testing.T) {
	sub := aeron.NewSubscriptionForHooks(9, 1201, "aeron:ipc")
	require.NotPanics(t, func() {
		aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, -1)
		aeron.SubscriptionInjectNilImageAtIndexForHooks(nil, 0)
		aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, sub.ImageCount()+1)
	})
	require.Equal(t, 0, sub.ImageCount())
}

func TestPollReplayFragments_table(t *testing.T) {
	sub := aeron.NewSubscriptionForHooks(8, 1201, "aeron:ipc")

	tests := []struct {
		name          string
		image         func(t *testing.T) (*aeron.Image, int64)
		want          int
		receivedStart int
		wantDone      bool
		wantCount     int
		wantErr       string
	}{
		{
			name: "incrementsReceived",
			image: func(t *testing.T) (*aeron.Image, int64) {
				image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
				return image, 0
			},
			want:      1,
			wantDone:  true,
			wantCount: 1,
		},
		{
			name: "stopPositionCompletesWithoutWant",
			image: func(t *testing.T) (*aeron.Image, int64) {
				image, frameLength := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
				return image, int64(frameLength)
			},
			want:      99,
			wantDone:  true,
			wantCount: 1,
		},
		{
			name: "wantNotMet",
			image: func(t *testing.T) (*aeron.Image, int64) {
				image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
				return image, 0
			},
			want:      2,
			wantDone:  false,
			wantCount: 1,
		},
		{
			name: "zeroWantZeroStopCompletesWithoutPoll",
			image: func(t *testing.T) (*aeron.Image, int64) {
				image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
				return image, 0
			},
			want:      0,
			wantDone:  true,
			wantCount: 0,
		},
		{
			name: "nilImage",
			image: func(t *testing.T) (*aeron.Image, int64) {
				return nil, 0
			},
			want:    1,
			wantErr: "image is nil",
		},
		{
			name: "closedImageIncomplete",
			image: func(t *testing.T) (*aeron.Image, int64) {
				return closedReplayImageForTest(t, sub), 0
			},
			want:    1,
			wantErr: "image is closed",
		},
		{
			name: "closedImageCompleteByStopPosition",
			image: func(t *testing.T) (*aeron.Image, int64) {
				return closedReplayImageAtStopPositionForTest(t)
			},
			want:      99,
			wantDone:  true,
			wantCount: 0,
		},
		{
			name: "closedImageCompleteByWant",
			image: func(t *testing.T) (*aeron.Image, int64) {
				local := aeron.NewSubscriptionForHooks(12, 1201, "aeron:ipc")
				return closedReplayImageForTest(t, local), 0
			},
			want:          1,
			receivedStart: 1,
			wantDone:      true,
			wantCount:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image, stopPosition := tt.image(t)
			var payloads [][]byte
			received, done, err := PollReplayFragments(
				image,
				tt.want,
				stopPosition,
				tt.receivedStart,
				appendClonedFragmentHandler(&payloads),
			)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDone, done)
			require.Equal(t, tt.wantCount, received)
			if tt.wantCount > 0 && tt.receivedStart == 0 {
				require.NotEmpty(t, payloads)
			}
		})
	}

	image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)

	t.Run("nilHandler", func(t *testing.T) {
		_, _, err := PollReplayFragments(image, 1, 0, 0, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "handler is nil")
	})
}

func closedReplayImageForTest(t *testing.T, sub *aeron.Subscription) *aeron.Image {
	t.Helper()

	image := addImageWithSessionID(t, sub, 777, 200)
	aeron.ImageInternalCloseForHooks(image)
	return image
}

func closedReplayImageAtStopPositionForTest(t *testing.T) (*aeron.Image, int64) {
	t.Helper()

	image, frameLength := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
	image.Poll(func(
		_ atomic.AtomicBuffer,
		_ int,
		_ int,
		_ *aeron.Header,
	) {
	}, 1)
	aeron.ImageInternalCloseForHooks(image)
	return image, int64(frameLength)
}
