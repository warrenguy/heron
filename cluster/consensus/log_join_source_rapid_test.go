//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

// drawFakeLogJoinImages builds fake SubscriptionImages with a drawn image set.
// Nil-slot and closed entries are included so both walk filters are exercised.
func drawFakeLogJoinImages(t *testing.T, rt *rapid.T) *fakeSubscriptionImages {
	rt.Helper()

	count := rapid.IntRange(0, 6).Draw(rt, "imageCount")
	sub := aeron.NewTestSubscriptionForHooks(t)
	images := make([]*aeron.Image, count)
	for i := range count {
		nilSlot := rapid.Bool().Draw(rt, "nilSlot")
		if nilSlot {
			images[i] = nil
			continue
		}
		sessionID := rapid.Int32Range(1, 8).Draw(rt, "sessionID")
		joinPos := rapid.Int64Range(0, 1<<20).Draw(rt, "joinPosition")
		image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
			t,
			sub,
			sessionID,
			int64(i+1),
			joinPos,
		)
		if rapid.Bool().Draw(rt, "closed") {
			aeron.ImageInternalCloseForHooks(image)
		}
		images[i] = image
	}

	return &fakeSubscriptionImages{
		channel:  "aeron:ipc",
		streamID: rapid.Int32Range(1, 1<<20).Draw(rt, "streamID"),
		images:   images,
	}
}

// firstMatchLogJoinImage is the selection oracle: first non-nil, non-closed
// image whose session matches, in slice order.
func firstMatchLogJoinImage(
	images []*aeron.Image,
	logSessionID int32,
) *aeron.Image {
	for _, image := range images {
		if image != nil && !image.IsClosed() && image.SessionID() == logSessionID {
			return image
		}
	}
	return nil
}

// TestSubscriptionLogJoinSource_selection_rapid checks Image and Ready against
// a first-match model over drawn image sets on fake SubscriptionImages.
func TestSubscriptionLogJoinSource_selection_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		fake := drawFakeLogJoinImages(t, rt)
		s := NewSubscriptionLogJoinSource(fake)
		query := rapid.Int32Range(1, 8).Draw(rt, "querySession")

		want := firstMatchLogJoinImage(fake.images, query)
		got := s.Image(query)
		require.Same(rt, want, got)

		params, ok := s.Ready(query)
		require.Equal(rt, want != nil, ok)
		if want == nil {
			require.Equal(rt, FollowerLogJoinParams{}, params)
			return
		}
		require.Equal(rt, FollowerLogJoinParams{
			LogChannel:      fake.channel,
			LogStreamID:     fake.streamID,
			LogSessionID:    query,
			LogJoinPosition: want.JoinPosition(),
		}, params)
		require.Same(rt, s.Image(query), got)
	})
}

// isOpenLogJoinImage reports whether the model holds a non-closed image for
// session. The model map never stores nil.
func isOpenLogJoinImage(images map[int32]*aeron.Image, session int32) bool {
	image, ok := images[session]
	return ok && !image.IsClosed()
}

// TestAgentFollowerLogJoin_sequence_rapid checks Ready and Join against a model
// over drawn wire, adapter, image, and join operation sequences.
func TestAgentFollowerLogJoin_sequence_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const (
			seedLeader int32 = 2
			seedTerm   int64 = 5
		)

		sub := aeron.NewTestSubscriptionForHooks(t)
		src := NewSubscriptionLogJoinSource(sub)
		images := make(map[int32]*aeron.Image)
		correlation := int64(1)

		a := NewAgent(AgentConfig{
			Role:             RoleCandidate,
			LeaderMemberID:   seedLeader,
			LeadershipTermID: seedTerm,
		})

		sourceWired := false
		var currentAdapter *LogAdapter
		var lastJoinedSession int32
		lastJoinedSet := false
		joinedOnce := false

		nOps := rapid.IntRange(1, 20).Draw(rt, "nOps")
		for range nOps {
			op := rapid.SampledFrom([]string{
				"setSource",
				"clearSource",
				"installAdapter",
				"clearAdapter",
				"closeImage",
				"addImage",
				"ready",
				"join",
			}).Draw(rt, "op")
			session := rapid.Int32Range(1, 4).Draw(rt, "session")

			switch op {
			case "setSource":
				a.SetLogJoinSource(src)
				sourceWired = true
			case "clearSource":
				a.SetLogJoinSource(nil)
				sourceWired = false
			case "installAdapter":
				// Fresh adapter only: SetLogAdapter(nil) does not clear the
				// previous adapter's image, so re-using it would restore a
				// non-nil Image with no intervening join.
				currentAdapter = NewLogAdapter(0)
				a.SetLogAdapter(currentAdapter)
				lastJoinedSet = false
			case "clearAdapter":
				a.SetLogAdapter(nil)
				currentAdapter = nil
			case "closeImage":
				if isOpenLogJoinImage(images, session) {
					aeron.ImageInternalCloseForHooks(images[session])
				}
			case "addImage":
				if isOpenLogJoinImage(images, session) {
					break
				}
				image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
					t,
					sub,
					session,
					correlation,
					int64(session)*32,
				)
				correlation++
				require.True(rt, aeron.SubscriptionAddImageForHooks(sub, image))
				images[session] = image
			case "ready":
				_, got := a.FollowerLogJoinReady(session)
				open := isOpenLogJoinImage(images, session)
				require.Equal(rt, sourceWired && open, got)
			case "join":
				got := a.JoinLogAsFollower(
					FollowerLogJoinParams{LogSessionID: session},
					rapid.Int64Range(0, 1<<40).Draw(rt, "nowNs"),
				)
				open := isOpenLogJoinImage(images, session)
				want := sourceWired && currentAdapter != nil && open
				require.Equal(rt, want, got)
				if got {
					lastJoinedSession = session
					lastJoinedSet = true
					joinedOnce = true
				}
			}

			require.Equal(rt, seedLeader, a.LeaderMemberID())
			require.Equal(rt, seedTerm, a.LeadershipTermID())
			if joinedOnce {
				require.Equal(rt, RoleFollower, a.Role())
			}

			if currentAdapter != nil {
				bound := currentAdapter.Image()
				if lastJoinedSet {
					require.NotNil(rt, bound)
					require.Equal(rt, lastJoinedSession, bound.SessionID())
				} else {
					require.Nil(rt, bound)
				}
			}
		}
	})
}
