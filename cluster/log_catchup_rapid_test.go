//go:build rapid && testhooks

package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
)

// prepareReseedModel is the reference state for the prepare reseed property.
type prepareReseedModel struct {
	// bound is the subscription the streams should hold. Nil after a double
	// failure.
	bound *aeron.Subscription
	// superseded holds every subscription a reseed replaced.
	superseded []*aeron.Subscription
	// image is the image the adapter should hold. Nil after a reseed.
	image *aeron.Image
	// imageOpen reports whether image is bound and open.
	imageOpen bool
	// missed reports whether the last join query since the last prepare missed.
	missed bool
}

// Asserts prepare replaces the log subscription exactly when the adapter holds
// no open image or the last join query missed. Draws random bind, close, query
// and prepare sequences with scripted add failures.
func TestCatchupControl_prepareReseed_rapid(t *testing.T) {
	t.Parallel()

	const (
		boundSessionID int32 = testCatchupLogSessionID
		otherSessionID int32 = testCatchupLogSessionID + 1
	)

	rapid.Check(t, func(rt *rapid.T) {
		fixture := openCatchupTestStreams(t)
		fixture.agent.SetCatchupControl(newTestCatchupControl(t, fixture, &fakeReplayControl{}))
		cause := errors.New("driver refused")
		model := prepareReseedModel{bound: fixture.streams.Subscription()}
		var correlationID int64

		ops := rapid.IntRange(1, 32).Draw(rt, "ops")
		for range ops {
			switch rapid.IntRange(0, 3).Draw(rt, "op") {
			case 0: // bind
				if model.imageOpen || model.bound == nil {
					continue
				}
				correlationID++
				model.image = joinTestLogImage(rt, fixture, boundSessionID, correlationID, 0)
				model.imageOpen = true
				model.missed = false
			case 1: // close
				if !model.imageOpen {
					continue
				}
				aeron.ImageInternalCloseForHooks(model.image)
				model.imageOpen = false
			case 2: // query
				sessionID := rapid.SampledFrom([]int32{boundSessionID, otherSessionID}).
					Draw(rt, "session")
				wantReady := sessionID == boundSessionID && model.imageOpen
				_, ready := fixture.agent.FollowerLogJoinReady(sessionID)
				require.Equal(rt, wantReady, ready)
				model.missed = !ready
			case 3: // prepare
				outcome := rapid.SampledFrom(reseedAddOutcomes).Draw(rt, "outcome")
				requirePrepareReseedStep(rt, fixture, &model, outcome, cause)
			}
			requirePrepareReseedState(rt, fixture, &model, boundSessionID)
		}
	})
}

// requirePrepareReseedStep runs one prepare and applies the expected reseed to
// model. outcome scripts the adds only when model expects a reseed.
func requirePrepareReseedStep(
	rt *rapid.T,
	fixture *catchupTestStreams,
	model *prepareReseedModel,
	outcome reseedAddOutcome,
	cause error,
) {
	rt.Helper()

	wantReseed := model.missed || !model.imageOpen
	wantAdds := 0
	if wantReseed {
		if outcome.arrange != nil {
			outcome.arrange(fixture.adder, cause)
		}
		wantAdds = outcome.wantAdds
	}
	addsBefore := len(fixture.adder.subCalls)

	_, prepared, err := fixture.agent.PrepareForNewLeadership(0, 0)
	require.NoError(rt, err)
	require.True(rt, prepared)
	require.Len(rt, fixture.adder.subCalls, addsBefore+wantAdds)
	model.missed = false
	if !wantReseed {
		require.Same(rt, model.image, fixture.adapter.Image())
		return
	}

	if model.bound != nil {
		model.superseded = append(model.superseded, model.bound)
	}
	model.image = nil
	model.imageOpen = false
	if !outcome.wantSub {
		model.bound = nil
		return
	}
	model.bound = fixture.adder.lastSub
}

// requirePrepareReseedState requires the streams, agent and adapter to match
// model.
func requirePrepareReseedState(
	rt *rapid.T,
	fixture *catchupTestStreams,
	model *prepareReseedModel,
	boundSessionID int32,
) {
	rt.Helper()

	for _, sub := range model.superseded {
		require.True(rt, sub.IsClosed())
	}
	if model.image == nil {
		require.Nil(rt, fixture.adapter.Image())
	} else {
		require.Same(rt, model.image, fixture.adapter.Image())
	}

	requireLogStreamsReseeded(rt, fixture, model.bound != nil)
	if model.bound == nil {
		return
	}
	require.Same(rt, model.bound, fixture.streams.Subscription())

	joinSource := fixture.agent.LogJoinSource()
	if model.imageOpen {
		require.Same(rt, model.image, joinSource.Image(boundSessionID))
	} else {
		require.Nil(rt, joinSource.Image(boundSessionID))
	}
}
