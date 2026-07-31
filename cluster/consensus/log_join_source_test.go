package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// fakeSubscriptionImages is a driver-free [SubscriptionImages] whose ImageAt
// returns ok true for every in-range index, including a nil entry. That is the
// only way to reach the walk's image != nil guard: *aeron.Subscription filters
// nil slots to ok false.
type fakeSubscriptionImages struct {
	channel  string
	streamID int32
	images   []*aeron.Image
}

var _ SubscriptionImages = (*fakeSubscriptionImages)(nil)

func (f *fakeSubscriptionImages) Channel() string { return f.channel }

func (f *fakeSubscriptionImages) StreamID() int32 { return f.streamID }

func (f *fakeSubscriptionImages) ImageCount() int { return len(f.images) }

func (f *fakeSubscriptionImages) ImageAt(index int) (*aeron.Image, bool) {
	if index < 0 || index >= len(f.images) {
		return nil, false
	}
	return f.images[index], true
}

func TestSubscriptionLogJoinSource_notReady(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		source func() *SubscriptionLogJoinSource
	}{
		{"nil receiver", func() *SubscriptionLogJoinSource {
			return nil
		}},
		{"nil images", func() *SubscriptionLogJoinSource {
			return NewSubscriptionLogJoinSource(nil)
		}},
		{"zero-value subscription", func() *SubscriptionLogJoinSource {
			return NewSubscriptionLogJoinSource(new(aeron.Subscription))
		}},
		// ok true with a nil entry reaches the walk's image != nil guard.
		{"fake nil entry", func() *SubscriptionLogJoinSource {
			return NewSubscriptionLogJoinSource(&fakeSubscriptionImages{
				channel:  "aeron:ipc",
				streamID: 100,
				images:   []*aeron.Image{nil},
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := tc.source()
			params, ok := s.Ready(7)
			require.False(t, ok)
			require.Equal(t, FollowerLogJoinParams{}, params)
			require.Nil(t, s.Image(7))
		})
	}
}

// stubLogJoinSource is a driver-free [LogJoinSource] for Agent join rows.
// Image returns image and records the call so adapter-guard order is testable.
type stubLogJoinSource struct {
	readyParams FollowerLogJoinParams
	readyOK     bool
	image       *aeron.Image
	imageCalls  int
}

var _ LogJoinSource = (*stubLogJoinSource)(nil)

func (s *stubLogJoinSource) Ready(logSessionID int32) (FollowerLogJoinParams, bool) {
	return s.readyParams, s.readyOK
}

func (s *stubLogJoinSource) Image(logSessionID int32) *aeron.Image {
	s.imageCalls++
	return s.image
}

func TestSetLogJoinSource_wireAndClear(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{})
	require.Nil(t, a.LogJoinSource())

	src := &stubLogJoinSource{}
	a.SetLogJoinSource(src)
	require.Equal(t, LogJoinSource(src), a.LogJoinSource())

	a.SetLogJoinSource(nil)
	require.Nil(t, a.LogJoinSource())
}

func TestAgentConfig_LogJoinSource(t *testing.T) {
	t.Parallel()

	src := &stubLogJoinSource{}
	a := NewAgent(AgentConfig{LogJoinSource: src})
	require.Equal(t, LogJoinSource(src), a.LogJoinSource())
}

func TestAgentFollowerLogJoin_sourceWiredNotReady(t *testing.T) {
	t.Parallel()

	src := &stubLogJoinSource{readyOK: false}
	a := NewAgent(AgentConfig{LogJoinSource: src})

	params, ready := a.FollowerLogJoinReady(7)
	require.False(t, ready)
	require.Equal(t, FollowerLogJoinParams{}, params)
	// Ready path calls source.Ready only; Image is not called.
	require.Equal(t, 0, src.imageCalls)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 7}, 1)
	require.NoError(t, err)
	require.False(t, joined)
}

func TestAgentJoinLogAsFollower_noAdapter(t *testing.T) {
	t.Parallel()

	// No adapter: join false; adapter guard runs before Image.
	src := &stubLogJoinSource{}
	a := NewAgent(AgentConfig{LogJoinSource: src})
	require.Nil(t, a.logAdapter)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 7}, 1)
	require.NoError(t, err)
	require.False(t, joined)
	require.Equal(t, 0, src.imageCalls)
}

func TestAgentJoinLogAsFollower_adapterImageNil(t *testing.T) {
	t.Parallel()

	// Source Image returns nil: join false; adapter stays unbound.
	src := &stubLogJoinSource{}
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 7}, 1)
	require.NoError(t, err)
	require.False(t, joined)
	require.Equal(t, 1, src.imageCalls)
	require.Nil(t, adapter.Image())
	require.True(t, adapter.IsImageClosed())
}
