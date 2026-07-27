//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// append_position_test uses testhooks fixtures (boundLogPublisher,
// NewImageWithCommittedDataFrameForHooks), same as log_publisher_test and
// log_adapter_test.

func advancedLogPublisher(t *testing.T) (*LogPublisher, int64) {
	t.Helper()
	pub, _ := boundLogPublisher(t)
	pos := pub.AppendMessage(1, 2, 3, []byte("hi"))
	require.Positive(t, pos)
	require.Equal(t, pos, pub.Position())
	return pub, pos
}

func adapterWithReceivedPosition(t *testing.T) (*LogAdapter, int64) {
	t.Helper()
	image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
	want := image.ReceivedPosition()
	require.Positive(t, want)
	adapter := NewLogAdapter(0)
	adapter.SetImage(image)
	return adapter, want
}

// appendAgent wires the log publisher and adapter a LogAppendPosition reads.
func appendAgent(role Role, pub *LogPublisher, adapter *LogAdapter) *Agent {
	agent := NewAgent(AgentConfig{Role: role})
	agent.SetLogPublisher(pub)
	agent.SetLogAdapter(adapter)
	return agent
}

func TestLogAppendPosition_AppendPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  func(t *testing.T) (*LogAppendPosition, int64)
	}{
		{
			name: "nil receiver",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				return nil, 0
			},
		},
		{
			name: "nil agent",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				return NewLogAppendPosition(nil), 0
			},
		},
		{
			name: "leader with bound publication",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pos := advancedLogPublisher(t)
				agent := appendAgent(RoleLeader, pub, NewLogAdapter(0))
				return NewLogAppendPosition(agent), pos
			},
		},
		{
			name: "leader unbound publication",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleLeader, NewLogPublisher(), NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "leader nil publisher",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleLeader, nil, NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower nil adapter",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleFollower, NewLogPublisher(), nil)
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower adapter no image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleFollower, NewLogPublisher(), NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				adapter, want := adapterWithReceivedPosition(t)
				agent := appendAgent(RoleFollower, NewLogPublisher(), adapter)
				return NewLogAppendPosition(agent), want
			},
		},
		{
			name: "candidate with bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				adapter, want := adapterWithReceivedPosition(t)
				agent := appendAgent(RoleCandidate, NewLogPublisher(), adapter)
				return NewLogAppendPosition(agent), want
			},
		},
		{
			// Dual-bound: leader must ignore image received high-water.
			name: "leader ignores bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pubPos := advancedLogPublisher(t)
				adapter, imagePos := adapterWithReceivedPosition(t)
				require.NotEqual(t, pubPos, imagePos)
				agent := appendAgent(RoleLeader, pub, adapter)
				return NewLogAppendPosition(agent), pubPos
			},
		},
		{
			// Dual-bound: follower must ignore publisher position.
			name: "follower ignores publisher position",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pubPos := advancedLogPublisher(t)
				adapter, imagePos := adapterWithReceivedPosition(t)
				require.NotEqual(t, pubPos, imagePos)
				agent := appendAgent(RoleFollower, pub, adapter)
				return NewLogAppendPosition(agent), imagePos
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src, want := tt.src(t)
			require.Equal(t, want, src.AppendPosition())
		})
	}
}

func TestLogAppendPosition_roleSwitch(t *testing.T) {
	t.Parallel()

	pub, pubPos := advancedLogPublisher(t)
	adapter, imagePos := adapterWithReceivedPosition(t)
	require.NotEqual(t, pubPos, imagePos)

	agent := appendAgent(RoleFollower, pub, adapter)
	src := NewLogAppendPosition(agent)

	require.Equal(t, imagePos, src.AppendPosition())

	agent.RoleSet(RoleLeader)
	require.Equal(t, pubPos, src.AppendPosition())

	agent.RoleSet(RoleCandidate)
	require.Equal(t, imagePos, src.AppendPosition())
}

// Follower open wires the adapter before any image; join binds the image later.
func TestLogAppendPosition_followerSeesImageBind(t *testing.T) {
	t.Parallel()

	adapter := NewLogAdapter(0)
	agent := appendAgent(RoleFollower, NewLogPublisher(), adapter)
	src := NewLogAppendPosition(agent)
	require.Equal(t, int64(0), src.AppendPosition())

	image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
	adapter.SetImage(image)
	require.Equal(t, image.ReceivedPosition(), src.AppendPosition())
}
