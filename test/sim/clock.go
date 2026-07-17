package sim

const (
	simStartTimeNs = int64(1_000_000_000)
	defaultEpochMs = int64(42_000)
)

// NodeClock supplies ElectionDeps nano and epoch time for one Election instance.
type NodeClock struct {
	nowNs         int64
	epochOffsetMs int64
}

// NewNodeClock initializes nano and epoch time at simulated start.
func NewNodeClock(nowNs, epochMs int64) *NodeClock {
	return &NodeClock{
		nowNs:         nowNs,
		epochOffsetMs: epochMs - nowNs/1_000_000,
	}
}

// TimeNs is the ElectionDeps nano clock.
func (c *NodeClock) TimeNs() int64 { return c.nowNs }

// TimeMs is the ElectionDeps epoch clock.
func (c *NodeClock) TimeMs() int64 { return c.epochOffsetMs + c.nowNs/1_000_000 }

// Set advances the clock to scheduler dispatch time.
func (c *NodeClock) Set(nowNs int64) { c.nowNs = nowNs }

// AdvanceEpoch shifts the epoch clock by dMs without changing nano time.
func (c *NodeClock) AdvanceEpoch(dMs int64) { c.epochOffsetMs += dMs }
