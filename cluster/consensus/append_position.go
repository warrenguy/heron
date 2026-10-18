package consensus

import (
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/cnc"
)

// AppendPositionSource supplies the local log append high-water.
//
// Behaviour:
//   - ready + position: source has a high-water it can report
//   - not-ready + 0: no high-water this source can report
//
// Zero is a valid log position. A source must not report zero to mean unknown.
// A durable-state source reports not-ready when that state is absent.
//
// Concurrency: invoker-only. Read from [Agent.DoWork] (same as commit).
//
// Returns:
//   - position, true: local append high-water
//   - 0, false: no reportable high-water; [Agent] skips leader rank and follower
//     publish for that tick
type AppendPositionSource interface {
	AppendPosition() (position int64, ready bool)
}

// AtomicAppendPosition is mode-1 inject for unit/sim.
type AtomicAppendPosition struct {
	V *atomic.Int64
}

// AppendPosition returns the inject value, or 0 when the receiver or counter is
// nil. Always reports ready.
func (s *AtomicAppendPosition) AppendPosition() (int64, bool) {
	if s == nil || s.V == nil {
		return 0, true
	}
	return s.V.Load(), true
}

// SetAppendSource wires the append high-water source for leader rank and
// follower AppendPosition publish.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Nil is legal; leader [Agent.consensusWork] returns 0 until rewired.
func (a *Agent) SetAppendSource(src AppendPositionSource) { a.appendSource = src }

// AppendSource returns the wired [AppendPositionSource], or nil if unset.
func (a *Agent) AppendSource() AppendPositionSource { return a.appendSource }

// LogAppendPosition reports the local cluster log append high-water for the
// role the agent currently holds. It uses the agent's wired log publisher and
// log adapter.
//
// Behaviour:
//   - leader: exclusive log publication position (this member wrote the log)
//   - follower or candidate: joined log image received position
//   - follower's own log publication stays at 0 for its life
//   - leader binds no log image
//
// The follower value is what the driver has received. It is not what this
// member has durably recorded.
//
// Limits of image receive as high-water:
//   - does not survive a process or driver restart
//   - cannot be replayed to a peer
//
// Ranking on image receive is sound only while no member records the log. A
// member that records must rank through [RecordingPosAppendPosition] instead.
//
// Bind:
//   - [cluster.OpenLogStreams] uses this source when deps.AppendSource is nil
//   - [cluster.OpenConsensusModule] supplies [RecordingPosAppendPosition] in
//     those deps instead
//   - direct log-stream binds and unit paths with no archive use this type
//
// The image received position keeps advancing while this member's consumption
// stalls. The driver's receive path is independent of the duty cycle. Upstream
// recording counters advance during a stall for the same reason. A consumer
// that must not be outrun by its own member's reported position needs its own
// back pressure under either source.
//
// Concurrency: invoker-only. Read from [Agent.DoWork] on the goroutine that
// owns the transport duty cycle.
//
// Lifetime: safe to construct before the log image exists, before the log
// publisher and adapter are wired, and before a role is known. A nil receiver,
// a nil agent, and a nil publisher or adapter report 0.
type LogAppendPosition struct {
	agent *Agent
}

// NewLogAppendPosition returns a role-aware append high-water source over the
// agent's wired log publisher and log adapter.
func NewLogAppendPosition(agent *Agent) *LogAppendPosition {
	return &LogAppendPosition{agent: agent}
}

// AppendPosition returns the leader log publication position, the non-leader
// (follower or candidate) image received high-water, or 0 when the publisher or
// image is not wired. Always reports ready.
func (s *LogAppendPosition) AppendPosition() (int64, bool) {
	if s == nil || s.agent == nil {
		return 0, true
	}
	if s.agent.Role() == RoleLeader {
		return s.agent.logPublisher.Position(), true
	}
	// Follower and candidate: image received high-water.
	adapter := s.agent.logAdapter
	if adapter == nil {
		return 0, true
	}
	image := adapter.Image()
	if image == nil {
		return 0, true
	}
	return image.ReceivedPosition(), true
}

var _ AppendPositionSource = (*LogAppendPosition)(nil)

// RecordingPosAppendPosition reports the local cluster log append high-water from
// the archive recording-position counter for the recording this member is running
// now.
//
// Behaviour:
//   - ready + type-100 value: [Agent.IsRecordingActive] true, non-null
//     [Agent.LocalLogRecordingID], and counter present
//   - not-ready + 0: otherwise
//
// Never reports a position from a recording that has stopped.
//
// Steady-state only: drives leader quorum rank and follower AppendPosition
// publish. Does not drive the election append position. The election seeds that
// value at construction and rewrites it from
// [AgentHandler.PrepareForNewLeadership]. NewLeadershipTerm fields and the
// election divergence test read the election value, not this source.
//
// Non-null id with [Agent.IsRecordingActive] false is expected, not a fault:
//   - last id kept across a leadership transition
//   - recovery plan id before any recording starts
//
// This source reads no counter in either state. A counter the archive has not
// yet freed cannot be reported as live.
//
// Cache: holds the last counter slot between calls. Revalidates on every read.
// The slot must still be:
//   - allocated
//   - type recording-position (type-100)
//   - keyed by the active recording id
//
// A freed or reused slot fails revalidation and forces a fresh scan. A stale
// slot is never read.
//
// Concurrency: invoker-only. Read from [Agent.DoWork] on the goroutine that owns
// the transport duty cycle.
//
// Lifetime: holds the counters reader. Does not own the reader. The reader stays
// valid only while the transport client is open: the reader is a view over the
// client's mapped counters file.
//
// Client close order:
//   - module-owned client: [cluster.ConsensusModule.Close] clears the append
//     source before it closes the client
//   - caller-supplied Config.Aeron client: that caller owns close order and must
//     stop the duty cycle before closing the client
//
// Ownership:
//   - does not own the [Agent]
//   - does not own the counters reader
//   - closes neither
type RecordingPosAppendPosition struct {
	agent     *Agent
	counters  *cnc.CountersReader
	archiveID int64

	// cachedCounterID is the last counter id found for cachedRecordingID, or
	// NullCounterID when no slot is held. cachedSlot is the bound value slot for
	// that id. Both are revalidated before every cached read.
	cachedCounterID   int32
	cachedRecordingID int64
	cachedSlot        cnc.CounterSlot
}

// NewRecordingPosAppendPosition returns a recording-derived append high-water
// source over the agent's recording state and the transport client's counters.
//
// archiveID selects the recording-position counter of the archive that
// records the log. cnc.NullValue matches any archive.
//
// A nil agent or a nil counters reader is allowed. That source reports not-ready
// forever.
func NewRecordingPosAppendPosition(
	agent *Agent,
	counters *cnc.CountersReader,
	archiveID int64,
) *RecordingPosAppendPosition {
	return &RecordingPosAppendPosition{
		agent:           agent,
		counters:        counters,
		archiveID:       archiveID,
		cachedCounterID: cnc.NullCounterID,
	}
}

// AppendPosition returns the type-100 recording-position counter value for the
// active recording.
//
// Behaviour:
//   - cache hit: slot still allocated and keyed by the active recording id;
//     return that slot's value
//   - cache miss: full counter scan; recache on success
//
// A steady-state tick pays a fixed revalidation, not a full scan, when the
// cache hits.
//
// Returns:
//   - counter value, true: recording active and counter present
//   - 0, false: nil receiver, nil agent, nil counters reader, inactive
//     recording, null recording id, or no counter for that recording id and
//     archive id
func (s *RecordingPosAppendPosition) AppendPosition() (int64, bool) {
	if s == nil || s.agent == nil || s.counters == nil {
		return 0, false
	}
	if !s.agent.IsRecordingActive() {
		return 0, false
	}
	recordingID := s.agent.LocalLogRecordingID()
	if recordingID == nullValue {
		return 0, false
	}

	cachedSlotLive := s.cachedCounterID != cnc.NullCounterID &&
		s.cachedRecordingID == recordingID &&
		s.counters.IsCounterActive(
			s.cachedCounterID,
			recordingID,
			cnc.ArchiveRecordingPositionCounterTypeID,
		)
	if cachedSlotLive {
		return s.cachedSlot.Get(), true
	}

	slot, ok := s.counters.FindRecordingPositionCounter(recordingID, s.archiveID)
	if !ok {
		s.cachedCounterID = cnc.NullCounterID
		return 0, false
	}
	s.cachedCounterID = slot.CounterID()
	s.cachedRecordingID = recordingID
	s.cachedSlot = slot
	return slot.Get(), true
}

var _ AppendPositionSource = (*RecordingPosAppendPosition)(nil)
