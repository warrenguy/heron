package consensus

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// LogReplay is a duty-cycle helper for leader and follower replay paths.
//
// Behaviour:
//   - DoWork drives one step
//   - Position reports how far the replay has run
//   - IsDone is sticky: once true it stays true
//   - when IsDone is true, Position stops moving
//   - IsDone true below the replay bound reports early end, not the bound
//   - at the bound, IsDone stays false while a snapshot wait pends
//
// Concurrency: invoker-only.
//
// Ownership: the caller owns the helper and must call Close. Close releases the
// replay session and any transport handle the helper opened. Close is
// idempotent. DoWork after Close returns 0 and changes nothing.
//
// Produced by [Agent.NewLogReplay].
type LogReplay interface {
	DoWork(nowNs int64) int
	Position() int64
	IsDone() bool
	Close()
}

// LogReplication is a duty-cycle helper for follower log replication.
// Archive-backed implementation lands with agent archive wiring.
// Produced by [Agent.NewLogReplication].
type LogReplication interface {
	DoWork(nowNs int64) int
	RecordingID() int64
	IsDone() bool
	OnSignal(
		correlationID, recordingID, position int64,
		signal archivewire.RecordingSignal,
	)
}
