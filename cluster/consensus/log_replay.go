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
//   - at the bound, IsDone stays false while a snapshot wait pends, or while
//     the module snapshot take of the popped wait is unresolved
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

// LogReplication copies the leader's log recording into this member's log
// recording.
//
// Behaviour:
//   - DoWork drives one step: the request offer, the response poll, the
//     progress check, or the stop after Close. A failure closes the helper
//     before DoWork returns it. Later DoWork calls drive the stop. DoWork
//     returns each failure once
//   - RecordingID returns the destination recording id, or -1 before EXTEND
//   - Position returns the replicated position, or -1 before EXTEND
//   - IsDone reports the end of the replication: REPLICATE_END and STOP.
//     Once true, IsDone stays true
//   - OnSignal folds one archive recording signal. A closed helper drops it
//   - Close starts the stop. Close never blocks. Close is idempotent
//   - IsClosed reports that the helper has no stop left to run
//
// Concurrency: invoker-only.
//
// Ownership: the election drives the helper from the build until IsClosed.
// Module close stops the last helper the factory built when its replication
// may still run. The helper owns its archive request handles. It does not own
// the archive client.
//
// Produced by [Agent.NewLogReplication].
type LogReplication interface {
	DoWork(nowNs int64) (int, error)
	RecordingID() int64
	Position() int64
	IsDone() bool
	OnSignal(
		correlationID, recordingID, position int64,
		signal archivewire.RecordingSignal,
	)
	Close()
	IsClosed() bool
}
