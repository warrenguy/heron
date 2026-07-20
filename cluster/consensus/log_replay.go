package consensus

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// LogReplay is a duty-cycle helper for leader/follower replay paths.
// Archive-backed implementation lands with agent archive wiring.
// Produced by [Agent.NewLogReplay].
type LogReplay interface {
	DoWork(nowNs int64) int
	Position() int64
	IsDone() bool
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
