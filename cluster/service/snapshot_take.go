package service

import "github.com/warrenguy/heron/archive"

// snapshotArchiveControl is the archive client surface the agent holds.
// DoWork calls Poll once per tick.
//
// *archive.Client implements it. Tests substitute a double.
type snapshotArchiveControl interface {
	Poll() int
	ControlSessionID() int64
	Config() archive.Config
	SetRecordingSignalHandler(handler archive.RecordingSignalHandler)
}

var _ snapshotArchiveControl = (*archive.Client)(nil)
