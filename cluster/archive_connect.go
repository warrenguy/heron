package cluster

import (
	"luas.fast/aeron"
	"luas.fast/archive"
)

// moduleArchiveResponseStreamID matches the Java consensus module archive
// response stream at cluster id 0.
const moduleArchiveResponseStreamID = 100 + archive.DefaultControlResponseStreamID

// archiveConnectConfig builds archive.Config for archive.Connect from cluster Config.
//
// Behaviour: copies ArchiveControlRequestChannel and ArchiveControlResponseChannel
// onto the archive control URIs unchanged. Always applies WithRemoteControl,
// so archive.Config.Normalize must not install LocalControlChannel. An IPC control
// path is an IPC URI in ArchiveControlRequestChannel.
//
// Ownership: the archive client does not own the transport client. The returned
// config sets Aeron to the shared client argument. OwnsAeronClient is false.
// archive.Client.Close must leave the transport client open.
func archiveConnectConfig(cfg Config, client *aeron.Client) archive.Config {
	return archive.Config{
		ControlRequestChannel:   cfg.ArchiveControlRequestChannel,
		ControlResponseChannel:  cfg.ArchiveControlResponseChannel,
		ControlResponseStreamID: moduleArchiveResponseStreamID,
		Aeron:                   client,
		AeronConfig:             cfg.AeronConfig,
	}.WithRemoteControl()
}
