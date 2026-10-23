package archive

import "github.com/warrenguy/heron/aeron/cnc"

// ReplicationParams holds the optional fields of a replicate request.
//
// Build it with NewReplicationParams. The zero value is not the default: 0
// names recording 0, stop position 0, tag 0, file IO cap 0 and session id 0.
//
// Lifetime: the slices alias caller memory. Proxy.TryReplicate copies them
// into its wire buffer.
type ReplicationParams struct {
	// DestinationRecordingID names the destination recording to extend.
	// cnc.NullValue asks the destination archive for a new recording.
	DestinationRecordingID int64

	// StopPosition is the source position where the replication stops.
	// NullPosition replicates without a stop position.
	StopPosition int64

	// LiveDestination is the destination for a merge with the live stream.
	// Empty means no merge.
	LiveDestination []byte

	// ReplicationChannel is the channel for the replay from the source
	// archive. Empty uses the destination archive's configured channel.
	ReplicationChannel []byte

	// ChannelTagID is the channel tag for the destination archive's
	// replication subscription. cnc.NullValue means untagged.
	ChannelTagID int64

	// SubscriptionTagID is the subscription tag for the destination archive's
	// replication subscription. cnc.NullValue means untagged.
	SubscriptionTagID int64

	// FileIOMaxLength caps each file read of the source archive's replay. -1
	// uses the source archive's default.
	FileIOMaxLength int32

	// ReplicationSessionID is the session id of the destination recording.
	// -1 keeps the session id of the source recording. The archive ignores
	// the field when LiveDestination is set.
	ReplicationSessionID int32

	// EncodedCredentials are the credentials the destination archive sends
	// to the source archive on connect. Empty uses the destination archive's
	// configured credentials.
	EncodedCredentials []byte

	// SourceResponseChannel is the response channel of the destination
	// archive's control session with the source archive. Empty uses the
	// destination archive's configured control response channel.
	SourceResponseChannel []byte
}

// NewReplicationParams returns the defaults of Java ReplicationParams.
func NewReplicationParams() ReplicationParams {
	return ReplicationParams{
		DestinationRecordingID: cnc.NullValue,
		StopPosition:           NullPosition,
		ChannelTagID:           cnc.NullValue,
		SubscriptionTagID:      cnc.NullValue,
		FileIOMaxLength:        -1,
		ReplicationSessionID:   -1,
	}
}
