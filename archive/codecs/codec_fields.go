package codecs

import (
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

type ControlResponseFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RelevantID       int64
	Code             archivewire.ControlResponseCode
	Version          int32
	ErrorMessage     []byte
}

type CloseSessionRequestFields struct {
	ControlSessionID int64
}

type StartRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	SourceLocation   archivewire.SourceLocation
	Channel          []byte
}

type StopRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	Channel          []byte
}

type StopRecordingByIdentityRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
}

type ReplayRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
	Position         int64
	Length           int64
	ReplayStreamID   int32
	// FileIoMaxLength is math.MinInt32 when actingVersion < 7 (field absent).
	FileIoMaxLength int32
	// ReplayToken is the null value when actingVersion < 10 (field absent).
	ReplayToken   int64
	ReplayChannel []byte
}

type StopReplayRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	ReplaySessionID  int64
}

type ReplicateRequest2Fields struct {
	ControlSessionID   int64
	CorrelationID      int64
	SrcRecordingID     int64
	DstRecordingID     int64
	StopPosition       int64
	ChannelTagID       int64
	SubscriptionTagID  int64
	SrcControlStreamID int32
	// FileIoMaxLength is math.MinInt32 when actingVersion < 7 (field absent).
	FileIoMaxLength int32
	// ReplicationSessionID is math.MinInt32 when actingVersion < 8 (field absent).
	ReplicationSessionID int32
	SrcControlChannel    []byte
	LiveDestination      []byte
	ReplicationChannel   []byte
	EncodedCredentials   []byte
	SrcResponseChannel   []byte
}

type StopReplicationRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	ReplicationID    int64
}

type ListRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
}

type TruncateRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
	Position         int64
}

type RecordingDescriptorFields struct {
	ControlSessionID  int64
	CorrelationID     int64
	RecordingID       int64
	StartTimestamp    int64
	StopTimestamp     int64
	StartPosition     int64
	StopPosition      int64
	InitialTermID     int32
	SegmentFileLength int32
	TermBufferLength  int32
	MtuLength         int32
	SessionID         int32
	StreamID          int32
	StrippedChannel   []byte
	OriginalChannel   []byte
	SourceIdentity    []byte
}

// CloneRecordingDescriptorFields deep-copies var-ascii byte slices for retention
// past buffer reuse.
func CloneRecordingDescriptorFields(
	fields RecordingDescriptorFields,
) RecordingDescriptorFields {
	fields.StrippedChannel = cloneVarBytes(fields.StrippedChannel)
	fields.OriginalChannel = cloneVarBytes(fields.OriginalChannel)
	fields.SourceIdentity = cloneVarBytes(fields.SourceIdentity)
	return fields
}

type RecordingSignalEventFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
	SubscriptionID   int64
	Position         int64
	Signal           archivewire.RecordingSignal
}

type AuthConnectRequestFields struct {
	CorrelationID      int64
	ResponseStreamID   int32
	Version            int32
	ResponseChannel    []byte
	EncodedCredentials []byte
	ClientInfo         []byte
}

type ChallengeFields struct {
	ControlSessionID int64
	CorrelationID    int64
	Version          int32
	EncodedChallenge []byte
}

type ChallengeResponseFields struct {
	ControlSessionID   int64
	CorrelationID      int64
	EncodedCredentials []byte
}

type KeepAliveRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
}

type ArchiveIDRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
}

type StartRecordingRequest2Fields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	SourceLocation   archivewire.SourceLocation
	AutoStop         archivewire.BooleanType
	Channel          []byte
}

type ExtendRecordingRequest2Fields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
	StreamID         int32
	SourceLocation   archivewire.SourceLocation
	AutoStop         archivewire.BooleanType
	Channel          []byte
}
