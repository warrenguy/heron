package codecs

import "github.com/warrenguy/heron/archive/codecs/gensbe"

type ControlResponseFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RelevantID       int64
	Code             gensbe.ControlResponseCode
	Version          int32
	ErrorMessage     []byte
}

type ConnectRequestFields struct {
	CorrelationID    int64
	ResponseStreamID int32
	Version          int32
	ResponseChannel  []byte
}

type CloseSessionRequestFields struct {
	ControlSessionID int64
}

type StartRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	SourceLocation   gensbe.SourceLocation
	Channel          []byte
}

type StopRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	Channel          []byte
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

type ListRecordingRequestFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
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

type RecordingSignalEventFields struct {
	ControlSessionID int64
	CorrelationID    int64
	RecordingID      int64
	SubscriptionID   int64
	Position         int64
	Signal           gensbe.RecordingSignal
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

type StartRecordingRequest2Fields struct {
	ControlSessionID int64
	CorrelationID    int64
	StreamID         int32
	SourceLocation   gensbe.SourceLocation
	AutoStop         gensbe.BooleanType
	Channel          []byte
}
