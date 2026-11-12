//go:build integration || testhooks

package archivesbe

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode"

	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
)

type valueKind int

const (
	kindInt64 valueKind = iota
	kindInt32
	kindEnum
	kindASCII
	kindBytes
)

type fieldDef struct {
	key   string
	kind  valueKind
	since uint16
	enum  string
}

type templateDef struct {
	name    string
	id      uint16
	fixed   []fieldDef
	ascii   []fieldDef
	data    []fieldDef
	encode  func([]byte, WireFixture) (int, bool)
	project func(resolvedFields) any
}

type resolvedFields struct {
	i64   map[string]int64
	i32   map[string]int32
	ascii map[string][]byte
	data  map[string][]byte
	enums map[string]int32
}

var (
	controlResponseVersionSince = uint16(
		(&archivewire.ControlResponse{}).VersionSinceVersion(),
	)
	replayFileIoSince = uint16(
		(&archivewire.ReplayRequest{}).FileIoMaxLengthSinceVersion(),
	)
	replayTokenSince = uint16(
		(&archivewire.ReplayRequest{}).ReplayTokenSinceVersion(),
	)
	authClientInfoSince = uint16(
		(&archivewire.AuthConnectRequest{}).ClientInfoSinceVersion(),
	)
	replicateFileIoSince = uint16(
		(&archivewire.ReplicateRequest2{}).FileIoMaxLengthSinceVersion(),
	)
	replicateSessionSince = uint16(
		(&archivewire.ReplicateRequest2{}).ReplicationSessionIdSinceVersion(),
	)
	replicateCredentialsSince = uint16(
		(&archivewire.ReplicateRequest2{}).EncodedCredentialsSinceVersion(),
	)
	replicateSrcResponseSince = uint16(
		(&archivewire.ReplicateRequest2{}).SrcResponseChannelSinceVersion(),
	)
)

var enumTables = map[string]map[string]int32{
	"ControlResponseCode": enumTable(map[string]archivewire.ControlResponseCode{
		"OK":                   archivewire.ControlResponseCode_OK,
		"ERROR":                archivewire.ControlResponseCode_ERROR,
		"RECORDING_UNKNOWN":    archivewire.ControlResponseCode_RECORDING_UNKNOWN,
		"SUBSCRIPTION_UNKNOWN": archivewire.ControlResponseCode_SUBSCRIPTION_UNKNOWN,
	}),
	"SourceLocation": enumTable(map[string]archivewire.SourceLocation{
		"LOCAL":  archivewire.SourceLocation_LOCAL,
		"REMOTE": archivewire.SourceLocation_REMOTE,
	}),
	"BooleanType": enumTable(map[string]archivewire.BooleanType{
		"FALSE": archivewire.BooleanType_FALSE,
		"TRUE":  archivewire.BooleanType_TRUE,
	}),
	"RecordingSignal": enumTable(map[string]archivewire.RecordingSignal{
		"START":         archivewire.RecordingSignal_START,
		"STOP":          archivewire.RecordingSignal_STOP,
		"EXTEND":        archivewire.RecordingSignal_EXTEND,
		"REPLICATE":     archivewire.RecordingSignal_REPLICATE,
		"MERGE":         archivewire.RecordingSignal_MERGE,
		"SYNC":          archivewire.RecordingSignal_SYNC,
		"DELETE":        archivewire.RecordingSignal_DELETE,
		"REPLICATE_END": archivewire.RecordingSignal_REPLICATE_END,
	}),
}

var templateCatalog = []templateDef{
	{
		name: "controlResponse",
		id:   archivewire.ControlResponseSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("relevantId"),
			enumField("code", "ControlResponseCode"),
			i32("version").sinceVersion(controlResponseVersionSince),
		},
		ascii: []fieldDef{ascii("errorMessage")},
		project: func(fields resolvedFields) any {
			return codecs.ControlResponseFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RelevantID:       fields.i64["relevantId"],
				Code:             archivewire.ControlResponseCode(fields.enums["code"]),
				Version:          fields.i32["version"],
				ErrorMessage:     fields.ascii["errorMessage"],
			}
		},
	},
	{
		name:   "closeSessionRequest",
		id:     archivewire.CloseSessionRequestSbeTemplateID,
		fixed:  []fieldDef{i64("controlSessionId")},
		encode: encoderFor(codecs.EncodeCloseSessionRequest),
		project: func(fields resolvedFields) any {
			return codecs.CloseSessionRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
			}
		},
	},
	{
		name: "startRecordingRequest",
		id:   archivewire.StartRecordingRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i32("streamId"),
			enumField("sourceLocation", "SourceLocation"),
		},
		ascii:  []fieldDef{ascii("channel")},
		encode: encoderFor(codecs.EncodeStartRecordingRequest),
		project: func(fields resolvedFields) any {
			return codecs.StartRecordingRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				StreamID:         fields.i32["streamId"],
				SourceLocation:   archivewire.SourceLocation(fields.enums["sourceLocation"]),
				Channel:          fields.ascii["channel"],
			}
		},
	},
	{
		name: "stopRecordingRequest",
		id:   archivewire.StopRecordingRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i32("streamId"),
		},
		ascii:  []fieldDef{ascii("channel")},
		encode: encoderFor(codecs.EncodeStopRecordingRequest),
		project: func(fields resolvedFields) any {
			return codecs.StopRecordingRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				StreamID:         fields.i32["streamId"],
				Channel:          fields.ascii["channel"],
			}
		},
	},
	{
		name: "replayRequest",
		id:   archivewire.ReplayRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
			i64("position"),
			i64("length"),
			i32("replayStreamId"),
			i32("fileIoMaxLength").sinceVersion(replayFileIoSince),
			i64("replayToken").sinceVersion(replayTokenSince),
		},
		ascii:  []fieldDef{ascii("replayChannel")},
		encode: encoderFor(codecs.EncodeReplayRequest),
		project: func(fields resolvedFields) any {
			return codecs.ReplayRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
				Position:         fields.i64["position"],
				Length:           fields.i64["length"],
				ReplayStreamID:   fields.i32["replayStreamId"],
				FileIoMaxLength:  fields.i32["fileIoMaxLength"],
				ReplayToken:      fields.i64["replayToken"],
				ReplayChannel:    fields.ascii["replayChannel"],
			}
		},
	},
	{
		name: "stopReplayRequest",
		id:   archivewire.StopReplayRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("replaySessionId"),
		},
		encode: encoderFor(codecs.EncodeStopReplayRequest),
		project: func(fields resolvedFields) any {
			return codecs.StopReplayRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				ReplaySessionID:  fields.i64["replaySessionId"],
			}
		},
	},
	{
		name: "listRecordingRequest",
		id:   archivewire.ListRecordingRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
		},
		encode: encoderFor(codecs.EncodeListRecordingRequest),
		project: func(fields resolvedFields) any {
			return codecs.ListRecordingRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
			}
		},
	},
	{
		name: "truncateRecordingRequest",
		id:   archivewire.TruncateRecordingRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
			i64("position"),
		},
		encode: encoderFor(codecs.EncodeTruncateRecordingRequest),
		project: func(fields resolvedFields) any {
			return codecs.TruncateRecordingRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
				Position:         fields.i64["position"],
			}
		},
	},
	{
		name: "recordingDescriptor",
		id:   archivewire.RecordingDescriptorSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
			i64("startTimestamp"),
			i64("stopTimestamp"),
			i64("startPosition"),
			i64("stopPosition"),
			i32("initialTermId"),
			i32("segmentFileLength"),
			i32("termBufferLength"),
			i32("mtuLength"),
			i32("sessionId"),
			i32("streamId"),
		},
		ascii: []fieldDef{
			ascii("strippedChannel"),
			ascii("originalChannel"),
			ascii("sourceIdentity"),
		},
		project: func(fields resolvedFields) any {
			return codecs.RecordingDescriptorFields{
				ControlSessionID:  fields.i64["controlSessionId"],
				CorrelationID:     fields.i64["correlationId"],
				RecordingID:       fields.i64["recordingId"],
				StartTimestamp:    fields.i64["startTimestamp"],
				StopTimestamp:     fields.i64["stopTimestamp"],
				StartPosition:     fields.i64["startPosition"],
				StopPosition:      fields.i64["stopPosition"],
				InitialTermID:     fields.i32["initialTermId"],
				SegmentFileLength: fields.i32["segmentFileLength"],
				TermBufferLength:  fields.i32["termBufferLength"],
				MtuLength:         fields.i32["mtuLength"],
				SessionID:         fields.i32["sessionId"],
				StreamID:          fields.i32["streamId"],
				StrippedChannel:   fields.ascii["strippedChannel"],
				OriginalChannel:   fields.ascii["originalChannel"],
				SourceIdentity:    fields.ascii["sourceIdentity"],
			}
		},
	},
	{
		name: "recordingSignalEvent",
		id:   archivewire.RecordingSignalEventSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
			i64("subscriptionId"),
			i64("position"),
			enumField("signal", "RecordingSignal"),
		},
		project: func(fields resolvedFields) any {
			return codecs.RecordingSignalEventFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
				SubscriptionID:   fields.i64["subscriptionId"],
				Position:         fields.i64["position"],
				Signal:           archivewire.RecordingSignal(fields.enums["signal"]),
			}
		},
	},
	{
		name: "stopReplicationRequest",
		id:   archivewire.StopReplicationRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("replicationId"),
		},
		encode: encoderFor(codecs.EncodeStopReplicationRequest),
		project: func(fields resolvedFields) any {
			return codecs.StopReplicationRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				ReplicationID:    fields.i64["replicationId"],
			}
		},
	},
	{
		name: "authConnectRequest",
		id:   archivewire.AuthConnectRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("correlationId"),
			i32("responseStreamId"),
			i32("version"),
		},
		ascii: []fieldDef{
			ascii("responseChannel"),
			ascii("clientInfo").sinceVersion(authClientInfoSince),
		},
		data:   []fieldDef{data("encodedCredentials")},
		encode: encoderFor(codecs.EncodeAuthConnectRequest),
		project: func(fields resolvedFields) any {
			return codecs.AuthConnectRequestFields{
				CorrelationID:      fields.i64["correlationId"],
				ResponseStreamID:   fields.i32["responseStreamId"],
				Version:            fields.i32["version"],
				ResponseChannel:    fields.ascii["responseChannel"],
				EncodedCredentials: fields.data["encodedCredentials"],
				ClientInfo:         fields.ascii["clientInfo"],
			}
		},
	},
	{
		name: "challenge",
		id:   archivewire.ChallengeSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i32("version"),
		},
		data: []fieldDef{data("encodedChallenge")},
		project: func(fields resolvedFields) any {
			return codecs.ChallengeFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				Version:          fields.i32["version"],
				EncodedChallenge: fields.data["encodedChallenge"],
			}
		},
	},
	{
		name: "challengeResponse",
		id:   archivewire.ChallengeResponseSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
		},
		data:   []fieldDef{data("encodedCredentials")},
		encode: encoderFor(codecs.EncodeChallengeResponse),
		project: func(fields resolvedFields) any {
			return codecs.ChallengeResponseFields{
				ControlSessionID:   fields.i64["controlSessionId"],
				CorrelationID:      fields.i64["correlationId"],
				EncodedCredentials: fields.data["encodedCredentials"],
			}
		},
	},
	{
		name: "keepAliveRequest",
		id:   archivewire.KeepAliveRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
		},
		encode: encoderFor(codecs.EncodeKeepAliveRequest),
		project: func(fields resolvedFields) any {
			return codecs.KeepAliveRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
			}
		},
	},
	{
		name: "startRecordingRequest2",
		id:   archivewire.StartRecordingRequest2SbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i32("streamId"),
			enumField("sourceLocation", "SourceLocation"),
			enumField("autoStop", "BooleanType"),
		},
		ascii:  []fieldDef{ascii("channel")},
		encode: encoderFor(codecs.EncodeStartRecordingRequest2),
		project: func(fields resolvedFields) any {
			return codecs.StartRecordingRequest2Fields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				StreamID:         fields.i32["streamId"],
				SourceLocation:   archivewire.SourceLocation(fields.enums["sourceLocation"]),
				AutoStop:         archivewire.BooleanType(fields.enums["autoStop"]),
				Channel:          fields.ascii["channel"],
			}
		},
	},
	{
		name: "extendRecordingRequest2",
		id:   archivewire.ExtendRecordingRequest2SbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
			i32("streamId"),
			enumField("sourceLocation", "SourceLocation"),
			enumField("autoStop", "BooleanType"),
		},
		ascii:  []fieldDef{ascii("channel")},
		encode: encoderFor(codecs.EncodeExtendRecordingRequest2),
		project: func(fields resolvedFields) any {
			return codecs.ExtendRecordingRequest2Fields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
				StreamID:         fields.i32["streamId"],
				SourceLocation:   archivewire.SourceLocation(fields.enums["sourceLocation"]),
				AutoStop:         archivewire.BooleanType(fields.enums["autoStop"]),
				Channel:          fields.ascii["channel"],
			}
		},
	},
	{
		name: "stopRecordingByIdentityRequest",
		id:   archivewire.StopRecordingByIdentityRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("recordingId"),
		},
		encode: encoderFor(codecs.EncodeStopRecordingByIdentityRequest),
		project: func(fields resolvedFields) any {
			return codecs.StopRecordingByIdentityRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
				RecordingID:      fields.i64["recordingId"],
			}
		},
	},
	{
		name: "replicateRequest2",
		id:   archivewire.ReplicateRequest2SbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
			i64("srcRecordingId"),
			i64("dstRecordingId"),
			i64("stopPosition"),
			i64("channelTagId"),
			i64("subscriptionTagId"),
			i32("srcControlStreamId"),
			i32("fileIoMaxLength").sinceVersion(replicateFileIoSince),
			i32("replicationSessionId").sinceVersion(replicateSessionSince),
		},
		ascii: []fieldDef{
			ascii("srcControlChannel"),
			ascii("liveDestination"),
			ascii("replicationChannel"),
			ascii("srcResponseChannel").sinceVersion(replicateSrcResponseSince),
		},
		data:   []fieldDef{data("encodedCredentials").sinceVersion(replicateCredentialsSince)},
		encode: encoderFor(codecs.EncodeReplicateRequest2),
		project: func(fields resolvedFields) any {
			return codecs.ReplicateRequest2Fields{
				ControlSessionID:     fields.i64["controlSessionId"],
				CorrelationID:        fields.i64["correlationId"],
				SrcRecordingID:       fields.i64["srcRecordingId"],
				DstRecordingID:       fields.i64["dstRecordingId"],
				StopPosition:         fields.i64["stopPosition"],
				ChannelTagID:         fields.i64["channelTagId"],
				SubscriptionTagID:    fields.i64["subscriptionTagId"],
				SrcControlStreamID:   fields.i32["srcControlStreamId"],
				FileIoMaxLength:      fields.i32["fileIoMaxLength"],
				ReplicationSessionID: fields.i32["replicationSessionId"],
				SrcControlChannel:    fields.ascii["srcControlChannel"],
				LiveDestination:      fields.ascii["liveDestination"],
				ReplicationChannel:   fields.ascii["replicationChannel"],
				EncodedCredentials:   fields.data["encodedCredentials"],
				SrcResponseChannel:   fields.ascii["srcResponseChannel"],
			}
		},
	},
	{
		name: "archiveIdRequest",
		id:   archivewire.ArchiveIdRequestSbeTemplateID,
		fixed: []fieldDef{
			i64("controlSessionId"),
			i64("correlationId"),
		},
		encode: encoderFor(codecs.EncodeArchiveIDRequest),
		project: func(fields resolvedFields) any {
			return codecs.ArchiveIDRequestFields{
				ControlSessionID: fields.i64["controlSessionId"],
				CorrelationID:    fields.i64["correlationId"],
			}
		},
	},
}

func templateByName(name string) (templateDef, bool) {
	for _, tmpl := range templateCatalog {
		if tmpl.name == name {
			return tmpl, true
		}
	}

	return templateDef{}, false
}

func templateByID(id uint16) (templateDef, bool) {
	for _, tmpl := range templateCatalog {
		if tmpl.id == id {
			return tmpl, true
		}
	}

	return templateDef{}, false
}

func encoderFor[T any](
	encode func([]byte, int, uint16, T) (int, bool),
) func([]byte, WireFixture) (int, bool) {
	return func(buf []byte, fixture WireFixture) (int, bool) {
		fields, ok := fixture.Fields.(T)
		if !ok {
			return 0, false
		}

		return encode(buf, 0, fixture.ActingVersion, fields)
	}
}

func enumTable[T ~int32](values map[string]T) map[string]int32 {
	table := make(map[string]int32, len(values))

	for name, value := range values {
		table[name] = int32(value)
	}

	return table
}

func resolveFields(
	tmpl templateDef,
	acting uint16,
	fields map[string]any,
) (any, error) {
	byKey := make(map[string]fieldDef, len(tmpl.fixed)+len(tmpl.ascii)+len(tmpl.data))

	for _, group := range [][]fieldDef{tmpl.fixed, tmpl.ascii, tmpl.data} {
		for _, field := range group {
			byKey[field.key] = field
		}
	}

	resolved := resolvedFields{
		i64:   map[string]int64{},
		i32:   map[string]int32{},
		ascii: map[string][]byte{},
		data:  map[string][]byte{},
		enums: map[string]int32{},
	}

	for key, value := range fields {
		field, known := byKey[key]
		if !known {
			return nil, fmt.Errorf("unknown field %q", key)
		}

		if field.since > acting {
			return nil, fmt.Errorf("gated field %q", key)
		}

		if err := putResolved(&resolved, field, value); err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
	}

	for _, field := range tmpl.fixed {
		if field.since > acting {
			continue
		}

		if _, present := fields[field.key]; !present {
			return nil, fmt.Errorf("missing field %q", field.key)
		}
	}

	return tmpl.project(resolved), nil
}

func putResolved(dst *resolvedFields, field fieldDef, value any) error {
	switch field.kind {
	case kindInt64:
		return put(dst.i64, field.key, value, jsonInt64)
	case kindInt32:
		return put(dst.i32, field.key, value, jsonInt32)
	case kindEnum:
		return put(dst.enums, field.key, value, func(value any) (int32, error) {
			return parseEnum(field.enum, value)
		})
	case kindASCII:
		return put(dst.ascii, field.key, value, parseASCII)
	case kindBytes:
		return put(dst.data, field.key, value, parseData)
	default:
		return fmt.Errorf("unknown field kind %d", field.kind)
	}
}

func put[T any](
	dst map[string]T,
	key string,
	value any,
	parse func(any) (T, error),
) error {
	parsed, err := parse(value)
	if err != nil {
		return err
	}

	dst[key] = parsed

	return nil
}

func jsonInt64(value any) (int64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("expected integral number")
	}

	parsed, err := number.Int64()
	if err == nil {
		return parsed, nil
	}

	if errors.Is(err, strconv.ErrRange) {
		return 0, errors.New("out of int64 range")
	}

	return 0, errors.New("expected integral number")
}

func jsonInt32(value any) (int32, error) {
	parsed, err := jsonInt64(value)
	if err != nil {
		return 0, err
	}

	if parsed < math.MinInt32 || parsed > math.MaxInt32 {
		return 0, errors.New("out of int32 range")
	}

	return int32(parsed), nil
}

func parseEnum(enumName string, value any) (int32, error) {
	name, ok := value.(string)
	if !ok {
		return 0, errors.New("expected string")
	}

	table, ok := enumTables[enumName]
	if !ok {
		return 0, fmt.Errorf("unknown enum %s", enumName)
	}

	parsed, ok := table[name]
	if !ok {
		return 0, fmt.Errorf("unknown %s %q", enumName, name)
	}

	return parsed, nil
}

func parseASCII(value any) ([]byte, error) {
	text, ok := value.(string)
	if !ok {
		return nil, errors.New("expected string")
	}

	if text == "" {
		return nil, errors.New("empty string")
	}

	for _, char := range text {
		if char > unicode.MaxASCII {
			return nil, errors.New("non-ASCII character")
		}
	}

	return []byte(text), nil
}

func parseData(value any) ([]byte, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("expected hex object")
	}

	text, ok := object["hex"].(string)
	if !ok || len(object) != 1 {
		return nil, errors.New(`expected {"hex": string} object`)
	}

	if text == "" {
		return nil, errors.New("empty hex")
	}

	return decodeLowerHex(text)
}

func i64(key string) fieldDef {
	return fieldDef{key: key, kind: kindInt64}
}

func i32(key string) fieldDef {
	return fieldDef{key: key, kind: kindInt32}
}

func enumField(key, enumName string) fieldDef {
	return fieldDef{key: key, kind: kindEnum, enum: enumName}
}

func ascii(key string) fieldDef {
	return fieldDef{key: key, kind: kindASCII}
}

func data(key string) fieldDef {
	return fieldDef{key: key, kind: kindBytes}
}

func (f fieldDef) sinceVersion(since uint16) fieldDef {
	f.since = since

	return f
}
