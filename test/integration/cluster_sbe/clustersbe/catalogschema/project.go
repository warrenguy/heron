package catalogschema

import (
	"fmt"
	"math"

	"github.com/warrenguy/heron/cluster/codecs"
)

func ProjectCatalogToFields(
	template string,
	actingVersion uint16,
	fields map[string]any,
	blobs map[string][]byte,
) (any, error) {
	switch template {
	case "sessionMessageHeader":
		return decodeSessionMessageHeaderFields(fields)
	case "sessionEvent":
		return decodeSessionEventFields(fields)
	case "sessionConnectRequest":
		return decodeSessionConnectRequestFields(fields, actingVersion, blobs)
	case "sessionCloseRequest":
		return decodeSessionCloseRequestFields(fields)
	case "sessionKeepAlive":
		return decodeSessionKeepAliveFields(fields)
	case "newLeaderEvent":
		return decodeNewLeaderEventFields(fields, blobs)
	case "challenge":
		return decodeChallengeFields(fields, blobs)
	case "challengeResponse":
		return decodeChallengeResponseFields(fields, blobs)
	case "adminRequest":
		return decodeAdminRequestFields(fields, blobs)
	case "adminResponse":
		return decodeAdminResponseFields(fields, blobs)
	case "snapshotMarker":
		return decodeSnapshotMarkerFields(fields)
	case "clientSession":
		return decodeClientSessionFields(fields, blobs)
	case "timer":
		return decodeTimerFields(fields)
	case "consensusModule":
		return decodeConsensusModuleFields(fields)
	default:
		return nil, fmt.Errorf("unknown template %q", template)
	}
}

func decodeSessionCloseRequestFields(
	fields map[string]any,
) (codecs.SessionCloseRequestFields, error) {
	termID, sessionID, err := requireSessionLeadershipPair(fields)
	return codecs.SessionCloseRequestFields{LeadershipTermID: termID, ClusterSessionID: sessionID}, err
}

func decodeSessionKeepAliveFields(fields map[string]any) (codecs.SessionKeepAliveFields, error) {
	termID, sessionID, err := requireSessionLeadershipPair(fields)
	return codecs.SessionKeepAliveFields{LeadershipTermID: termID, ClusterSessionID: sessionID}, err
}

func decodeSessionMessageHeaderFields(
	fields map[string]any,
) (codecs.SessionMessageHeaderFields, error) {
	var out codecs.SessionMessageHeaderFields
	var err error

	out.LeadershipTermID, out.ClusterSessionID, err = requireSessionLeadershipPair(fields)
	if err != nil {
		return out, err
	}
	out.Timestamp, err = requireInt64(fields, "timestamp")
	return out, err
}

func decodeSessionEventFields(fields map[string]any) (codecs.SessionEventFields, error) {
	var out codecs.SessionEventFields
	var err error

	out.ClusterSessionID, err = requireInt64(fields, "clusterSessionId")
	if err != nil {
		return out, err
	}
	out.CorrelationID, err = requireInt64(fields, "correlationId")
	if err != nil {
		return out, err
	}
	out.LeadershipTermID, err = requireInt64(fields, "leadershipTermId")
	if err != nil {
		return out, err
	}
	out.LeaderMemberID, err = requireInt32(fields, "leaderMemberId")
	if err != nil {
		return out, err
	}
	out.Code, err = lookupEnum(fields, "code", LookupEventCode)
	if err != nil {
		return out, err
	}
	out.Version, err = optionalInt32(fields, "version")
	if err != nil {
		return out, err
	}
	out.LeaderHeartbeatTimeoutNs, err = HeartbeatFromCatalog(fields[SessionEventHeartbeatField])
	if err != nil {
		return out, fmt.Errorf("%s: %w", SessionEventHeartbeatField, err)
	}
	out.Detail, err = optionalCatalogStringBytes(fields, "detail")
	return out, err
}

func decodeSessionConnectRequestFields(
	fields map[string]any,
	actingVersion uint16,
	blobs map[string][]byte,
) (codecs.SessionConnectRequestFields, error) {
	var out codecs.SessionConnectRequestFields
	var err error

	out.CorrelationID, err = requireInt64(fields, "correlationId")
	if err != nil {
		return out, err
	}
	out.ResponseStreamID, err = requireInt32(fields, "responseStreamId")
	if err != nil {
		return out, err
	}
	err = ValidateConnectVersion(templateSessionConnectRequest, actingVersion, fields)
	if err != nil {
		return out, err
	}
	out.Version, err = optionalInt32(fields, "version")
	if err != nil {
		return out, err
	}
	out.ResponseChannel, err = optionalCatalogStringBytes(fields, "responseChannel")
	if err != nil {
		return out, err
	}
	out.EncodedCredentials, err = optionalBytesField(fields, blobs, "encodedCredentials")
	if err != nil {
		return out, err
	}
	out.ClientInfo, err = optionalCatalogStringBytes(fields, "clientInfo")
	return out, err
}

func decodeNewLeaderEventFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.NewLeaderEventFields, error) {
	var out codecs.NewLeaderEventFields
	var err error

	out.LeadershipTermID, out.ClusterSessionID, err = requireSessionLeadershipPair(fields)
	if err != nil {
		return out, err
	}
	out.LeaderMemberID, err = requireInt32(fields, "leaderMemberId")
	if err != nil {
		return out, err
	}
	out.IngressEndpoints, err = optionalCatalogVarStringBytes(fields, blobs, "ingressEndpoints")
	return out, err
}

func decodeChallengeFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.ChallengeFields, error) {
	var out codecs.ChallengeFields
	var err error

	out.CorrelationID, out.ClusterSessionID, err = requireCorrelationClusterPair(fields)
	if err != nil {
		return out, err
	}
	out.EncodedChallenge, err = optionalBytesField(fields, blobs, "encodedChallenge")
	return out, err
}

func decodeChallengeResponseFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.ChallengeResponseFields, error) {
	var out codecs.ChallengeResponseFields
	var err error

	out.CorrelationID, out.ClusterSessionID, err = requireCorrelationClusterPair(fields)
	if err != nil {
		return out, err
	}
	out.EncodedCredentials, err = optionalBytesField(fields, blobs, "encodedCredentials")
	return out, err
}

func decodeAdminRequestFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.AdminRequestFields, error) {
	var out codecs.AdminRequestFields
	var err error

	out.LeadershipTermID, out.ClusterSessionID, err = requireSessionLeadershipPair(fields)
	if err != nil {
		return out, err
	}
	out.CorrelationID, err = requireInt64(fields, "correlationId")
	if err != nil {
		return out, err
	}
	out.RequestType, err = lookupEnum(fields, "requestType", LookupAdminRequestType)
	if err != nil {
		return out, err
	}
	out.Payload, err = optionalBytesField(fields, blobs, "payload")
	return out, err
}

func decodeAdminResponseFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.AdminResponseFields, error) {
	var out codecs.AdminResponseFields
	var err error

	out.ClusterSessionID, err = requireInt64(fields, "clusterSessionId")
	if err != nil {
		return out, err
	}
	out.CorrelationID, err = requireInt64(fields, "correlationId")
	if err != nil {
		return out, err
	}
	out.RequestType, err = lookupEnum(fields, "requestType", LookupAdminRequestType)
	if err != nil {
		return out, err
	}
	out.ResponseCode, err = lookupEnum(fields, "responseCode", LookupAdminResponseCode)
	if err != nil {
		return out, err
	}
	out.Message, err = optionalCatalogStringBytes(fields, "message")
	if err != nil {
		return out, err
	}
	out.Payload, err = optionalBytesField(fields, blobs, "payload")
	return out, err
}

func decodeSnapshotMarkerFields(fields map[string]any) (codecs.SnapshotMarkerFields, error) {
	var out codecs.SnapshotMarkerFields
	var err error

	out.TypeID, err = requireInt64(fields, "typeId")
	if err != nil {
		return out, err
	}
	out.LogPosition, err = requireInt64(fields, "logPosition")
	if err != nil {
		return out, err
	}
	out.LeadershipTermID, err = requireInt64(fields, "leadershipTermId")
	if err != nil {
		return out, err
	}
	out.Index, err = requireInt32(fields, "index")
	if err != nil {
		return out, err
	}
	out.Mark, err = lookupEnum(fields, "mark", LookupSnapshotMark)
	if err != nil {
		return out, err
	}
	out.TimeUnit, err = optionalEnum(fields, "timeUnit", LookupClusterTimeUnit)
	if err != nil {
		return out, err
	}
	out.AppVersion, err = optionalInt32(fields, "appVersion")
	return out, err
}

func decodeClientSessionFields(
	fields map[string]any,
	blobs map[string][]byte,
) (codecs.ClientSessionFields, error) {
	var out codecs.ClientSessionFields
	var err error

	out.ClusterSessionID, err = requireInt64(fields, "clusterSessionId")
	if err != nil {
		return out, err
	}
	out.ResponseStreamID, err = requireInt32(fields, "responseStreamId")
	if err != nil {
		return out, err
	}
	out.ResponseChannel, err = optionalCatalogStringBytes(fields, "responseChannel")
	if err != nil {
		return out, err
	}
	out.EncodedPrincipal, err = optionalBytesField(fields, blobs, "encodedPrincipal")
	return out, err
}

func decodeTimerFields(fields map[string]any) (codecs.TimerFields, error) {
	correlationID, deadline, err := requireInt64Pair(fields, "correlationId", "deadline")
	return codecs.TimerFields{CorrelationID: correlationID, Deadline: deadline}, err
}

func decodeConsensusModuleFields(fields map[string]any) (codecs.ConsensusModuleFields, error) {
	var out codecs.ConsensusModuleFields
	var err error

	out.NextSessionID, err = requireInt64(fields, "nextSessionId")
	if err != nil {
		return out, err
	}
	out.NextServiceSessionID, err = optionalInt64(fields, "nextServiceSessionId")
	if err != nil {
		return out, err
	}
	out.LogServiceSessionID, err = optionalInt64(fields, "logServiceSessionId")
	if err != nil {
		return out, err
	}
	out.PendingMessageCapacity, err = optionalInt32(fields, "pendingMessageCapacity")
	return out, err
}

func requireInt64Pair(fields map[string]any, key1, key2 string) (int64, int64, error) {
	v1, err := requireInt64(fields, key1)
	if err != nil {
		return 0, 0, err
	}
	v2, err := requireInt64(fields, key2)
	return v1, v2, err
}

func requireSessionLeadershipPair(fields map[string]any) (int64, int64, error) {
	return requireInt64Pair(fields, "leadershipTermId", "clusterSessionId")
}

func requireCorrelationClusterPair(fields map[string]any) (int64, int64, error) {
	return requireInt64Pair(fields, "correlationId", "clusterSessionId")
}

func requireInt64(fields map[string]any, key string) (int64, error) {
	value, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	return jsonInt64(value, key)
}

func requireInt32(fields map[string]any, key string) (int32, error) {
	value, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	return jsonInt32(value, key)
}

func jsonInt64(value any, field string) (int64, error) {
	n, err := Int64FromJSONValue(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	return n, nil
}

func jsonInt32(value any, field string) (int32, error) {
	n, err := jsonInt64(value, field)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("%s: out of int32 range", field)
	}
	return int32(n), nil
}

func optionalInt32(fields map[string]any, key string) (int32, error) {
	value, ok := fields[key]
	if !ok {
		return 0, nil
	}
	return jsonInt32(value, key)
}

func optionalInt64(fields map[string]any, key string) (int64, error) {
	value, ok := fields[key]
	if !ok {
		return 0, nil
	}
	return jsonInt64(value, key)
}

func optionalCatalogStringBytes(fields map[string]any, key string) ([]byte, error) {
	value, ok := fields[key]
	if !ok {
		return nil, nil
	}
	data, err := OptionalStringFromCatalog(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return data, nil
}

func optionalCatalogVarStringBytes(
	fields map[string]any,
	blobs map[string][]byte,
	key string,
) ([]byte, error) {
	value, ok := fields[key]
	if !ok {
		return nil, nil
	}
	data, err := resolveCatalogVarStringField(key, value, blobs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return data, nil
}

func optionalBytesField(
	fields map[string]any,
	blobs map[string][]byte,
	key string,
) ([]byte, error) {
	value, ok := fields[key]
	if !ok {
		return nil, nil
	}
	return decodeOptionalCatalogBytes(value, blobs, key)
}

func decodeOptionalCatalogBytes(value any, blobs map[string][]byte, key string) ([]byte, error) {
	data, err := DecodeBytesValue(value, blobs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return data, nil
}

func lookupEnum[T ~int32](
	fields map[string]any,
	key string,
	lookup func(string) (T, error),
) (T, error) {
	name, err := requireEnumString(fields, key)
	if err != nil {
		var zero T
		return zero, err
	}
	return lookup(name)
}

// optionalEnum returns the zero value when key is absent. Otherwise
// optionalEnum calls lookupEnum.
func optionalEnum[T ~int32](
	fields map[string]any,
	key string,
	lookup func(string) (T, error),
) (T, error) {
	if _, ok := fields[key]; !ok {
		var zero T
		return zero, nil
	}
	return lookupEnum(fields, key, lookup)
}

func requireEnumString(fields map[string]any, key string) (string, error) {
	value, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("missing %s", key)
	}
	name, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: expected string", key)
	}
	return name, nil
}
