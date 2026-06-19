package catalogschema

import (
	"fmt"
	"math"

	"github.com/warrenguy/heron/aeron/sbe"
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
		return decodeNewLeaderEventFields(fields)
	case "challenge":
		return decodeChallengeFields(fields, blobs)
	case "challengeResponse":
		return decodeChallengeResponseFields(fields, blobs)
	case "adminRequest":
		return decodeAdminRequestFields(fields, blobs)
	case "adminResponse":
		return decodeAdminResponseFields(fields, blobs)
	default:
		return nil, fmt.Errorf("unknown template %q", template)
	}
}

func decodeSessionCloseRequestFields(fields map[string]any) (sbe.SessionCloseRequestFields, error) {
	termID, sessionID, err := requireSessionLeadershipPair(fields)
	return sbe.SessionCloseRequestFields{LeadershipTermID: termID, ClusterSessionID: sessionID}, err
}

func decodeSessionKeepAliveFields(fields map[string]any) (sbe.SessionKeepAliveFields, error) {
	termID, sessionID, err := requireSessionLeadershipPair(fields)
	return sbe.SessionKeepAliveFields{LeadershipTermID: termID, ClusterSessionID: sessionID}, err
}

func decodeSessionMessageHeaderFields(
	fields map[string]any,
) (sbe.SessionMessageHeaderFields, error) {
	var out sbe.SessionMessageHeaderFields
	var err error

	out.LeadershipTermID, out.ClusterSessionID, err = requireSessionLeadershipPair(fields)
	if err != nil {
		return out, err
	}
	out.Timestamp, err = requireInt64(fields, "timestamp")
	return out, err
}

func decodeSessionEventFields(fields map[string]any) (sbe.SessionEventFields, error) {
	var out sbe.SessionEventFields
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
) (sbe.SessionConnectRequestFields, error) {
	var out sbe.SessionConnectRequestFields
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

func decodeNewLeaderEventFields(fields map[string]any) (sbe.NewLeaderEventFields, error) {
	var out sbe.NewLeaderEventFields
	var err error

	out.LeadershipTermID, out.ClusterSessionID, err = requireSessionLeadershipPair(fields)
	if err != nil {
		return out, err
	}
	out.LeaderMemberID, err = requireInt32(fields, "leaderMemberId")
	if err != nil {
		return out, err
	}
	out.IngressEndpoints, err = optionalCatalogStringBytes(fields, "ingressEndpoints")
	return out, err
}

func decodeChallengeFields(
	fields map[string]any,
	blobs map[string][]byte,
) (sbe.ChallengeFields, error) {
	var out sbe.ChallengeFields
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
) (sbe.ChallengeResponseFields, error) {
	var out sbe.ChallengeResponseFields
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
) (sbe.AdminRequestFields, error) {
	var out sbe.AdminRequestFields
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
) (sbe.AdminResponseFields, error) {
	var out sbe.AdminResponseFields
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

func optionalBytesField(
	fields map[string]any,
	blobs map[string][]byte,
	key string,
) ([]byte, error) {
	value, ok := fields[key]
	if !ok {
		return nil, nil
	}
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
