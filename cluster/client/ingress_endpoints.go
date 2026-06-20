package client

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

func parseIngressMemberID(pair, memberIDASCII string) (int32, error) {
	n, err := strconv.ParseInt(memberIDASCII, 10, 32)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("cluster client: invalid ingress member id: %s", pair)
	}
	return int32(n), nil
}

// ParseIngressEndpoints parses comma-separated memberId=endpoint pairs into a map.
//
// Empty input returns a non-nil empty map with no error. Duplicate member ids: last
// pair wins.
//
// Errors use the cluster client: prefix. Missing '=' in a pair cites the full
// original endpoints string; invalid member id or empty endpoint cites the malformed
// pair.
func ParseIngressEndpoints(endpoints string) (MemberIngressMap, error) {
	if endpoints == "" {
		return MemberIngressMap{}, nil
	}
	out := make(MemberIngressMap)
	for part := range strings.SplitSeq(endpoints, ",") {
		memberIDASCII, ep, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf(
				"cluster client: endpoint missing '=' separator: %s", endpoints,
			)
		}
		memberID, err := parseIngressMemberID(part, memberIDASCII)
		if err != nil {
			return nil, err
		}
		if ep == "" {
			return nil, fmt.Errorf(
				"cluster client: ingress member endpoint is empty: %s", part,
			)
		}
		out[memberID] = &MemberIngress{MemberID: memberID, Endpoint: ep}
	}
	return out, nil
}

// ParseIngressEndpointsBytes parses wire-format ingress endpoints from a byte view.
//
// Nil or empty input returns a non-nil empty map with no error. Non-empty input
// delegates to ParseIngressEndpoints after copying to string once.
func ParseIngressEndpointsBytes(endpoints []byte) (MemberIngressMap, error) {
	return ParseIngressEndpoints(string(endpoints))
}

// FormatIngressEndpoints serializes map entries as "id=endpoint,..." sorted by
// memberId ascending.
//
// For tests and debug only; not used on hot paths. Nil or empty map returns "".
// Nil map entries are omitted, consistent with Lookup.
func FormatIngressEndpoints(m MemberIngressMap) string {
	if len(m) == 0 {
		return ""
	}
	ids := make([]int32, 0, len(m))
	for id, ingress := range m {
		if ingress == nil {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ""
	}
	slices.Sort(ids)
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(int64(id), 10))
		b.WriteByte('=')
		b.WriteString(m[id].Endpoint)
	}
	return b.String()
}

// NewMemberIngressMapFromContext parses ctx.IngressEndpoints.
//
// Caller should pass a normalized context; structural validation (IPC+endpoints,
// UDP endpoint= with ingress map) is enforced by Validate, not here. Invalid endpoint
// grammar returns a parse error.
func NewMemberIngressMapFromContext(ctx ClusterContext) (MemberIngressMap, error) {
	return ParseIngressEndpoints(ctx.IngressEndpoints)
}
