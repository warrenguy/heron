package client

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/warrenguy/heron/aeron/conductor"
)

func appendIngressPairPrefix(b *strings.Builder, memberID int) {
	if b.Len() > 0 {
		b.WriteByte(',')
	}
	b.WriteString(strconv.Itoa(memberID))
	b.WriteByte('=')
}

func appendIngressEndpointPair(b *strings.Builder, memberID int, endpoint string) {
	appendIngressPairPrefix(b, memberID)
	b.WriteString(endpoint)
}

func appendIngressHostnamePair(b *strings.Builder, memberID int, hostname string, port int) {
	appendIngressPairPrefix(b, memberID)
	b.WriteString(hostname)
	b.WriteByte(':')
	b.WriteString(strconv.Itoa(port))
}

func sortedMemberIDs(m MemberIngressMap) []int32 {
	ids := make([]int32, 0, len(m))
	for id, ingress := range m {
		if ingress == nil {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func memberIngressFromEndpoint(memberID int32, endpoint string) *MemberIngress {
	return &MemberIngress{
		MemberID:       memberID,
		Endpoint:       endpoint,
		RegistrationID: conductor.NullCorrelationID,
	}
}

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
		out[memberID] = memberIngressFromEndpoint(memberID, ep)
	}
	return out, nil
}

// ParseIngressEndpointsBytes parses wire-format ingress endpoints from a byte view.
//
// Nil or empty input returns a non-nil empty map with no error. Non-empty input
// delegates to ParseIngressEndpoints after copying to string once.
func ParseIngressEndpointsBytes(endpoints []byte) (MemberIngressMap, error) {
	if len(endpoints) == 0 {
		return MemberIngressMap{}, nil
	}
	return ParseIngressEndpoints(string(endpoints))
}

// FormatIngressEndpoints serializes map entries as "id=endpoint,..." sorted by
// memberId ascending.
//
// For tests and debug only; not used on hot paths. Nil or empty map returns "".
// Nil map entries are omitted, consistent with Lookup.
func FormatIngressEndpoints(m MemberIngressMap) string {
	ids := sortedMemberIDs(m)
	if len(ids) == 0 {
		return ""
	}
	var b strings.Builder
	for _, id := range ids {
		appendIngressEndpointPair(&b, int(id), m[id].Endpoint)
	}
	return b.String()
}

const (
	// DefaultIngressPortBase is the default cluster ingress port base (9000).
	DefaultIngressPortBase = 9000

	// DefaultIngressPortsPerNode is the default ports reserved per cluster member (100).
	DefaultIngressPortsPerNode = 100

	// DefaultClientFacingPortOffset is the client-facing ingress offset within each
	// member's port block (CLIENT_FACING_PORT_OFFSET = 2).
	DefaultClientFacingPortOffset = 2
)

// IngressPortForMember returns the UDP port for a cluster member's client-facing
// ingress channel:
//
//	portBase + memberID*portsPerNode + clientFacingOffset
//
// Exported for tests and callers that need the port without formatting the full
// endpoints string. memberID is the cluster member id (0-based).
//
// Java ClusterConfig.calculatePort uses the same formula with
// ClusterConfig.PORTS_PER_NODE (100) hard-coded. This Go helper accepts
// portsPerNode as a parameter.
func IngressPortForMember(
	memberID, portBase, portsPerNode, clientFacingOffset int,
) int {
	return portBase + memberID*portsPerNode + clientFacingOffset
}

// FormatIngressEndpointsFromHostnames builds a comma-separated ingress endpoints
// string from hostnames and port parameters.
//
// memberId is the slice index (0..len(hostnames)-1). Each pair is
// "memberId=hostname:port" with port from IngressPortForMember. Pairs appear in
// ascending memberId order (slice iteration order). Member ids start at 0;
// hostnames[0] is always member 0.
//
// Nil or empty hostnames returns "". Empty hostname elements are emitted as-is
// (e.g. "0=:port"); callers should supply non-empty hostnames. Practical cluster
// sizes only: member ids are slice indices and must fit int32 for
// ParseIngressEndpoints roundtrip.
//
// UDP ingress only. For IPC or ipcx channels, leave ClusterContext.IngressEndpoints
// empty; Validate rejects non-empty endpoints on passthrough channels
// (validateClusterIngressEndpoints in context.go).
//
// Intended for ClusterContext.IngressEndpoints on UDP channels without ?endpoint=.
// May be validated with ParseIngressEndpoints before assigning to ClusterContext.
//
// Not used on offer/poll hot paths.
func FormatIngressEndpointsFromHostnames(
	hostnames []string,
	portBase, portsPerNode, clientFacingOffset int,
) string {
	if len(hostnames) == 0 {
		return ""
	}
	var b strings.Builder
	for memberID, hostname := range hostnames {
		appendIngressHostnamePair(
			&b,
			memberID,
			hostname,
			IngressPortForMember(memberID, portBase, portsPerNode, clientFacingOffset),
		)
	}
	return b.String()
}

// NewMemberIngressMapFromContext parses clusterCtx.IngressEndpoints.
//
// Caller should pass a normalized context; structural validation (IPC+endpoints,
// UDP endpoint= with ingress map) is enforced by Validate, not here. Invalid endpoint
// grammar returns a parse error.
func NewMemberIngressMapFromContext(clusterCtx ClusterContext) (MemberIngressMap, error) {
	return ParseIngressEndpoints(clusterCtx.IngressEndpoints)
}
