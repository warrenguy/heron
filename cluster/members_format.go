package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/election"
)

// CalculatePort returns the UDP port for a member's port block slot:
//
//	portBase + int(memberID)*PortsPerNode + offset
//
// Pure arithmetic with no range guard, matching upstream calculatePort.
//
// Callers that accept external input use BuildClusterMembersString, which
// rejects any derived port outside MinValidPort..MaxValidPort. Config.Validate
// applies the same range check to the consensus and log endpoint ports it parses
// out of Config.ClusterMembers.
func CalculatePort(memberID int32, portBase, offset int) int {
	return portBase + int(memberID)*PortsPerNode + offset
}

// MembersStringConfig holds inputs for BuildClusterMembersString.
type MembersStringConfig struct {
	// StartingMemberID is the id of IngressHostnames[0]. Ids increment by one.
	StartingMemberID int32

	// IngressHostnames supplies the client-facing host per member.
	IngressHostnames []string

	// ClusterHostnames supplies the consensus, log, catchup, and archive host
	// per member. Same length as IngressHostnames; may be the same slice.
	ClusterHostnames []string

	// PortBase is the first port of member zero's block. Zero takes
	// DefaultPortBase; there is no way to ask for a base of literally zero,
	// which would place a cluster on privileged ports 1 through 5.
	PortBase int

	// SharedLogEndpoint replaces the derived per-member log endpoint on every
	// row when non-empty. One multicast group address and port for all members
	// is the delivery scheme this package supports, because the transport client
	// has no destination-add API for exclusive publications.
	//
	// Empty preserves the per-member unicast log endpoint and reproduces
	// upstream output exactly.
	//
	// Multicast data addresses must have an odd final octet: the media driver
	// derives the control address by incrementing that octet and rejects an
	// even group when the publication is added. This package does not check
	// the constraint; the driver is the sole validator.
	SharedLogEndpoint string
}

// BuildClusterMembersString formats a cluster members string parseable by
// election.ParseMembers.
//
// Field order per member is id, ingress, consensus, log, catchup, archive, and
// every row is terminated by a pipe including the last. With an empty
// SharedLogEndpoint the output is byte-identical to upstream clusterMembers for
// the same inputs.
//
// A zero PortBase takes DefaultPortBase before any port is derived.
//
// Errors when hostname slice lengths differ, when either slice is empty, when a
// hostname is empty, when StartingMemberID is negative, or when any derived port
// falls outside MinValidPort..MaxValidPort. On error the returned string is empty.
func BuildClusterMembersString(cfg MembersStringConfig) (string, error) {
	if cfg.StartingMemberID < 0 {
		return "", errors.New("cluster: StartingMemberID must not be negative")
	}
	if len(cfg.IngressHostnames) == 0 || len(cfg.ClusterHostnames) == 0 {
		return "", errors.New("cluster: IngressHostnames and ClusterHostnames must not be empty")
	}
	if len(cfg.IngressHostnames) != len(cfg.ClusterHostnames) {
		return "", errors.New("cluster: IngressHostnames and ClusterHostnames must be the same length")
	}

	portBase := cfg.PortBase
	if portBase == 0 {
		portBase = DefaultPortBase
	}

	var b strings.Builder
	for i, ingressHost := range cfg.IngressHostnames {
		clusterHost := cfg.ClusterHostnames[i]
		if ingressHost == "" || clusterHost == "" {
			return "", errors.New("cluster: hostname must not be empty")
		}

		memberID := cfg.StartingMemberID + int32(i)

		// Field order matches upstream clusterMembers: ingress, consensus, log,
		// catchup, archive. SharedLogEndpoint replaces the derived log endpoint.
		fields := [...]struct {
			host   string
			offset int
			fixed  string
		}{
			{ingressHost, ClientFacingPortOffset, ""},
			{clusterHost, MemberFacingPortOffset, ""},
			{clusterHost, LogPortOffset, cfg.SharedLogEndpoint},
			{clusterHost, TransferPortOffset, ""},
			{clusterHost, ArchiveControlPortOffset, ""},
		}

		b.WriteString(strconv.FormatInt(int64(memberID), 10))
		for _, f := range fields {
			field := f.fixed
			if field == "" {
				var err error
				field, err = formatEndpoint(memberID, f.host, portBase, f.offset)
				if err != nil {
					return "", err
				}
			}
			b.WriteByte(',')
			b.WriteString(field)
		}
		b.WriteByte('|')
	}

	return b.String(), nil
}

func formatEndpoint(memberID int32, hostname string, portBase, offset int) (string, error) {
	port := CalculatePort(memberID, portBase, offset)
	if !IsValidPort(port) {
		return "", fmt.Errorf("cluster: derived port %d is out of range", port)
	}
	return hostname + ":" + strconv.Itoa(port), nil
}

// buildIngressEndpointsString formats members as "id=endpoint" pairs via
// client.FormatIngressEndpoints. Pairs are comma-joined and sorted by member id
// ascending. Cluster and client share one encoder for Config.IngressEndpoints
// (for example "0=host:port,1=host:port").
//
// Skips nil members. Empty or all-nil input returns "".
func buildIngressEndpointsString(members []*election.Member) string {
	ingress := make(client.MemberIngressMap, len(members))
	for _, m := range members {
		if m != nil {
			ingress[m.ID] = &client.MemberIngress{
				MemberID: m.ID,
				Endpoint: m.IngressEndpoint,
			}
		}
	}
	return client.FormatIngressEndpoints(ingress)
}
