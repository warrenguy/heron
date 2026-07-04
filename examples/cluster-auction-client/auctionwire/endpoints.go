package auctionwire

import (
	"strconv"
	"strings"
)

const (
	DefaultPortBase               = 9000
	DefaultPortsPerNode           = 100
	DefaultClientFacingPortOffset = 2 // CLIENT_FACING_PORT_OFFSET
)

// IngressEndpoints builds comma-separated memberId=host:port pairs for
// client-facing ingress ports, matching BasicAuctionClusterClient.ingressEndpoints.
func IngressEndpoints(hostnames []string, portBase, portsPerNode, offset int) string {
	if len(hostnames) == 0 {
		return ""
	}
	var b strings.Builder
	for memberID, hostname := range hostnames {
		if memberID > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(memberID))
		b.WriteByte('=')
		b.WriteString(hostname)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(portBase + memberID*portsPerNode + offset))
	}
	return b.String()
}
