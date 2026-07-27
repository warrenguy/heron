//go:build integration && cluster_java

// Package clusterjava is the Java cluster integration contract oracle.
// oracle.go: single-node IPC goldens; failover_oracle.go: 3-node UDP failover goldens.
// Hand-sync with ClusterConstantsTest and ClusterConstantsFailoverTest (like java_interop).
package clusterjava

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	PortBase            = 9000
	MemberID            = 0
	MemberCount         = 1
	IngressEndpointHost = "localhost"
)

const (
	clientFacingPortOffset = 2
	IngressEndpointPort    = PortBase + clientFacingPortOffset
	termLength64kParam     = "term-length=64k"
)

const (
	// IngressChannel and EgressChannel fix 64KiB term length for Java cluster IPC parity.
	IngressChannel = harness.IPCChannel + "?" + termLength64kParam
	EgressChannel  = harness.IPCChannel + "?" + termLength64kParam

	// IngressEndpoints is empty for IPC passthrough.
	IngressEndpoints = ""
)

const (
	IngressStreamID = client.DefaultIngressStreamID
	EgressStreamID  = client.DefaultEgressStreamID
)

const (
	ComposeAeronDir = "/dev/shm/aeron"
	// ComposeDir is where failover-runtime copies compose.yaml overlays in-container.
	ComposeDir = "/app/compose"
	BaseDir    = "/tmp/heron-cluster-0"
)

const (
	ComposeGateEnvKey          = "HERON_CLUSTER_JAVA_COMPOSE"
	FailoverComposeGateEnvKey  = "HERON_CLUSTER_JAVA_FAILOVER_COMPOSE"
	ComposeDirEnvKey           = "HERON_CLUSTER_JAVA_COMPOSE_DIR"
	FailoverComposeProfileName = "failover"
	composeSkipRunMake         = "requires docker compose; run make "
	ComposeSkipHint            = "cluster java " + composeSkipRunMake + "test-cluster-java"
	FailoverComposeSkipHint    = "failover " + composeSkipRunMake + "test-cluster-java-failover"
)

const TermMinLength = logbuffer.TermMinLength

// ClusterMembersString is member 0 topology golden for PortBase=9000 and localhost.
// Ports: port = PortBase + (memberId * 100) + offset (archive=1, client-facing=2,
// member=3, log=4, transfer=5).
const ClusterMembersString = "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005," +
	"localhost:9001|"

// IngressEndpointsString is the UDP ingress endpoints golden for member 0 at
// client-facing offset 2 (port 9002).
const IngressEndpointsString = "0=localhost:9002"

func harnessConfig(
	ingressChannel string,
	egressChannel string,
	ingressEndpoints string,
) client.Config {
	return client.Config{
		IngressChannel:   ingressChannel,
		EgressChannel:    egressChannel,
		IngressEndpoints: ingressEndpoints,
	}
}

// HarnessConfig returns cluster client defaults for IPC connect tests.
// Stream IDs are left at zero so callers or NewAsyncConnect Normalize applies defaults.
// AeronDir is unset; callers assign it from compose env before connect.
func HarnessConfig() client.Config {
	return harnessConfig(
		IngressChannel,
		EgressChannel,
		IngressEndpoints,
	)
}
