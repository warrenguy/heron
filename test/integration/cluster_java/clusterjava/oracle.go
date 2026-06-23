//go:build integration && cluster_java

// Package clusterjava is the cross-language harness contract oracle for the Java
// cluster integration module. Constant goldens are hand-synced with Java
// ClusterConstantsTest (like java_interop), not a shared artifact; keep both tables aligned.
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
)

const (
	// IngressChannel and EgressChannel pin 64KiB term length for Java cluster IPC parity.
	IngressChannel = harness.IPCChannel + "?term-length=64k"
	EgressChannel  = harness.IPCChannel + "?term-length=64k"

	// IngressEndpoints is empty for IPC passthrough.
	IngressEndpoints = ""
)

const (
	IngressStreamID = client.DefaultIngressStreamID
	EgressStreamID  = client.DefaultEgressStreamID
)

const (
	ComposeAeronDir = "/dev/shm/aeron"
	BaseDir         = "/tmp/heron-cluster-0"
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

// HarnessClusterContext returns normalized cluster client defaults for IPC connect
// tests. Stream IDs are left at zero so Normalize applies defaults. AeronDir is unset;
// callers assign it from compose env before connect.
func HarnessClusterContext() client.ClusterContext {
	return client.ClusterContext{
		IngressChannel:   IngressChannel,
		EgressChannel:    EgressChannel,
		IngressEndpoints: IngressEndpoints,
	}.Normalize()
}
