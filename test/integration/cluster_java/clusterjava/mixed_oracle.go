//go:build integration && cluster_java

package clusterjava

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/warrenguy/heron/cluster"
)

// Mixed Go/Java multi-node goldens. Hand-sync with ClusterConstantsMixedTest.
// Java ClusterConfig helpers stay authoritative for topology strings.
// Appointed leader is member 0 (Java) or member 2 (Go). Go member is id 2.

const (
	// MixedComposeJavaLeaderEnvKey is set to "1" by the java-leader overlay.
	MixedComposeJavaLeaderEnvKey = "HERON_CLUSTER_MIXED_JAVA_LEADER_COMPOSE"

	// MixedComposeGoLeaderEnvKey is set to "1" by the go-leader overlay.
	MixedComposeGoLeaderEnvKey = "HERON_CLUSTER_MIXED_GO_LEADER_COMPOSE"

	// MixedSmokeNameJavaLeader is the TestClusterMixedJavaLeader function name.
	//
	// Compose overlay -test.run, scripts/cluster-mixed-smoke.sh SMOKE_NAME, and
	// MixedRequiredSmoke must use this same literal. A rename that misses a site
	// must fail the run rather than pass with no proof completed.
	MixedSmokeNameJavaLeader = "TestClusterMixedJavaLeader"

	// MixedSmokeNameGoLeader is the TestClusterMixedGoLeader function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameGoLeader = "TestClusterMixedGoLeader"

	// Skip-hint strings name the make target for each mixed compose overlay.
	MixedComposeSkipHintJavaLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-java-leader"
	MixedComposeSkipHintGoLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader"

	MixedMemberCount = 3
	MixedGoMemberID  = int32(2)

	// MixedAppointedLeaderIDJavaLeader is the appointed leader id for java-leader.
	MixedAppointedLeaderIDJavaLeader = int32(0)

	// MixedAppointedLeaderIDGoLeader is the appointed leader id for go-leader.
	MixedAppointedLeaderIDGoLeader = int32(2)

	// MixedReadyPath is the readiness file path. The Go service writes MixedReadyPath after
	// the consensus module opens. MixedReadyPath sits outside every Aeron directory so
	// no driver maps it. The Go healthcheck tests MixedReadyPath. The java-leader
	// compose overlay starts Java peers only after that healthcheck passes.
	MixedReadyPath = "/dev/shm/heron-mixed/member-ready"
)

// Default bridge address literals for the 172.30.0.0/24 topology.
// Compose assigns each address as ipv4_address on the default subnet.
//
// Compose smokes pick a per-run /24 under 172.30.0.0/16 and set
// HERON_CLUSTER_HOSTNAMES. Runtime helpers read MixedHostnamesEnvKey.
//
// MixedMember2Address is the default aeronmd address. The Go test binary
// joins that driver with network_mode service:aeronmd. Consensus and log
// endpoints listen on the member-2 address that peers list in the members string.
const (
	MixedMember0Address = "172.30.0.10"
	MixedMember1Address = "172.30.0.11"
	MixedMember2Address = "172.30.0.12"
	MixedAeronMDAddress = MixedMember2Address

	// MixedHostnamesEnvKey is the compose env key for the three member address
	// literals in id order. Java ClusterNode and the Go smoke read this value.
	MixedHostnamesEnvKey = "HERON_CLUSTER_HOSTNAMES"
)

const (
	// MixedIngressEndpoints is the induction client endpoint map for Java members
	// 0 and 1 on the default topology. The map omits Go member 2. That member has
	// no client ingress listener. The cluster members string still carries member
	// 2's ingress port slot for topology. Compose smokes use
	// MixedComposeIngressEndpoints for the compose host list.
	MixedIngressEndpoints = "0=" + MixedMember0Address + ":9002,1=" + MixedMember1Address + ":9102"

	// MixedIngressChannel is the induction client ingress URI. The URI carries no
	// endpoint query. Endpoints come from MixedIngressEndpoints or
	// MixedComposeIngressEndpoints.
	MixedIngressChannel = udpChannel64k

	// MixedEgressChannel advertises the default shared driver address (member 2 /
	// aeronmd). MixedEgressChannel must not use loopback. Cluster nodes publish
	// egress from separate containers. Compose smokes use MixedComposeEgressChannel.
	MixedEgressChannel = UDPChannel + "?endpoint=" + MixedAeronMDAddress + ":0|" + termLength64kParam
)

// Per-member Aeron media directories under the shared /dev/shm volume.
const (
	MixedAeronDirMember0 = "/dev/shm/aeron-j0"
	MixedAeronDirMember1 = "/dev/shm/aeron-j1"
	MixedAeronDirGo      = "/dev/shm/aeron-go"
)

// Per-member baseDirs. Compose mounts each path as a volume root.
// Paths are baseDirs, not the cluster/ subdirectory. Go uses member id 2.
const (
	// MixedBaseDirPrefix matches ClusterConstants.MIXED_BASE_DIR_PREFIX.
	MixedBaseDirPrefix = "/tmp/heron-cluster-"

	MixedClusterDirMember0 = MixedBaseDirPrefix + "0"
	MixedClusterDirMember1 = MixedBaseDirPrefix + "1"
	MixedClusterDirGo      = MixedBaseDirPrefix + "2"

	// ServiceProbeFilename is the status file name under each Java member baseDir.
	// EchoService writes the file on term start.
	//
	// Hand-sync with ClusterConstants.SERVICE_PROBE_FILENAME.
	ServiceProbeFilename = "service-probe"

	// Service-probe key names.
	//
	// Hand-sync with ClusterConstants.SERVICE_PROBE_KEY_*.
	ServiceProbeKeyTerm        = "term"
	ServiceProbeKeyLogPosition = "logPosition"
	ServiceProbeKeyRole        = "role"
	ServiceProbeKeyMemberID    = "memberId"
)

const (
	// MixedLogChannelUnicast is the Go log channel template when a Java member is the
	// appointed leader. Topology appends the per-member log endpoint. Unicast
	// only. No interface or ttl. Same URI as cluster.DefaultLogChannel.
	MixedLogChannelUnicast = cluster.DefaultLogChannel

	// MixedLogEndpoint is the shared multicast log endpoint for go-leader.
	// Final octet is odd so the media driver can derive the control address.
	// Hand-sync with ClusterConstants.MIXED_LOG_ENDPOINT.
	MixedLogEndpoint = "239.255.12.15:9004"

	// MixedLogChannelMulticast is the Go log channel template when the Go member
	// is appointed. interface= pins the compose bridge subnet; ttl=1 keeps frames
	// on that network. Must not carry endpoint=: cluster.Config rejects endpoint
	// on LogChannel; Topology.LogChannel merges MixedLogEndpoint from the members
	// string. Java MIXED_LOG_CHANNEL_MULTICAST keeps endpoint= in the final URI so
	// the consensus module clears isLogMdc. Intentional asymmetry, not byte parity.
	MixedLogChannelMulticast = MixedLogChannelUnicast +
		"|interface=172.30.0.0/16|ttl=1"
)

// MixedClusterMembersJavaLeader is the three-node members golden with per-member
// unicast log endpoints. PortBase is 9000. Hosts are the MixedMember*Address constants.
//
// Ports: PortBase + memberId*100 + offset (archive=1, client-facing=2,
// member=3, log=4, transfer=5).
const MixedClusterMembersJavaLeader = "0,172.30.0.10:9002,172.30.0.10:9003,172.30.0.10:9004," +
	"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
	"172.30.0.11:9104,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
	"172.30.0.12:9203,172.30.0.12:9204,172.30.0.12:9205,172.30.0.12:9201|"

// MixedClusterMembersGoLeader is the three-node members golden with every log
// slot set to MixedLogEndpoint. Hand-sync with MIXED_CLUSTER_MEMBERS_GO_LEADER.
const MixedClusterMembersGoLeader = "0,172.30.0.10:9002,172.30.0.10:9003," +
	MixedLogEndpoint + "," +
	"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
	MixedLogEndpoint + ",172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
	"172.30.0.12:9203," + MixedLogEndpoint + ",172.30.0.12:9205,172.30.0.12:9201|"

// Budgets for mixed cluster smokes. Each value is a deadline ceiling.
const (
	MixedStartupBudget  = 30 * time.Second
	MixedElectionBudget = 20 * time.Second
	MixedConnectBudget  = 20 * time.Second
	MixedCommitBudget   = 20 * time.Second

	// MixedTestTimeoutJavaLeader bounds the java-leader smoke wall clock.
	MixedTestTimeoutJavaLeader = 120 * time.Second

	// MixedTestTimeoutGoLeader bounds the go-leader smoke wall clock.
	// Stack: Startup, Election, Election for service probe, Commit.
	//
	// No client connect wait.
	MixedTestTimeoutGoLeader = 120 * time.Second
)

// MixedRequiredSmoke reports which mixed proof the current overlay requires.
//
// Behaviour: reads the two overlay gate keys through getEnv. A key set to
// "1" names that overlay's proof. Any other value, including unset, sets no
// gate. No gate set means no proof is required (developer run outside
// compose). Both gates set to "1" is a fixture error; the function still
// returns the java-leader name so the failure path stays diagnostic and does
// not panic.
//
// Concurrency: pure. Reads only through getEnv.
//
// Returns:
//   - name, true: the named proof must run to completion
//   - "", false: no proof is required
func MixedRequiredSmoke(getEnv func(string) string) (string, bool) {
	if getEnv(MixedComposeJavaLeaderEnvKey) == "1" {
		return MixedSmokeNameJavaLeader, true
	}
	if getEnv(MixedComposeGoLeaderEnvKey) == "1" {
		return MixedSmokeNameGoLeader, true
	}
	return "", false
}

// MixedMemberHostnames returns the default three member address literals in id
// order. Unit goldens and the manual 172.30.0.0/24 topology use this list.
//
// Compose smokes call MixedComposeHostnames when a per-run /24 overrides the defaults.
//
// Ownership: the caller owns the returned slice.
func MixedMemberHostnames() []string {
	return []string{MixedMember0Address, MixedMember1Address, MixedMember2Address}
}

// MixedComposeHostnames returns the three member addresses for a compose run.
//
// Behaviour:
//   - When MixedHostnamesEnvKey is unset or blank, returns MixedMemberHostnames.
//   - When MixedHostnamesEnvKey is set, returns exactly MixedMemberCount non-empty
//     comma-separated hosts in id order.
//   - Present-but-invalid values fail closed. Same contract as Java multi-node
//     parseHostnames.
//
// Ownership: the caller owns the returned slice.
//
// Returns an error when:
//   - MixedHostnamesEnvKey is set and the host count is not MixedMemberCount
//   - MixedHostnamesEnvKey is set and a comma-delimited token is blank
func MixedComposeHostnames() ([]string, error) {
	raw := strings.TrimSpace(os.Getenv(MixedHostnamesEnvKey))
	if raw == "" {
		return MixedMemberHostnames(), nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) != MixedMemberCount {
		return nil, fmt.Errorf(
			"%s must list exactly %d hostnames (got %d)",
			MixedHostnamesEnvKey,
			MixedMemberCount,
			len(parts),
		)
	}
	out := make([]string, MixedMemberCount)
	for i, p := range parts {
		host := strings.TrimSpace(p)
		if host == "" {
			return nil, fmt.Errorf("%s contains empty hostname", MixedHostnamesEnvKey)
		}
		out[i] = host
	}
	return out, nil
}

// MixedComposeClusterMembers builds the members string for the compose host list.
//
// Log endpoints:
//   - sharedLogEndpoint empty keeps per-member unicast log endpoints (java-leader).
//   - sharedLogEndpoint non-empty replaces every log slot (go-leader multicast).
//
// Ownership: the caller owns the returned string.
//
// Returns an error when:
//   - MixedComposeHostnames fails
//   - cluster.BuildClusterMembersString fails
func MixedComposeClusterMembers(sharedLogEndpoint string) (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	return cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		SharedLogEndpoint: sharedLogEndpoint,
	})
}

// MixedComposeIngressEndpoints is the induction client endpoint map for Java
// members 0 and 1 on the compose host list.
//
// Ownership: the caller owns the returned string.
//
// Returns an error when:
//   - MixedComposeHostnames fails
func MixedComposeIngressEndpoints() (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	port0 := cluster.CalculatePort(0, cluster.DefaultPortBase, cluster.ClientFacingPortOffset)
	port1 := cluster.CalculatePort(1, cluster.DefaultPortBase, cluster.ClientFacingPortOffset)
	return fmt.Sprintf("0=%s:%d,1=%s:%d", hosts[0], port0, hosts[1], port1), nil
}

// MixedComposeEgressChannel advertises the compose member-2 / aeronmd address.
//
// Ownership: the caller owns the returned string.
//
// Returns an error when:
//   - MixedComposeHostnames fails
func MixedComposeEgressChannel() (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	return UDPChannel + "?endpoint=" + hosts[2] + ":0|" + termLength64kParam, nil
}

// ServiceProbe is one parsed EchoService service-probe file.
type ServiceProbe struct {
	Term        int64
	LogPosition int64
	Role        string
	MemberID    int32
}

// ServiceProbePath returns the service-probe path for memberID under MixedBaseDirPrefix.
//
// Ownership: the caller owns the returned string.
func ServiceProbePath(memberID int) string {
	return MixedBaseDirPrefix + strconv.Itoa(memberID) + "/" + ServiceProbeFilename
}

// ParseServiceProbe parses key=value lines from an EchoService probe file.
//
// Returns an error when:
// - a required key is missing
// - a key is duplicate or unknown
// - term, logPosition, or memberId is not an integer
// - role is empty
// - a line is not key=value
//
// Ownership: the caller owns the returned value.
func ParseServiceProbe(data []byte) (ServiceProbe, error) {
	var probe ServiceProbe
	seen := make(map[string]bool, 4)

	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return ServiceProbe{}, fmt.Errorf("service-probe line %d: want key=value, got %q", i+1, line)
		}
		if seen[key] {
			return ServiceProbe{}, fmt.Errorf("service-probe duplicate key %q", key)
		}
		switch key {
		case ServiceProbeKeyTerm:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ServiceProbe{}, fmt.Errorf("service-probe term: %w", err)
			}
			probe.Term = n
		case ServiceProbeKeyLogPosition:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ServiceProbe{}, fmt.Errorf("service-probe logPosition: %w", err)
			}
			probe.LogPosition = n
		case ServiceProbeKeyRole:
			if value == "" {
				return ServiceProbe{}, errors.New("service-probe role: empty")
			}
			probe.Role = value
		case ServiceProbeKeyMemberID:
			n, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return ServiceProbe{}, fmt.Errorf("service-probe memberId: %w", err)
			}
			probe.MemberID = int32(n)
		default:
			return ServiceProbe{}, fmt.Errorf("service-probe line %d: unknown key %q", i+1, key)
		}
		seen[key] = true
	}

	for _, key := range []string{
		ServiceProbeKeyTerm,
		ServiceProbeKeyLogPosition,
		ServiceProbeKeyRole,
		ServiceProbeKeyMemberID,
	} {
		if !seen[key] {
			return ServiceProbe{}, fmt.Errorf("service-probe missing key %q", key)
		}
	}
	return probe, nil
}
