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

	// MixedComposeExLeaderRejoinEnvKey is set to "1" by the ex-leader-rejoin overlay.
	MixedComposeExLeaderRejoinEnvKey = "HERON_CLUSTER_MIXED_EX_LEADER_REJOIN_COMPOSE"

	// MixedSmokeNameExLeaderRejoin is the TestClusterMixedGoExLeaderRejoin function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameExLeaderRejoin = "TestClusterMixedGoExLeaderRejoin"

	// MixedComposeJavaLeaderSnapshotEnvKey is set to "1" by the java-leader-snapshot overlay.
	MixedComposeJavaLeaderSnapshotEnvKey = "HERON_CLUSTER_MIXED_JAVA_LEADER_SNAPSHOT_COMPOSE"

	// MixedComposeGoLeaderSnapshotEnvKey is set to "1" by the go-leader-snapshot overlay.
	MixedComposeGoLeaderSnapshotEnvKey = "HERON_CLUSTER_MIXED_GO_LEADER_SNAPSHOT_COMPOSE"

	// MixedSmokeNameJavaLeaderSnapshot is the TestClusterMixedJavaLeaderSnapshotRestart function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameJavaLeaderSnapshot = "TestClusterMixedJavaLeaderSnapshotRestart"

	// MixedSmokeNameGoLeaderSnapshot is the TestClusterMixedGoLeaderSnapshotRestart function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameGoLeaderSnapshot = "TestClusterMixedGoLeaderSnapshotRestart"

	// Skip-hint strings name the make target for each mixed compose overlay.
	MixedComposeSkipHintJavaLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-java-leader"
	MixedComposeSkipHintGoLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader"
	MixedComposeSkipHintExLeaderRejoin = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-ex-leader-rejoin"
	MixedComposeSkipHintJavaLeaderSnapshot = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-java-leader-snapshot"
	MixedComposeSkipHintGoLeaderSnapshot = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader-snapshot"

	MixedMemberCount = 3
	MixedGoMemberID  = int32(2)

	// MixedAppointedLeaderIDJavaLeader is the appointed leader id for java-leader.
	MixedAppointedLeaderIDJavaLeader = int32(0)

	// MixedAppointedLeaderIDGoLeader is the appointed leader id for go-leader.
	MixedAppointedLeaderIDGoLeader = int32(2)

	// MixedAppointedLeaderIDNone matches
	// ClusterConstants.MIXED_APPOINTED_LEADER_ID_NONE. The value equals the
	// Java appointedLeaderId context default.
	MixedAppointedLeaderIDNone = int32(-1)

	// MixedReadyPath is the readiness file the Go service healthcheck tests.
	// The path sits outside every Aeron directory. A driver does not map the
	// file. Each smoke writes the file before the waiting services start:
	//   - java-leader starts both Java members.
	//   - ex-leader-rejoin starts Java member 1.
	MixedReadyPath = "/dev/shm/heron-mixed/member-ready"

	// MixedStopPathMember1 is the stop file for Java member 1. The path sits
	// outside every Aeron directory. A driver does not map the file. The
	// ex-leader-rejoin overlay repeats this literal.
	MixedStopPathMember1 = "/dev/shm/heron-mixed/stop-member-1"

	// MixedRestartPathMember1 is the restart file for Java member 1. The path
	// sits outside every Aeron directory. A driver does not map the file. The
	// go-leader-snapshot overlay repeats this literal.
	MixedRestartPathMember1 = "/dev/shm/heron-mixed/restart-member-1"
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

	// SnapshotProbeFilename is the snapshot load file under each Java member
	// baseDir. EchoService writes the file when onStart receives a snapshot image.
	//
	// Hand-sync with ClusterConstants.SNAPSHOT_PROBE_FILENAME.
	SnapshotProbeFilename = "snapshot-probe"
)

const (
	// MixedLogChannelUnicast is the Go log channel template when a Java member is the
	// appointed leader. Topology appends the per-member log endpoint. Unicast
	// only. No interface or ttl. Same URI as cluster.DefaultLogChannel.
	MixedLogChannelUnicast = cluster.DefaultLogChannel

	// MixedLogEndpoint is the shared multicast log endpoint for the go-leader and
	// ex-leader-rejoin overlays. The final octet is odd. The media driver derives
	// the control address from the next even octet.
	//
	// Hand-sync with ClusterConstants.MIXED_LOG_ENDPOINT.
	MixedLogEndpoint = "239.255.12.15:9004"

	// MixedLogChannelMulticast is the Go log channel template when the Go member
	// leads on the go-leader and ex-leader-rejoin overlays. interface= fixes the
	// compose bridge subnet. ttl=1 keeps frames on the bridge subnet. The
	// template must not carry endpoint=. cluster.Config rejects endpoint on
	// LogChannel. Topology.LogChannel merges MixedLogEndpoint from the members
	// string. Java MIXED_LOG_CHANNEL_MULTICAST keeps endpoint= in the final URI.
	// The consensus module then clears isLogMdc. The two strings differ on
	// purpose.
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

	// MixedRejoinStartupCanvassTimeout shortens the Go member's startup canvass
	// from the 60s default. A restart keeps the value. Java rejects a Go vote
	// when the Java log is ahead.
	MixedRejoinStartupCanvassTimeout = 2 * time.Second

	// MixedJavaReelectBudget bounds the wait for the first Java term after the
	// Go member closes.
	MixedJavaReelectBudget = 30 * time.Second

	MixedRejoinBudget = 30 * time.Second

	// MixedLogRebindBudget bounds the Go module reopen. ConsensusModule.Close
	// returns before the media driver releases the log socket.
	MixedLogRebindBudget = 5 * time.Second

	// MixedTestTimeoutExLeaderRejoin bounds the ex-leader-rejoin smoke. The
	// wait stack inside the test is 270s.
	MixedTestTimeoutExLeaderRejoin = 300 * time.Second

	// MixedTestTimeoutJavaLeaderSnapshot bounds the java-leader-snapshot smoke. The
	// wait stack inside the test is 295s.
	MixedTestTimeoutJavaLeaderSnapshot = 300 * time.Second

	// MixedTestTimeoutGoLeaderSnapshot bounds the go-leader-snapshot smoke. The
	// wait stack inside the test is 260s.
	MixedTestTimeoutGoLeaderSnapshot = 300 * time.Second
)

// MixedRequiredSmoke reports which mixed proof the current overlay requires.
//
// Behaviour: reads the overlay env keys in a fixed order: java-leader,
// go-leader, ex-leader-rejoin, java-leader-snapshot, then
// go-leader-snapshot. A key set to "1" selects that overlay.
// Any other value, including unset, does not select an overlay. The caller
// does not require a proof when the keys do not select an overlay. A
// developer run outside compose leaves the keys unset. Several keys set to
// "1" is a fixture error. MixedRequiredSmoke returns the first match in that
// order. MixedRequiredSmoke does not panic.
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
	if getEnv(MixedComposeExLeaderRejoinEnvKey) == "1" {
		return MixedSmokeNameExLeaderRejoin, true
	}
	if getEnv(MixedComposeJavaLeaderSnapshotEnvKey) == "1" {
		return MixedSmokeNameJavaLeaderSnapshot, true
	}
	if getEnv(MixedComposeGoLeaderSnapshotEnvKey) == "1" {
		return MixedSmokeNameGoLeaderSnapshot, true
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
//   - sharedLogEndpoint non-empty replaces every log slot (go-leader and
//     ex-leader-rejoin multicast).
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
	channel, err := MixedComposeReplicationChannel()
	if err != nil {
		return "", err
	}
	return channel + "|" + termLength64kParam, nil
}

// MixedComposeReplicationChannel is the Go member's replication channel.
//
// Archive replay keeps the member-2 host. Replay replaces a wildcard port.
//
// Ownership: the caller owns the returned string.
//
// Returns an error when:
//   - MixedComposeHostnames fails
func MixedComposeReplicationChannel() (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	return UDPChannel + "?endpoint=" + hosts[2] + ":0", nil
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

// SnapshotProbe is one parsed EchoService snapshot-probe file.
type SnapshotProbe struct {
	LogPosition int64
}

// SnapshotProbePath returns the snapshot-probe path for memberID under MixedBaseDirPrefix.
//
// Ownership: the caller owns the returned string.
func SnapshotProbePath(memberID int) string {
	return MixedBaseDirPrefix + strconv.Itoa(memberID) + "/" + SnapshotProbeFilename
}

// ParseSnapshotProbe parses an EchoService snapshot-probe file. The file holds
// one non-blank line, logPosition=<int>.
//
// Returns an error when:
//   - logPosition is missing
//   - a key is duplicate or unknown
//   - logPosition is not an integer
//   - a line is not key=value
func ParseSnapshotProbe(data []byte) (SnapshotProbe, error) {
	var probe SnapshotProbe
	seen := false

	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return SnapshotProbe{}, fmt.Errorf("snapshot-probe line %d: want key=value, got %q", i+1, line)
		}
		if key != ServiceProbeKeyLogPosition {
			return SnapshotProbe{}, fmt.Errorf("snapshot-probe line %d: unknown key %q", i+1, key)
		}
		if seen {
			return SnapshotProbe{}, fmt.Errorf("snapshot-probe duplicate key %q", key)
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return SnapshotProbe{}, fmt.Errorf("snapshot-probe logPosition: %w", err)
		}
		probe.LogPosition = n
		seen = true
	}
	if !seen {
		return SnapshotProbe{}, fmt.Errorf("snapshot-probe missing key %q", ServiceProbeKeyLogPosition)
	}
	return probe, nil
}
