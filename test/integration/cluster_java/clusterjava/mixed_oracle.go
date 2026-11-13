//go:build integration && cluster_java

package clusterjava

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/test/integration/harness"
)

// Mixed Go/Java multi-node goldens. Hand-sync with ClusterConstantsMixedTest.
// Java ClusterConfig helpers stay authoritative for topology strings.

const (
	// MixedComposeJavaLeaderEnvKey names the env key that the java-leader overlay sets to "1".
	MixedComposeJavaLeaderEnvKey = "LUAS_CLUSTER_MIXED_JAVA_LEADER_COMPOSE"

	// MixedComposeGoLeaderEnvKey names the env key that the go-leader overlay sets to "1".
	MixedComposeGoLeaderEnvKey = "LUAS_CLUSTER_MIXED_GO_LEADER_COMPOSE"

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

	// MixedComposeGoLeaderUnicastEnvKey names the env key that the
	// go-leader-unicast overlay sets to "1".
	MixedComposeGoLeaderUnicastEnvKey = "LUAS_CLUSTER_MIXED_GO_LEADER_UNICAST_COMPOSE"

	// MixedSmokeNameGoLeaderUnicast is the TestClusterMixedGoLeaderUnicast function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameGoLeaderUnicast = "TestClusterMixedGoLeaderUnicast"

	// MixedComposeExLeaderRejoinEnvKey names the env key that the
	// ex-leader-rejoin overlay sets to "1".
	MixedComposeExLeaderRejoinEnvKey = "LUAS_CLUSTER_MIXED_EX_LEADER_REJOIN_COMPOSE"

	// MixedSmokeNameExLeaderRejoin is the TestClusterMixedGoExLeaderRejoin function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameExLeaderRejoin = "TestClusterMixedGoExLeaderRejoin"

	// MixedComposeJavaLeaderSnapshotEnvKey names the env key that the
	// java-leader-snapshot overlay sets to "1".
	MixedComposeJavaLeaderSnapshotEnvKey = "LUAS_CLUSTER_MIXED_JAVA_LEADER_SNAPSHOT_COMPOSE"

	// MixedComposeGoLeaderSnapshotEnvKey names the env key that the
	// go-leader-snapshot overlay sets to "1".
	MixedComposeGoLeaderSnapshotEnvKey = "LUAS_CLUSTER_MIXED_GO_LEADER_SNAPSHOT_COMPOSE"

	// MixedSmokeNameJavaLeaderSnapshot is the TestClusterMixedJavaLeaderSnapshotRestart function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameJavaLeaderSnapshot = "TestClusterMixedJavaLeaderSnapshotRestart"

	// MixedSmokeNameGoLeaderSnapshot is the TestClusterMixedGoLeaderSnapshotRestart function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameGoLeaderSnapshot = "TestClusterMixedGoLeaderSnapshotRestart"

	// MixedComposeUnappointedJavaMajorityEnvKey names the env key that the
	// unappointed-java-majority overlay sets to "1".
	MixedComposeUnappointedJavaMajorityEnvKey = "LUAS_CLUSTER_MIXED_UNAPPOINTED_JAVA_MAJORITY_COMPOSE"

	// MixedComposeUnappointedGoMajorityEnvKey names the env key that the
	// unappointed-go-majority overlay sets to "1".
	MixedComposeUnappointedGoMajorityEnvKey = "LUAS_CLUSTER_MIXED_UNAPPOINTED_GO_MAJORITY_COMPOSE"

	// MixedSmokeNameUnappointedJavaMajority is the
	// TestClusterMixedUnappointedJavaMajority function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameUnappointedJavaMajority = "TestClusterMixedUnappointedJavaMajority"

	// MixedSmokeNameUnappointedGoMajority is the
	// TestClusterMixedUnappointedGoMajority function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameUnappointedGoMajority = "TestClusterMixedUnappointedGoMajority"

	// MixedComposeGoLeaderJavaClientEnvKey names the env key that the
	// go-leader-java-client overlay sets to "1".
	MixedComposeGoLeaderJavaClientEnvKey = "LUAS_CLUSTER_MIXED_GO_LEADER_JAVA_CLIENT_COMPOSE"

	// MixedSmokeNameGoLeaderJavaClient is the
	// TestClusterMixedGoLeaderJavaClient function name.
	//
	// Same multi-site literal contract as MixedSmokeNameJavaLeader.
	MixedSmokeNameGoLeaderJavaClient = "TestClusterMixedGoLeaderJavaClient"

	// Skip-hint strings name the make target for each mixed compose overlay.
	MixedComposeSkipHintJavaLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-java-leader"
	MixedComposeSkipHintGoLeader = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader"
	MixedComposeSkipHintGoLeaderUnicast = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader-unicast"
	MixedComposeSkipHintExLeaderRejoin = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-ex-leader-rejoin"
	MixedComposeSkipHintJavaLeaderSnapshot = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-java-leader-snapshot"
	MixedComposeSkipHintGoLeaderSnapshot = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader-snapshot"
	MixedComposeSkipHintUnappointedJavaMajority = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-unappointed-java-majority"
	MixedComposeSkipHintUnappointedGoMajority = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-unappointed-go-majority"
	MixedComposeSkipHintGoLeaderJavaClient = "cluster mixed " + composeSkipRunMake +
		"smoke-cluster-mixed-go-leader-java-client"

	MixedMemberCount = 3
	MixedGoMemberID  = int32(2)

	// MixedSecondGoMemberID is the extra Go member id in unappointed-go-majority.
	MixedSecondGoMemberID = int32(1)

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
	MixedReadyPath = "/dev/shm/luas-mixed/member-ready"

	// MixedStopPathMember1 is the stop file for Java member 1. The path sits
	// outside every Aeron directory. A driver does not map the file. The
	// ex-leader-rejoin overlay repeats this literal.
	MixedStopPathMember1 = "/dev/shm/luas-mixed/stop-member-1"

	// MixedRestartPathMember1 is the restart file for Java member 1. The path
	// sits outside every Aeron directory. A driver does not map the file. The
	// go-leader-snapshot overlay repeats this literal.
	MixedRestartPathMember1 = "/dev/shm/luas-mixed/restart-member-1"
)

// Default bridge address literals for the 172.30.0.0/24 topology.
// Compose assigns each address as ipv4_address on the default subnet.
//
// Compose smokes pick a per-run /24 under 172.30.0.0/16 and set
// LUAS_CLUSTER_HOSTNAMES. Runtime helpers read MixedHostnamesEnvKey.
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
	MixedHostnamesEnvKey = "LUAS_CLUSTER_HOSTNAMES"
)

const (
	// MixedIngressEndpoints is the induction client endpoint map for Java
	// members 0 and 1 on the default topology. The cluster members string still
	// carries member 2's ingress port slot. Compose smokes use
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

	// MixedAeronDirGoMember1 is Go member 1's media driver directory.
	// unappointed-go-majority runs aeronmd-1 on this path.
	MixedAeronDirGoMember1 = "/dev/shm/aeron-go1"
)

// Per-member baseDirs. Compose mounts each path as a volume root.
// Paths are baseDirs, not the cluster/ subdirectory. Go member 2 uses
// MixedClusterDirGo. unappointed-go-majority runs Go member 1 on
// MixedClusterDirMember1.
const (
	// MixedBaseDirPrefix matches ClusterConstants.MIXED_BASE_DIR_PREFIX.
	MixedBaseDirPrefix = "/tmp/luas-cluster-"

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

	// SessionProbeFilename is the session event file under each Java member
	// baseDir.
	//
	// Hand-sync with ClusterConstants.SESSION_PROBE_FILENAME.
	SessionProbeFilename = "session-probe"

	// Session-probe line kinds.
	//
	// Hand-sync with ClusterConstants.SESSION_PROBE_KIND_*.
	SessionProbeKindOpen    = "open"
	SessionProbeKindMessage = "message"
	SessionProbeKindClose   = "close"
)

const (
	// MixedLogChannelUnicast is the Go log channel template for every member
	// on the unicast log, under a Java or a Go appointed leader. Topology
	// derives each member's log endpoint from the members string.
	MixedLogChannelUnicast = cluster.DefaultLogChannel

	// MixedLogEndpoint is the shared multicast log endpoint of every multicast
	// overlay. The final octet is odd. The media driver derives the control
	// address from the next even octet.
	//
	// Hand-sync with ClusterConstants.MIXED_LOG_ENDPOINT.
	MixedLogEndpoint = "239.255.12.15:9004"

	// MixedLogChannelMulticast is the Go log channel template on
	// MixedLogEndpoint. interface= and ttl=1 keep frames on the compose bridge
	// subnet. The template must not carry endpoint=, because cluster.Config
	// rejects it. Topology.LogChannel merges MixedLogEndpoint from the members
	// string.
	//
	// Hand-sync with ClusterConstants.MIXED_LOG_CHANNEL_MULTICAST, which keeps
	// endpoint=.
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

	// MixedTestTimeoutGoLeader bounds the go-leader and go-leader-unicast
	// smoke wall clock.
	MixedTestTimeoutGoLeader = 140 * time.Second

	// MixedTestTimeoutGoLeaderJavaClient bounds the go-leader-java-client
	// smoke wall clock.
	MixedTestTimeoutGoLeaderJavaClient = 180 * time.Second

	// MixedRejoinStartupCanvassTimeout shortens the Go member's startup canvass
	// from the 60s default. A restart keeps the value. Java rejects a Go vote
	// when the Java log is ahead.
	MixedRejoinStartupCanvassTimeout = 2 * time.Second

	// MixedJavaReelectBudget bounds the wait for the first Java term after the
	// Go member closes.
	MixedJavaReelectBudget = 30 * time.Second

	MixedRejoinBudget = 30 * time.Second

	// MixedLogRebindBudget is the deadline for starting a Go module reopen attempt.
	// ConsensusModule.Close returns before the media driver releases the log
	// socket. The test retries Open until the deadline. An attempt that starts
	// before the deadline runs to completion.
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

	// MixedTestTimeoutUnappointed bounds each unappointed smoke. The wait stack
	// inside the test is 90s.
	MixedTestTimeoutUnappointed = 120 * time.Second
)

// mixedOverlays lists each compose overlay env key with the proof it selects.
var mixedOverlays = []struct {
	envKey string
	smoke  string
}{
	{MixedComposeJavaLeaderEnvKey, MixedSmokeNameJavaLeader},
	{MixedComposeGoLeaderEnvKey, MixedSmokeNameGoLeader},
	{MixedComposeGoLeaderUnicastEnvKey, MixedSmokeNameGoLeaderUnicast},
	{MixedComposeExLeaderRejoinEnvKey, MixedSmokeNameExLeaderRejoin},
	{MixedComposeJavaLeaderSnapshotEnvKey, MixedSmokeNameJavaLeaderSnapshot},
	{MixedComposeGoLeaderSnapshotEnvKey, MixedSmokeNameGoLeaderSnapshot},
	{MixedComposeUnappointedJavaMajorityEnvKey, MixedSmokeNameUnappointedJavaMajority},
	{MixedComposeUnappointedGoMajorityEnvKey, MixedSmokeNameUnappointedGoMajority},
	{MixedComposeGoLeaderJavaClientEnvKey, MixedSmokeNameGoLeaderJavaClient},
}

// MixedRequiredSmoke reports which mixed proof the current overlay requires.
//
// Behaviour: scans mixedOverlays in order. MixedRequiredSmoke does not panic.
//   - The first env key set to "1" selects that overlay.
//   - Any other value, including unset, does not select an overlay. A
//     developer run outside compose leaves every key unset.
//   - Several keys set to "1" is a fixture error. The first match in
//     mixedOverlays still names the smoke.
//
// Concurrency: pure. Reads only through getEnv.
//
// Returns:
//   - name, true: the named proof must run to completion
//   - "", false: no proof is required
func MixedRequiredSmoke(getEnv func(string) string) (string, bool) {
	for _, overlay := range mixedOverlays {
		if getEnv(overlay.envKey) == "1" {
			return overlay.smoke, true
		}
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
//   - sharedLogEndpoint empty keeps per-member unicast log endpoints (java-leader
//     and go-leader-unicast).
//   - sharedLogEndpoint non-empty replaces every log slot (multicast
//     overlays).
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

// MixedComposeClientIngressEndpoints is the client ingress endpoint map for
// members 0, 1, and 2 on the compose host list.
//
// Every member opens client ingress. Only the steady-state leader accepts
// connects.
//
// Returns an error when:
//   - MixedComposeHostnames fails
func MixedComposeClientIngressEndpoints() (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	return formatMixedIngressEndpoints(hosts), nil
}

// MixedComposeClientEgressChannel is the Java client egress channel on the
// compose member-2 host.
//
// Returns an error when:
//   - MixedComposeHostnames fails
func MixedComposeClientEgressChannel() (string, error) {
	return mixedComposeGoMemberUDPEndpoint(harness.ClientEgressPort(harness.ClusterJavaPortBase))
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
	return formatMixedIngressEndpoints(hosts[:2]), nil
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
	return mixedComposeGoMemberUDPEndpoint(0)
}

func formatMixedIngressEndpoints(hosts []string) string {
	return client.FormatIngressEndpointsFromHostnames(
		hosts,
		cluster.DefaultPortBase,
		cluster.PortsPerNode,
		cluster.ClientFacingPortOffset,
	)
}

func mixedComposeGoMemberUDPEndpoint(port int) (string, error) {
	hosts, err := MixedComposeHostnames()
	if err != nil {
		return "", err
	}
	return UDPChannel + "?endpoint=" + hosts[MixedGoMemberID] + ":" + strconv.Itoa(port), nil
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
	return mixedMemberProbePath(memberID, ServiceProbeFilename)
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
	return mixedMemberProbePath(memberID, SnapshotProbeFilename)
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

// SessionProbeEvent is one parsed EchoService session-probe line.
type SessionProbeEvent struct {
	Kind      string
	SessionID int64
	Principal []byte
	Payload   []byte
	Reason    string
}

// SessionProbePath returns the session-probe path for memberID under
// MixedBaseDirPrefix.
func SessionProbePath(memberID int) string {
	return mixedMemberProbePath(memberID, SessionProbeFilename)
}

func mixedMemberProbePath(memberID int, filename string) string {
	return MixedBaseDirPrefix + strconv.Itoa(memberID) + "/" + filename
}

// ParseSessionProbe parses EchoService session-probe lines.
//
// Behaviour: ParseSessionProbe turns each newline-terminated line into one
// SessionProbeEvent. ParseSessionProbe ignores a final line without a
// newline. That line can be a partial write. ParseSessionProbe returns a nil
// slice when data holds no complete line.
//
// Ownership: the caller owns the returned slice and its byte fields.
//
// Returns an error when:
//   - a complete line does not hold three fields
//   - the kind is not open, message, or close
//   - a field lacks its key= prefix
//   - session is not an integer
//   - principal or payload is not hex
func ParseSessionProbe(data []byte) ([]SessionProbeEvent, error) {
	parts := strings.Split(string(data), "\n")
	complete := parts[:len(parts)-1]
	if len(complete) == 0 {
		return nil, nil
	}

	events := make([]SessionProbeEvent, 0, len(complete))
	for i, line := range complete {
		event, err := parseSessionProbeLine(line)
		if err != nil {
			return nil, fmt.Errorf("session-probe line %d: %w", i+1, err)
		}
		events = append(events, event)
	}

	return events, nil
}

func parseSessionProbeLine(line string) (SessionProbeEvent, error) {
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return SessionProbeEvent{}, fmt.Errorf("want 3 fields, got %q", line)
	}

	sessionRaw, err := cutSessionProbeField(line, fields[1], "session")
	if err != nil {
		return SessionProbeEvent{}, err
	}

	sessionID, err := strconv.ParseInt(sessionRaw, 10, 64)
	if err != nil {
		return SessionProbeEvent{}, fmt.Errorf("session: %w", err)
	}

	event := SessionProbeEvent{Kind: fields[0], SessionID: sessionID}

	switch fields[0] {
	case SessionProbeKindOpen:
		event.Principal, err = cutSessionProbeHex(line, fields[2], "principal")
	case SessionProbeKindMessage:
		event.Payload, err = cutSessionProbeHex(line, fields[2], "payload")
	case SessionProbeKindClose:
		event.Reason, err = cutSessionProbeField(line, fields[2], "reason")
	default:
		return SessionProbeEvent{}, fmt.Errorf("unknown kind %q", fields[0])
	}

	if err != nil {
		return SessionProbeEvent{}, err
	}
	return event, nil
}

func cutSessionProbeField(line, field, key string) (string, error) {
	raw, ok := strings.CutPrefix(field, key+"=")
	if !ok {
		return "", fmt.Errorf("%s field, got %q", key, line)
	}
	return raw, nil
}

func cutSessionProbeHex(line, field, key string) ([]byte, error) {
	raw, err := cutSessionProbeField(line, field, key)
	if err != nil || raw == "" {
		return nil, err
	}

	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return decoded, nil
}
