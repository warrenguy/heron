//go:build integration

package harness

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
)

const (
	processMemberStatusIntervalMs = 50
	processClusterImageBound      = 30 * time.Second
	processClusterStopBound       = 30 * time.Second
)

// ProcessClusterConfig selects the members a process cluster starts.
//
// Zero values:
//   - nil Network: loopback hosts
//   - empty LogDir: a temporary directory
//   - empty Workload: the sequence service
//   - zero StatusIntervalMs: the default status interval
//
// AfterStop runs after every Archive stops and before each driver stops. The
// cluster and Archive directories still exist. AfterStop also runs when
// StartProcessCluster fails after member bootstrap begins.
type ProcessClusterConfig struct {
	MemberCount          int
	PortBase             int
	LogMulticastEndpoint string
	Network              *FaultNetwork
	LogDir               string
	StatusHandler        func(slot, incarnation int, status MemberStatus)
	Workload             string
	StatusIntervalMs     int
	AfterStop            func(*ProcessCluster)
}

// ProcessCluster is a set of child cluster members. Each member has its own
// media driver and Archive. Call every method from the test goroutine.
//
// A race line in any child log fails the test.
type ProcessCluster struct {
	t                testing.TB
	portBase         int
	logDir           string
	ingressEndpoints string
	slots            []processSlot
	statusHandler    func(slot, incarnation int, status MemberStatus)
	network          *FaultNetwork
	afterStop        func(*ProcessCluster)
	aeronmdPath      string
	javaPath         string
	jarPath          string
}

type processSlot struct {
	spec           ProcessMemberSpec
	incarnations   []*ProcessMember
	counters       *DriverCounters
	archiveID      int64
	driver         *MediaDriver
	archive        *ArchiveNode
	controlChannel string
	archiveDir     string
	archiveInc     int
}

// StartProcessCluster starts cfg.MemberCount child members.
//
// Each child runs the test binary. TestMain must call RunProcessMemberIfChild
// before m.Run.
//
// Each child waits for SIGUSR1. StartProcessCluster sends SIGUSR1 after every
// member reports every log session, its own included.
func StartProcessCluster(t testing.TB, cfg ProcessClusterConfig) *ProcessCluster {
	t.Helper()

	// A child reaches this call only when TestMain omits the hook. It would
	// start more children.
	require.Empty(t, os.Getenv(ProcessMemberEnv), "TestMain must call RunProcessMemberIfChild")

	require.Positive(t, cfg.MemberCount, "ProcessCluster: MemberCount")
	require.Positive(t, cfg.PortBase, "ProcessCluster: PortBase")
	require.NotEmpty(t, cfg.LogMulticastEndpoint, "ProcessCluster: LogMulticastEndpoint")

	aeronmdPath := resolveAeronmdOrSkip(t)
	javaPath, jarPath := resolveArchiveRuntimeOrSkip(t)

	hosts := make([]string, cfg.MemberCount)

	for i := range hosts {
		hosts[i] = "127.0.0.1"

		if cfg.Network != nil {
			hosts[i] = cfg.Network.MemberHost(i)
		}
	}

	membersString, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		PortBase:          cfg.PortBase,
		SharedLogEndpoint: cfg.LogMulticastEndpoint,
	})
	require.NoError(t, err)

	logDir := cfg.LogDir

	if logDir == "" {
		logDir = t.TempDir()
	}

	interval := cmp.Or(cfg.StatusIntervalMs, processMemberStatusIntervalMs)

	pc := &ProcessCluster{
		t:        t,
		portBase: cfg.PortBase,
		logDir:   logDir,
		ingressEndpoints: client.FormatIngressEndpointsFromHostnames(
			hosts,
			cfg.PortBase,
			cluster.PortsPerNode,
			cluster.ClientFacingPortOffset,
		),
		slots:         make([]processSlot, cfg.MemberCount),
		statusHandler: cfg.StatusHandler,
		network:       cfg.Network,
		aeronmdPath:   aeronmdPath,
		javaPath:      javaPath,
		jarPath:       jarPath,
		afterStop:     cfg.AfterStop,
	}

	// Workers must not call t.TempDir. t.TempDir can call FailNow.
	boots := make([]memberBootstrap, cfg.MemberCount)

	for i := range boots {
		slot := &pc.slots[i]
		slot.archiveDir = t.TempDir()
		slot.controlChannel = memberArchiveControl(hosts[i], i, cfg.PortBase)
		slot.spec = ProcessMemberSpec{
			MemberID:         int32(i),
			MembersString:    membersString,
			ClusterDir:       t.TempDir(),
			StatusIntervalMs: interval,
			Workload:         cfg.Workload,
		}
		boots[i] = memberBootstrap{
			memberID:      i,
			portBase:      cfg.PortBase,
			membersString: membersString,
			clusterDir:    slot.spec.ClusterDir,
			archiveDir:    slot.archiveDir,
			aeronmdPath:   aeronmdPath,
			javaPath:      javaPath,
			jarPath:       jarPath,
		}

		if cfg.Network == nil {
			continue
		}

		slot.spec.LogChannel = clusterLogChannelPrefix + "|interface=" + hosts[i] + "|ttl=1"
		slot.spec.ReplicationChannel = "aeron:udp?endpoint=" + hosts[i] + ":0"
		boots[i].startMediaDriver = func(path string, overrides DriverEnv) (*MediaDriver, error) {
			var md *MediaDriver
			startErr := cfg.Network.InMemberNamespace(i, func() error {
				var err error
				md, err = startEphemeralMediaDriver(path, overrides)
				return err
			})

			return md, startErr
		}
		boots[i].startArchiveNode = func(
			aeronDir string,
			archiveDir string,
			_ string,
			java string,
			jar string,
		) (*ArchiveNode, error) {
			return startArchiveNodeLogged(
				aeronDir,
				archiveDir,
				slot.controlChannel,
				java,
				jar,
				archiveIncLog(logDir, i, 0),
				archiveReadyTimeout,
			)
		}
	}

	// Register after the t.TempDir calls and before bootstrap. A failed start
	// must run AfterStop while the cluster and Archive directories exist.
	t.Cleanup(pc.cleanup)

	bootErrs := make([]error, cfg.MemberCount)
	var wg sync.WaitGroup

	for i := range pc.slots {
		wg.Go(func() {
			slot := &pc.slots[i]
			var bootErr error
			slot.driver, slot.archive, _, bootErr = bootstrapMemberInfra(boots[i])
			if bootErr != nil {
				bootErrs[i] = fmt.Errorf("member %d: %w", i, bootErr)
			}
		})
	}

	wg.Wait()
	require.NoError(t, errors.Join(bootErrs...))

	for i := range pc.slots {
		slot := &pc.slots[i]

		slot.counters, err = OpenDriverCounters(slot.driver.Dir)
		require.NoError(t, err, "member %d", i)

		slot.archiveID, err = strconv.ParseInt(slot.archive.ArchiveID, 10, 64)
		require.NoError(t, err, "member %d", i)

		slot.spec.AeronDir = slot.driver.Dir
	}

	started := make([]*ProcessMember, 0, cfg.MemberCount)

	for i := range pc.slots {
		child, startErr := pc.startChild(i)
		require.NoError(t, startErr)
		started = append(started, child)
	}

	require.NoError(t, pc.awaitLogImages(processClusterImageBound, nil, started))
	return pc
}

func memberArchiveControl(host string, memberID, portBase int) string {
	port := cluster.CalculatePort(
		int32(memberID),
		portBase,
		cluster.ArchiveControlPortOffset,
	)

	return "aeron:udp?endpoint=" + net.JoinHostPort(host, strconv.Itoa(port))
}

func archiveIncLog(dir string, member, inc int) string {
	return filepath.Join(dir, fmt.Sprintf("archive-%d-inc-%d.log", member, inc))
}

func (pc *ProcessCluster) cleanup() {
	t := pc.t

	if stopErr := pc.StopAll(processClusterStopBound); stopErr != nil {
		t.Errorf("stop process cluster: %v", stopErr)
	}

	for i := range pc.slots {
		pc.slots[i].archive.Stop()
	}

	if pc.afterStop != nil {
		pc.afterStop(pc)
	}

	for i := range pc.slots {
		_ = pc.slots[i].counters.Close()
		pc.slots[i].driver.Stop()
	}

	for i := range pc.slots {
		for _, inc := range pc.slots[i].incarnations {
			for _, line := range inc.RaceReports() {
				t.Errorf("member %d: %s", i, line)
			}
		}
	}
}

// ClusterDir returns the cluster directory of index.
//
// It returns an empty string when index is out of range.
func (pc *ProcessCluster) ClusterDir(index int) string {
	slot := pc.slotAt(index)

	if slot == nil {
		return ""
	}

	return slot.spec.ClusterDir
}

// ArchiveDir returns the Archive directory of index.
//
// It returns an empty string when index is out of range.
func (pc *ProcessCluster) ArchiveDir(index int) string {
	slot := pc.slotAt(index)

	if slot == nil {
		return ""
	}

	return slot.archiveDir
}

// DriverDir returns the media driver directory of index.
//
// It returns an empty string when index is out of range or the driver did not
// start.
func (pc *ProcessCluster) DriverDir(index int) string {
	slot := pc.slotAt(index)

	if slot == nil || slot.driver == nil {
		return ""
	}

	return slot.driver.Dir
}

func (pc *ProcessCluster) startChild(index int) (*ProcessMember, error) {
	slot := &pc.slots[index]
	raw, err := json.Marshal(slot.spec)
	if err != nil {
		return nil, fmt.Errorf("process member spec: %w", err)
	}

	incarnation := len(slot.incarnations)
	logPath := filepath.Join(
		pc.logDir,
		fmt.Sprintf("member-%d-inc-%d.log", index, incarnation),
	)
	cmd := exec.Command(os.Args[0])
	cmd.Env = mergeCmdEnv(map[string]string{ProcessMemberEnv: string(raw)})

	var handler func(MemberStatus)

	if pc.statusHandler != nil {
		handler = func(status MemberStatus) {
			pc.statusHandler(index, incarnation, status)
		}
	}

	child, err := startProcessMember(cmd, logPath, handler)
	if err != nil {
		return nil, err
	}

	slot.incarnations = append(slot.incarnations, child)
	return child, nil
}

// awaitLogImages waits until hasLogImages reports true. It then calls
// SignalStart on each member in started.
func (pc *ProcessCluster) awaitLogImages(
	bound time.Duration,
	pump func(),
	started []*ProcessMember,
) error {
	begin := time.Now()
	ok := pc.pollPumped(bound, pump, pc.hasLogImages)
	elapsed := time.Since(begin)
	if !ok {
		return fmt.Errorf(
			"process cluster: log image wait elapsed=%s\n%s",
			elapsed,
			pc.State(),
		)
	}
	pc.t.Logf("log image wait elapsed=%s\n%s", elapsed, pc.State())

	var errs []error
	for _, member := range started {
		if err := member.SignalStart(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (pc *ProcessCluster) pollPumped(bound time.Duration, pump func(), check func() bool) bool {
	return PollUntilBool(bound, time.Millisecond, func() bool {
		if pump != nil {
			pump()
		}
		return check()
	})
}

// hasLogImages reports whether every slot has a live member with a bound log
// publication. It also requires each status to list every member's log session.
func (pc *ProcessCluster) hasLogImages() bool {
	statuses := make([]MemberStatus, 0, len(pc.slots))
	for i := range pc.slots {
		member := pc.slots[i].current()
		if !member.isLive() {
			return false
		}
		status, ok := member.Status()
		if !ok {
			return false
		}

		if !status.LogPublicationBound {
			return false
		}
		statuses = append(statuses, status)
	}
	for _, status := range statuses {
		for _, other := range statuses {
			if !slices.Contains(status.LogImageSessions, other.LogSessionID) {
				return false
			}
		}
	}
	return true
}

func (s *processSlot) current() *ProcessMember {
	if len(s.incarnations) == 0 {
		return nil
	}
	return s.incarnations[len(s.incarnations)-1]
}

func (pc *ProcessCluster) slotAt(index int) *processSlot {
	if index < 0 || index >= len(pc.slots) {
		return nil
	}
	return &pc.slots[index]
}

// Member returns the current incarnation of index, or nil when index is out of range.
func (pc *ProcessCluster) Member(index int) *ProcessMember {
	slot := pc.slotAt(index)
	if slot == nil {
		return nil
	}
	return slot.current()
}

// Incarnations returns every incarnation of index, oldest first.
func (pc *ProcessCluster) Incarnations(index int) []*ProcessMember {
	slot := pc.slotAt(index)
	if slot == nil {
		return nil
	}
	return slices.Clone(slot.incarnations)
}

// Kill sends SIGKILL to the current incarnation of index.
func (pc *ProcessCluster) Kill(index int) error {
	member := pc.Member(index)
	if member == nil {
		return fmt.Errorf("process cluster: member %d is absent", index)
	}
	return member.Kill()
}

// StopAll sends SIGTERM to every live child, then waits for each one.
//
// Every wait shares one deadline. The returned error joins every failed signal
// and wait.
func (pc *ProcessCluster) StopAll(bound time.Duration) error {
	deadline := time.Now().Add(bound)
	var errs []error
	var live []int
	for i := range pc.slots {
		member := pc.slots[i].current()
		if member == nil || !member.isLive() {
			continue
		}

		if err := member.SignalStop(); err != nil {
			errs = append(errs, fmt.Errorf("member %d: %w", i, err))
		}
		live = append(live, i)
	}
	for _, i := range live {
		remain := max(time.Until(deadline), 0)
		if err := pc.slots[i].current().AwaitStop(remain); err != nil {
			errs = append(errs, fmt.Errorf("member %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Restart starts a new incarnation of index on the same cluster directory,
// driver, and Archive. It returns an error when the current incarnation is
// still running.
//
// Restart sends SIGUSR1 to the new child after the same log-image wait as
// StartProcessCluster. A non-nil pump runs before each log-image check.
func (pc *ProcessCluster) Restart(index int, pump func()) error {
	slot := pc.slotAt(index)
	if slot == nil {
		return fmt.Errorf("process cluster: member %d is absent", index)
	}
	if slot.current().isLive() {
		return fmt.Errorf("process cluster: member %d is still running", index)
	}

	child, err := pc.startChild(index)
	if err != nil {
		return err
	}
	return pc.awaitLogImages(processClusterImageBound, pump, []*ProcessMember{child})
}

// Statuses returns the latest status of every current incarnation that has one.
// The slice is in member order.
func (pc *ProcessCluster) Statuses() []MemberStatus {
	out := make([]MemberStatus, 0, len(pc.slots))
	for i := range pc.slots {
		status, ok := pc.slots[i].current().Status()
		if !ok {
			continue
		}
		out = append(out, status)
	}
	return out
}

// AwaitStatuses polls at 1ms until pred reports true or bound passes.
//
// A non-nil pump runs before each predicate check.
func (pc *ProcessCluster) AwaitStatuses(
	bound time.Duration,
	pump func(),
	pred func([]MemberStatus) bool,
) bool {
	return pc.pollPumped(bound, pump, func() bool { return pred(pc.Statuses()) })
}

// IngressEndpoints returns the client ingress list built at start.
func (pc *ProcessCluster) IngressEndpoints() string {
	return pc.ingressEndpoints
}

// RecordingPosition reads the RecordingPos counter for recordingID on the
// member's Archive. The second result is false when index is out of range or
// the counter is absent.
func (pc *ProcessCluster) RecordingPosition(index int, recordingID int64) (int64, bool) {
	slot := pc.slotAt(index)

	if slot == nil {
		return 0, false
	}

	return slot.counters.RecordingPosition(slot.archiveID, recordingID)
}

// HasClient reports whether the member's driver still holds clientID.
func (pc *ProcessCluster) HasClient(index int, clientID int64) bool {
	slot := pc.slotAt(index)
	if slot == nil {
		return false
	}
	return slot.counters.HasClient(clientID)
}

// State returns one diagnostic block per incarnation. Each block ends with the
// last 20 log lines.
func (pc *ProcessCluster) State() string {
	var b strings.Builder
	for i := range pc.slots {
		slot := &pc.slots[i]
		for n, inc := range slot.incarnations {
			fmt.Fprintf(
				&b,
				"slot %d incarnation %d pid %d %s applied %d\n",
				i,
				n,
				inc.cmd.Process.Pid,
				processStateText(inc.ProcessState()),
				len(inc.Applied()),
			)
			if ready, ok := inc.Ready(); ok {
				fmt.Fprintf(&b, "ready %+v hasClient %t\n", ready, slot.counters.HasClient(ready.ClientID))
			}
			if status, ok := inc.Status(); ok {
				pos, posOK := pc.RecordingPosition(i, status.LocalLogRecordingID)
				fmt.Fprintf(&b, "status %+v recording %d ok %t\n", status, pos, posOK)
			}
			if exitRec, ok := inc.ExitRecord(); ok {
				fmt.Fprintf(&b, "exit %q\n", exitRec.Error)
			}
			fmt.Fprintf(&b, "log %s\n%s\n", inc.LogPath(), lastLogLines(inc.LogPath(), 20))
		}
	}
	return b.String()
}

func processStateText(state *os.ProcessState) string {
	if state == nil {
		return "running"
	}
	return state.String()
}

func lastLogLines(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("log read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
