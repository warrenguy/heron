//go:build integration

package harness

import (
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
)

const (
	processMemberStatusIntervalMs = 50
	processClusterImageBound      = 30 * time.Second
	processClusterStopBound       = 30 * time.Second
)

// ProcessClusterConfig selects the members a process cluster starts.
type ProcessClusterConfig struct {
	MemberCount          int
	PortBase             int
	LogMulticastEndpoint string
}

// ProcessCluster is a set of child cluster members. Each member has its own
// media driver and Archive. Call every method from the test goroutine.
//
// Cleanup stops every live child, then closes counter mappings, then stops
// Archives and drivers. A data-race line in any child log fails the test.
type ProcessCluster struct {
	t                testing.TB
	portBase         int
	logDir           string
	ingressEndpoints string
	slots            []processSlot
}

type processSlot struct {
	spec         ProcessMemberSpec
	incarnations []*ProcessMember
	counters     *DriverCounters
	archiveID    int64
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

	hosts := slices.Repeat([]string{"127.0.0.1"}, cfg.MemberCount)
	membersString, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		PortBase:          cfg.PortBase,
		SharedLogEndpoint: cfg.LogMulticastEndpoint,
	})
	require.NoError(t, err)

	// Workers must not call t.TempDir. t.TempDir can call FailNow.
	boots := make([]memberBootstrap, cfg.MemberCount)
	for i := range boots {
		boots[i] = memberBootstrap{
			memberID:      i,
			portBase:      cfg.PortBase,
			membersString: membersString,
			clusterDir:    t.TempDir(),
			archiveDir:    t.TempDir(),
			aeronmdPath:   aeronmdPath,
			javaPath:      javaPath,
			jarPath:       jarPath,
		}
	}

	type memberInfra struct {
		md   *MediaDriver
		node *ArchiveNode
		err  error
	}
	results := make([]memberInfra, cfg.MemberCount)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			md, node, _, bootErr := bootstrapMemberInfra(boots[i])
			results[i] = memberInfra{md: md, node: node, err: bootErr}
		})
	}
	wg.Wait()

	// Register cleanups for every started resource before failing.
	var bootErrs []error
	for i, r := range results {
		if r.md != nil {
			t.Cleanup(func() { r.md.Stop() })
		}
		if r.node != nil {
			t.Cleanup(func() { r.node.Stop() })
		}
		if r.err != nil {
			bootErrs = append(bootErrs, fmt.Errorf("member %d: %w", i, r.err))
		}
	}
	require.NoError(t, errors.Join(bootErrs...))

	pc := &ProcessCluster{
		t:        t,
		portBase: cfg.PortBase,
		logDir:   t.TempDir(),
		ingressEndpoints: client.FormatIngressEndpointsFromHostnames(
			hosts,
			cfg.PortBase,
			cluster.PortsPerNode,
			cluster.ClientFacingPortOffset,
		),
		slots: make([]processSlot, cfg.MemberCount),
	}
	for i, r := range results {
		counters, openErr := OpenDriverCounters(r.md.Dir)
		require.NoError(t, openErr)
		t.Cleanup(func() { _ = counters.Close() })

		archiveID, parseErr := strconv.ParseInt(r.node.ArchiveID, 10, 64)
		require.NoError(t, parseErr)

		pc.slots[i] = processSlot{
			spec: ProcessMemberSpec{
				MemberID:         int32(i),
				MembersString:    membersString,
				ClusterDir:       boots[i].clusterDir,
				AeronDir:         r.md.Dir,
				StatusIntervalMs: processMemberStatusIntervalMs,
			},
			counters:  counters,
			archiveID: archiveID,
		}
	}

	t.Cleanup(func() {
		if stopErr := pc.StopAll(processClusterStopBound); stopErr != nil {
			t.Errorf("stop process cluster: %v", stopErr)
		}
		var races []string
		for i := range pc.slots {
			for _, inc := range pc.slots[i].incarnations {
				for _, line := range inc.RaceReports() {
					races = append(races, fmt.Sprintf("member %d: %s", i, line))
				}
			}
		}
		require.Empty(t, races)
	})

	started := make([]*ProcessMember, 0, cfg.MemberCount)
	for i := range pc.slots {
		child, startErr := pc.startChild(i)
		require.NoError(t, startErr)
		started = append(started, child)
	}
	require.NoError(t, pc.awaitLogImages(processClusterImageBound, nil, started))
	return pc
}

// startChild starts the next incarnation of index.
func (pc *ProcessCluster) startChild(index int) (*ProcessMember, error) {
	slot := &pc.slots[index]
	raw, err := json.Marshal(slot.spec)
	if err != nil {
		return nil, fmt.Errorf("process member spec: %w", err)
	}
	logPath := filepath.Join(
		pc.logDir,
		fmt.Sprintf("member-%d-inc-%d.log", index, len(slot.incarnations)),
	)
	cmd := exec.Command(os.Args[0])
	cmd.Env = mergeCmdEnv(map[string]string{ProcessMemberEnv: string(raw)})
	child, err := startProcessMember(cmd, logPath)
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

// hasLogImages reports whether every slot has a live member whose status
// lists the log session of every member.
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
// member's Archive. The second result is false when the counter is absent.
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
				pos, posOK := slot.counters.RecordingPosition(slot.archiveID, status.LocalLogRecordingID)
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
