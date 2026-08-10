//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// defaultClusterMemberCloseLinger is the Client.Close linger cap per member at
// teardown.
//
// The aeron default is three seconds. Nothing polls after the duty cycle stops,
// so the fixture must not wait that long per member.
const defaultClusterMemberCloseLinger = 250 * time.Millisecond

// Integration election heartbeats for live harness opens. Production defaults
// are a 10s leader heartbeat timeout and a 200ms interval. Re-elect waits use
// LeaderHeartbeatTimeoutNs as a lower bound, so the production timeout
// dominates suite wall time. These shorter values are for harness opens only.
// They are not production defaults.
//
// The timeout carries margin over duty-cycle stalls under race-detector load.
// A 1s timeout fired on such stalls and re-entered elections mid-test.
// Re-entry across a term boundary cannot recover while the log replication
// factory defaults to nil, so spurious re-entries must stay rare. 5s at the
// 100ms interval matches the production 50-interval miss tolerance.
const (
	IntegrationLeaderHeartbeatTimeoutNs  int64 = 5_000_000_000 // 5s
	IntegrationLeaderHeartbeatIntervalNs int64 = 100_000_000   // 100ms
)

// IntegrationElectionConfig returns election timeouts for harness member opens.
//
// Behaviour: sets only LeaderHeartbeatTimeoutNs and LeaderHeartbeatIntervalNs.
// Zero timeout fields still take election.DefaultElectionConfig via
// cluster.Config.Normalize.
func IntegrationElectionConfig() election.ElectionConfig {
	return election.ElectionConfig{
		LeaderHeartbeatTimeoutNs:  IntegrationLeaderHeartbeatTimeoutNs,
		LeaderHeartbeatIntervalNs: IntegrationLeaderHeartbeatIntervalNs,
	}
}

// ClusterMemberSet is a set of in-process consensus members. One media driver,
// one Archive server, and one ConsensusModule per member. All members share
// one cluster members string.
//
// The fixture co-locates one Archive server per member. Cluster topology does
// the same. No member shares an Archive with another member.
//
// Concurrency: invoker-only. Every method runs on the test goroutine.
//
// Lifetime:
//   - StartClusterMemberSet registers driver Stop and Archive Stop on the
//     test goroutine after parallel bootstrap. It registers each slot close
//     after OpenConsensusModule. One-off harness.Start and StartArchiveNode
//     paths still register Cleanup inside those helpers.
//   - LIFO teardown per member is the slot close, then Archive Stop, then
//     driver Stop.
//   - Across the set, later slots close before earlier slots. After every
//     slot close, Cleanup LIFO stops later Archives and drivers before
//     earlier ones.
//   - Close is public and idempotent. Callers may call Close early.
//   - Invoker must not call Tick, PumpTransportUntil, TickUntil,
//     AwaitLogImages, AwaitElection, or LogSessionIDs after Close. Close
//     clears the transport client, archive control client, and LogStreams.
//   - State stays readable after Close for failure dumps. Nil handles
//     report as sentinel values.
//   - modules, nodes, and configs share length MemberCount. The index is the
//     member id. StopMember and RestartMember preserve those lengths.
type ClusterMemberSet struct {
	modules       []*cluster.ConsensusModule
	nodes         []*ArchiveNode
	configs       []cluster.Config
	membersString string
}

// ClusterMemberSetConfig holds the port block and multicast log endpoint the
// caller owns. PortBase and LogMulticastEndpoint must be unique per test.
// See cluster_log_endpoints.go for the block each constant reserves.
type ClusterMemberSetConfig struct {
	MemberCount int

	// PortBase is the first port of member zero's block. Must be positive.
	//
	// The set reserves PortBase+1 .. PortBase+(MemberCount-1)*100+5.
	// Wire formula: portBase+memberID*100+offset for offsets 1 through 5 and
	// member ids 0 .. MemberCount-1.
	PortBase int

	// LogMulticastEndpoint is the shared cluster log group and port, host:port,
	// with an odd final octet in the group address. Must be non-empty.
	LogMulticastEndpoint string

	// StartMediaDriver overrides ephemeral media driver start. Nil means
	// startEphemeralMediaDriver.
	//
	// When non-nil, StartClusterMemberSet skips resolveAeronmdOrSkip and passes a
	// placeholder aeronmd path into the starter.
	StartMediaDriver func(aeronmdPath string, overrides DriverEnv) (*MediaDriver, error)

	// StartArchiveNode overrides Archive process start. Nil means
	// startArchiveNodeProcess.
	//
	// When non-nil, StartClusterMemberSet skips resolveArchiveRuntimeOrSkip and
	// passes placeholder java and jar paths into the starter.
	StartArchiveNode func(
		aeronDir string,
		archiveDir string,
		controlChannel string,
		javaPath string,
		jarPath string,
	) (*ArchiveNode, error)

	// AmendMemberConfig adjusts one member's cluster.Config before open. Nil
	// means no amendment.
	//
	// Runs once per member on the test goroutine after bootstrap and before
	// open. The amended config is stored per slot. RestartMember reuses it.
	// The hook must not clear fields MemberClusterConfig owns.
	AmendMemberConfig func(cfg cluster.Config) cluster.Config
}

// StartClusterMemberSet starts MemberCount media drivers, one Archive server
// per driver, and one ConsensusModule per driver against a shared multicast
// cluster log.
//
// Open order uses two steps so every Archive is ready before any member opens:
//  1. Per member in parallel: start media driver and Archive server; store
//     cluster.Config. After the wait, StartClusterMemberSet registers driver
//     and Archive Cleanup on the test goroutine (driver then Archive per
//     member index, low to high) so LIFO matches sequential registration.
//  2. Per member sequential: successful OpenConsensusModule registers the
//     slot close, which closes whatever module occupies the slot.
//
// Per member LIFO is the slot close, then Archive Stop, then driver Stop.
// Across a three-member set, Cleanup LIFO is:
//
//	slot 2, slot 1, slot 0, archive 2, driver 2, archive 1, driver 1,
//	archive 0, driver 0
//
// Every slot closes before any Archive stops.
//
// A mid-bootstrap failure keeps Cleanup for every resource already started.
//
// Worker goroutines must not call require or FailNow on t. Errors collect.
// The test goroutine fails after cleanups register.
func StartClusterMemberSet(t testing.TB, cfg ClusterMemberSetConfig) *ClusterMemberSet {
	t.Helper()

	if cfg.MemberCount < 1 {
		require.FailNow(t, "ClusterMemberSet: MemberCount must be >= 1")
	}
	if cfg.PortBase <= 0 {
		require.FailNow(t, "ClusterMemberSet: PortBase must be positive")
	}
	if cfg.LogMulticastEndpoint == "" {
		require.FailNow(t, "ClusterMemberSet: LogMulticastEndpoint must be non-empty")
	}

	// CaptureFailTB does not stop the goroutine. Return before any driver starts.
	if failedCaptureTB(t) {
		return nil
	}

	hosts := make([]string, cfg.MemberCount)
	for i := range hosts {
		hosts[i] = "127.0.0.1"
	}

	membersString, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		PortBase:          cfg.PortBase,
		SharedLogEndpoint: cfg.LogMulticastEndpoint,
	})
	require.NoError(t, err)
	if failedCaptureTB(t) {
		return nil
	}

	// Resolve real aeronmd and Archive runtimes only when the matching starter is
	// nil. A non-nil starter replaces process start. Unit tests then need no host
	// Archive runtime or aeronmd.
	aeronmdPath := "aeronmd"
	if cfg.StartMediaDriver == nil {
		aeronmdPath = resolveAeronmdOrSkip(t)
	}
	javaPath, jarPath := "java", "jar"
	if cfg.StartArchiveNode == nil {
		javaPath, jarPath = resolveArchiveRuntimeOrSkip(t)
	}
	if failedCaptureTB(t) || skippedCaptureTB(t) {
		return nil
	}

	// Temp dirs on the test goroutine. Workers never call t.TempDir.
	clusterDirs := make([]string, cfg.MemberCount)
	archiveDirs := make([]string, cfg.MemberCount)
	for i := range cfg.MemberCount {
		clusterDirs[i] = t.TempDir()
		archiveDirs[i] = t.TempDir()
	}

	type memberInfra struct {
		md   *MediaDriver
		node *ArchiveNode
		cfg  cluster.Config
		err  error
	}
	results := make([]memberInfra, cfg.MemberCount)

	var wg sync.WaitGroup
	for i := range cfg.MemberCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			md, node, memberCfg, bootErr := bootstrapMemberInfra(memberBootstrap{
				memberID:         i,
				portBase:         cfg.PortBase,
				membersString:    membersString,
				clusterDir:       clusterDirs[i],
				archiveDir:       archiveDirs[i],
				aeronmdPath:      aeronmdPath,
				javaPath:         javaPath,
				jarPath:          jarPath,
				startMediaDriver: cfg.StartMediaDriver,
				startArchiveNode: cfg.StartArchiveNode,
			})
			results[i] = memberInfra{md: md, node: node, cfg: memberCfg, err: bootErr}
		}(i)
	}
	wg.Wait()

	// Register cleanups for every started resource before failing. Order matches
	// sequential loop 1: driver then Archive per member index, low to high.
	set := &ClusterMemberSet{
		membersString: membersString,
		nodes:         make([]*ArchiveNode, cfg.MemberCount),
		configs:       make([]cluster.Config, cfg.MemberCount),
		modules:       make([]*cluster.ConsensusModule, 0, cfg.MemberCount),
	}
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
			continue
		}
		memberCfg := r.cfg
		if cfg.AmendMemberConfig != nil {
			memberCfg = cfg.AmendMemberConfig(memberCfg)
		}
		set.nodes[i] = r.node
		set.configs[i] = memberCfg
	}
	require.NoError(t, errors.Join(bootErrs...))
	if failedCaptureTB(t) {
		return nil
	}

	// Loop 2: open members after every Archive is ready. Sequential so open
	// order and slot Cleanup registration stay deterministic.
	for i := range cfg.MemberCount {
		module, openErr := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
			Config: set.configs[i],
		})
		require.NoError(t, openErr)
		require.NotNil(t, module)
		set.modules = append(set.modules, module)

		// Register the slot close after driver and Archive cleanups so LIFO
		// closes the module before Archive Stop and driver Stop. StopMember
		// is idempotent on an empty slot, so a test that already stopped or
		// restarted a member closes only the module that occupies the slot.
		CleanupClose(t, fmt.Sprintf("cluster member %d", i), func() error {
			return set.StopMember(i)
		})
	}
	return set
}

// memberBootstrap carries one member's bootstrap inputs across the worker
// goroutine boundary. Every field is a value copy the worker owns.
type memberBootstrap struct {
	memberID      int
	portBase      int
	membersString string
	clusterDir    string
	archiveDir    string
	aeronmdPath   string
	javaPath      string
	jarPath       string

	// Non-nil startMediaDriver and startArchiveNode replace the real process
	// start helpers. Nil means the real helpers. Same contract as
	// ClusterMemberSetConfig.StartMediaDriver and StartArchiveNode.
	startMediaDriver func(aeronmdPath string, overrides DriverEnv) (*MediaDriver, error)
	startArchiveNode func(
		aeronDir string,
		archiveDir string,
		controlChannel string,
		javaPath string,
		jarPath string,
	) (*ArchiveNode, error)
}

// bootstrapMemberInfra starts one media driver and one Archive for the member.
//
// Concurrency: safe from a worker goroutine. Does not call testing.TB.
//
// Returns: on error both handles are nil. A failed Archive start stops the
// driver before return.
func bootstrapMemberInfra(
	spec memberBootstrap,
) (*MediaDriver, *ArchiveNode, cluster.Config, error) {
	startMD := startEphemeralMediaDriver
	if spec.startMediaDriver != nil {
		startMD = spec.startMediaDriver
	}
	md, err := startMD(spec.aeronmdPath, nil)
	if err != nil {
		return nil, nil, cluster.Config{}, err
	}

	controlPort := cluster.CalculatePort(
		int32(spec.memberID),
		spec.portBase,
		cluster.ArchiveControlPortOffset,
	)
	startArchive := startArchiveNodeProcess
	if spec.startArchiveNode != nil {
		startArchive = spec.startArchiveNode
	}
	node, err := startArchive(
		md.Dir,
		spec.archiveDir,
		ArchiveControlChannel(controlPort),
		spec.javaPath,
		spec.jarPath,
	)
	if err != nil {
		md.Stop()
		return nil, nil, cluster.Config{}, err
	}

	cfg := MemberClusterConfig(
		int32(spec.memberID),
		spec.membersString,
		spec.clusterDir,
		md.Dir,
	)
	return md, node, cfg, nil
}

// MemberClusterConfig builds the cluster.Config for one harness member open.
//
// Behaviour: sets Election to IntegrationElectionConfig and caps close linger
// below the 3s client default. Other zero fields take cluster.Config.Normalize
// defaults at open.
func MemberClusterConfig(
	memberID int32,
	membersString string,
	clusterDir string,
	aeronDir string,
) cluster.Config {
	return cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                membersString,
		ClusterDir:                    clusterDir,
		AeronDir:                      aeronDir,
		LogChannel:                    ClusterLogChannelTemplate,
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
		Election:                      IntegrationElectionConfig(),
		AeronConfig: aeron.Config{
			InterServiceTimeout: ConnectedPollTimeout,
			CloseLingerDuration: defaultClusterMemberCloseLinger,
		},
	}
}

// Modules returns every consensus module in member-id order.
//
// Ownership: the set owns the slice. Caller must not mutate the returned slice
// (append, reorder, or replace elements). Empty slots are nil. Length stays
// MemberCount across StopMember and RestartMember.
func (s *ClusterMemberSet) Modules() []*cluster.ConsensusModule {
	return s.modules
}

// Module returns the consensus module in slot index, or nil when the slot is
// empty or index is out of range.
func (s *ClusterMemberSet) Module(index int) *cluster.ConsensusModule {
	if index < 0 || index >= len(s.modules) {
		return nil
	}
	return s.modules[index]
}

// ClusterDir returns the cluster directory stored for slot index.
// Empty when index is out of range.
func (s *ClusterMemberSet) ClusterDir(index int) string {
	if index < 0 || index >= len(s.configs) {
		return ""
	}
	return s.configs[index].ClusterDir
}

// ArchiveNodes returns every Archive server in member-id order.
//
// Ownership: the set owns the slice. Caller must not mutate the returned slice
// or stop the nodes. Process lifetime is StartClusterMemberSet Cleanup for
// Archive Stop. Close does not stop Archives. A member restart leaves the
// Archive node in place.
func (s *ClusterMemberSet) ArchiveNodes() []*ArchiveNode {
	return s.nodes
}

// MembersString returns the shared cluster members string.
func (s *ClusterMemberSet) MembersString() string {
	return s.membersString
}

// live returns every non-empty module in member-id order. Set iterations that
// must skip stopped slots range over live. The skip rule lives in one place.
//
// State and Close walk all slots on purpose:
//   - State renders nil slots
//   - Close uses the module's nil-safe Close in LIFO order
func (s *ClusterMemberSet) live() iter.Seq[*cluster.ConsensusModule] {
	return func(yield func(*cluster.ConsensusModule) bool) {
		for _, m := range s.modules {
			if m == nil {
				continue
			}
			if !yield(m) {
				return
			}
		}
	}
}

// StopMember closes the module in slot index and leaves the slot empty.
//
// Behaviour:
//   - Nils the slot then closes the module. Module Close releases the cluster
//     directory lock last.
//   - Leaves the media driver and the Archive node running so RestartMember can
//     reuse both. Does not call ArchiveNode.Stop. Does not call MediaDriver.Stop.
//   - Does not wait for cluster.lock. A release can trail Close.
//     [cluster.OpenRecovery] waits [cluster.DirectoryLockWait] on RestartMember.
//   - A durable hold fails at RestartMember with the StopMember release note on
//     EWOULDBLOCK.
//
// Concurrency: invoker-only. Must not run concurrently with Tick.
//
// Lifetime: the cluster directory and the Archive data directory survive.
//
// Idempotent: a second call on an empty slot returns nil.
//
// Returns an error when:
//   - index is out of range
//   - the module's Close reports one
func (s *ClusterMemberSet) StopMember(index int) error {
	if index < 0 || index >= len(s.modules) {
		return fmt.Errorf("cluster member set: index %d out of range [0, %d)", index, len(s.modules))
	}
	m := s.modules[index]
	if m == nil {
		return nil
	}
	s.modules[index] = nil
	return m.Close()
}

// RestartMember opens a replacement module in slot index against the
// cluster.Config the first open used.
//
// Behaviour:
//   - Opens one cluster.ConsensusModule against the retained config. The cluster
//     directory, media-driver directory, and Archive control channels are the
//     previous boot's. The recovery plan is non-empty when the previous boot
//     wrote term entries.
//   - Invoker must call StopMember for the same index first.
//   - On an occupied slot returns an error without opening a second module. No
//     transport connect or archive session runs for that refused open.
//
// Concurrency: invoker-only. Must not run concurrently with Tick.
//
// Every open failure is wrapped with slot and ClusterDir. When errno is
// EWOULDBLOCK the wrap notes that StopMember must have released cluster.lock.
//
// Must not retry the open. Must not sleep. OpenRecovery already waits
// cluster.DirectoryLockWait for a release that trails Close. An EWOULDBLOCK
// that reaches here outlived that wait.
//
// On error the empty slot stays empty and an occupied slot is left unchanged.
//
// Returns an error when:
//   - index is out of range
//   - the slot is occupied
//   - the open fails
func (s *ClusterMemberSet) RestartMember(index int) error {
	if index < 0 || index >= len(s.modules) {
		return fmt.Errorf("cluster member set: index %d out of range [0, %d)", index, len(s.modules))
	}
	if s.modules[index] != nil {
		return fmt.Errorf(
			"cluster member set: slot %d holds an open module; call StopMember first",
			index,
		)
	}
	cfg := s.configs[index]
	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cfg,
	})
	if err != nil {
		return wrapRestartOpenError(index, cfg.ClusterDir, err)
	}
	s.modules[index] = module
	return nil
}

// wrapRestartOpenError phrases a RestartMember open failure with slot and
// ClusterDir. Appends the StopMember release note only when errno is
// EWOULDBLOCK. Preserves errors.Is via %w.
func wrapRestartOpenError(index int, dir string, err error) error {
	if err == nil {
		return nil
	}
	hint := ""
	if errors.Is(err, unix.EWOULDBLOCK) {
		hint = " (StopMember must have released cluster.lock)"
	}
	return fmt.Errorf(
		"cluster member set: restart slot %d ClusterDir %s: %w%s",
		index,
		dir,
		err,
		hint,
	)
}

// LogSessionIDs returns each non-empty member's log publication session id in
// member-id order. Empty slots are skipped, so the result may be shorter than
// MemberCount.
//
// Invoker must not call LogSessionIDs after Close. Close clears LogStreams.
func (s *ClusterMemberSet) LogSessionIDs() []int32 {
	sessions := make([]int32, 0, len(s.modules))
	for m := range s.live() {
		sessions = append(sessions, m.LogStreams().Publication().SessionID())
	}
	return sessions
}

// Leaders returns every non-empty member reporting IsLeader, in member-id order.
//
// A settled election returns exactly one element. Empty slots are skipped.
func (s *ClusterMemberSet) Leaders() []*cluster.ConsensusModule {
	var leaders []*cluster.ConsensusModule
	for m := range s.live() {
		if m.IsLeader() {
			leaders = append(leaders, m)
		}
	}
	return leaders
}

// Tick runs one duty cycle over every non-empty member. First every transport
// client and that member's archive control poll. Then every agent.
//
// Two loops, not one interleaved loop. A member's consensus publication and a
// peer's consensus subscription live on different drivers. A frame written by
// one agent is visible only after the peer's client has pumped.
//
// The archive poll sits in the transport loop. Archive input is that member's
// own conductor output over local IPC. The archive poll has no cross-member
// dependency.
//
// Empty slots are skipped so a stopped member does not panic the duty cycle.
//
// Invoker must not call Tick after Close.
func (s *ClusterMemberSet) Tick() {
	for m := range s.live() {
		m.Client().DoWork()
		m.ArchiveClient().Poll()
	}
	for m := range s.live() {
		m.Agent().DoWork(m.NanoClock()())
	}
}

// PumpTransportUntil pumps every non-empty transport client until ok reports
// true or the deadline bound elapses. Does not call any agent. Does not poll
// archive control. Use Tick or TickUntil when control demux is required.
//
// Empty slots are skipped.
//
// Returns:
//   - true when ok reports true before the bound
//   - false on timeout
//
// Sleep is 1ms between pumps. That sleep is not part of Tick.
//
// Invoker must not call PumpTransportUntil after Close.
func (s *ClusterMemberSet) PumpTransportUntil(bound time.Duration, ok func() bool) bool {
	return PollUntilBool(bound, time.Millisecond, func() bool {
		for m := range s.live() {
			m.Client().DoWork()
		}
		return ok()
	})
}

// TickUntil runs the full duty cycle until ok reports true or the deadline
// bound elapses.
//
// Returns:
//   - true when ok reports true before the bound
//   - false on timeout
//
// Sleep is 1ms between duty cycles. That sleep is not part of Tick.
//
// Invoker must not call TickUntil after Close.
func (s *ClusterMemberSet) TickUntil(bound time.Duration, ok func() bool) bool {
	return PollUntilBool(bound, time.Millisecond, func() bool {
		s.Tick()
		return ok()
	})
}

// AwaitLogImages pumps transport until every non-empty member's log
// subscription holds an image for every non-empty member's log publication
// session. Does not call any agent.
//
// Walks images by session id, not bare ImageCount. ImageCount can pass on
// foreign images while a peer session is still missing. Empty slots are
// skipped; a stopped member has no publication session for peers to image.
//
// Returns:
//   - true when every image is present before the bound
//   - false on timeout
//
// Invoker must not call AwaitLogImages after Close.
func (s *ClusterMemberSet) AwaitLogImages(bound time.Duration) bool {
	sessions := s.LogSessionIDs()
	return s.PumpTransportUntil(bound, func() bool {
		for m := range s.live() {
			sub := m.LogStreams().Subscription()
			for _, session := range sessions {
				if _, ok := ImageBySessionID(sub, session); !ok {
					return false
				}
			}
		}
		return true
	})
}

// AwaitElection runs the full duty cycle until every non-empty member reports
// ElectionComplete. Empty slots are skipped.
//
// Returns:
//   - true when every non-empty member completes before the bound
//   - false on timeout
//
// Invoker must not call AwaitElection after Close.
func (s *ClusterMemberSet) AwaitElection(bound time.Duration) bool {
	return s.TickUntil(bound, func() bool {
		for m := range s.live() {
			if !m.ElectionComplete() {
				return false
			}
		}
		return true
	})
}

// State returns a one-line-per-member dump for failure messages.
//
// Safe after Close. Closed handles report as sentinel values.
func (s *ClusterMemberSet) State() string {
	var b strings.Builder
	for _, m := range s.modules {
		b.WriteString(ClusterMemberState(m))
	}
	return b.String()
}

// ClusterMemberState returns a one-line dump of one consensus module for
// failure messages.
//
// Behaviour: safe after Close. A nil module reports a sentinel line. A missing
// agent leaves agent-sourced fields at dump sentinels (-1, false, or empty
// string).
func ClusterMemberState(m *cluster.ConsensusModule) string {
	if m == nil {
		return "member=<nil>\n"
	}

	images := -1
	logPubPos := int64(-1)
	logPubSession := int32(-1)
	if streams := m.LogStreams(); streams != nil {
		if sub := streams.Subscription(); sub != nil {
			images = sub.ImageCount()
		}
		if pub := streams.Publication(); pub != nil {
			logPubPos = pub.Position()
			logPubSession = pub.SessionID()
		}
	}

	adapterPos := int64(-1)
	imagePos := int64(-1)
	joinPos := int64(-1)
	imageSession := int32(-1)
	if adapter := m.LogAdapter(); adapter != nil {
		adapterPos = adapter.Position()
		if img := adapter.Image(); img != nil {
			imagePos = img.Position()
			joinPos = img.JoinPosition()
			imageSession = img.SessionID()
		}
	}

	memberID := int32(-1)
	role := consensus.Role(-1)
	term := int64(-1)
	leaderID := int32(-1)
	appendSrc := int64(-1)
	commit := int64(-1)
	notified := int64(-1)
	localLogRecordingID := int64(-1)
	recordingActive := false
	appendRankFailure := ""
	electionCount := int64(-1)
	lastElectionTrigger := ""
	openSessions := -1
	pendingSessions := -1
	closingSessions := -1
	if agent := m.Agent(); agent != nil {
		memberID = agent.ThisMemberID()
		role = agent.Role()
		term = agent.LeadershipTermID()
		leaderID = agent.LeaderMemberID()
		if src := agent.AppendSource(); src != nil {
			if pos, ready := src.AppendPosition(); ready {
				appendSrc = pos
			}
		}
		commit = agent.CommitPosition()
		notified = agent.NotifiedCommitPosition()
		localLogRecordingID = agent.LocalLogRecordingID()
		recordingActive = agent.IsRecordingActive()
		if rankErr := agent.AppendRankFailure(); rankErr != nil {
			appendRankFailure = rankErr.Error()
		}
		electionCount = agent.ElectionCount()
		lastElectionTrigger = agent.LastElectionTrigger().String()
		openSessions = agent.OpenSessionCount()
		pendingSessions = agent.PendingSessionCount()
		closingSessions = agent.ClosingSessionCount()
	}

	recEntries := 0
	if rec := m.RecordingLog(); rec != nil {
		recEntries = len(rec.Entries())
	}

	archiveSession := int64(-1)
	archiveConnected := false
	if archiveClient := m.ArchiveClient(); archiveClient != nil && !archiveClient.IsClosed() {
		archiveSession = archiveClient.ControlSessionID()
		archiveConnected = true
	}

	// An active recording reports no stop position. Skip the lookup so a State
	// dump does not issue an archive control round trip per member.
	stopPosition := int64(recordinglog.NullPosition)
	if !recordingActive {
		stopPosition = m.LogRecordingStopPosition()
	}

	// A null recording id reports -1 without scanning counters. A missing type-100
	// counter also reports -1.
	recordingPos := int64(-1)
	if localLogRecordingID != recordinglog.NullRecordingID {
		recordingPos = RecordingCounterPosition(m, localLogRecordingID)
	}

	return fmt.Sprintf(
		"member=%d state=%v complete=%v role=%v term=%d leaderID=%d "+
			"images=%d logPubSession=%d logPubPos=%d adapterPos=%d imagePos=%d "+
			"joinPos=%d imageSession=%d appendSrc=%d commit=%d notified=%d "+
			"recLogEntries=%d coldStart=%v localLogRecordingID=%d recordingPos=%d "+
			"archiveConnected=%v archiveSession=%d replayReason=%q "+
			"recordingActive=%v appendRankFailure=%q electionCount=%d "+
			"lastElectionTrigger=%s logRecordingStopPosition=%d "+
			"openSessions=%d pendingSessions=%d closingSessions=%d\n",
		memberID,
		m.ElectionState(),
		m.ElectionComplete(),
		role,
		term,
		leaderID,
		images,
		logPubSession,
		logPubPos,
		adapterPos,
		imagePos,
		joinPos,
		imageSession,
		appendSrc,
		commit,
		notified,
		recEntries,
		m.IsColdStart(),
		localLogRecordingID,
		recordingPos,
		archiveConnected,
		archiveSession,
		m.LogReplayReason(),
		recordingActive,
		appendRankFailure,
		electionCount,
		lastElectionTrigger,
		stopPosition,
		openSessions,
		pendingSessions,
		closingSessions,
	)
}

// RecordingCounterPosition returns the recording-position counter value for
// recordingID on m. Returns -1 when the module, counters reader, or counter is
// missing.
func RecordingCounterPosition(m *cluster.ConsensusModule, recordingID int64) int64 {
	if m == nil {
		return -1
	}
	reader, err := m.Client().CountersReader()
	if err != nil {
		return -1
	}
	slot, ok := reader.FindRecordingPositionCounter(recordingID, cnc.NullValue)
	if !ok {
		return -1
	}
	return slot.Get()
}

// Close closes every module in reverse open order. Idempotent via each
// module's idempotent Close. Module Close clears each transport client,
// archive control client, and LogStreams.
//
// Ownership: does not stop Archives or media drivers. StartClusterMemberSet
// owns those via t.Cleanup (driver Stop and Archive Stop). One-off
// harness.Start and StartArchiveNode paths register their own Cleanup.
func (s *ClusterMemberSet) Close() error {
	if s == nil {
		return nil
	}

	var err error
	for _, m := range slices.Backward(s.modules) {
		err = errors.Join(err, m.Close())
	}
	return err
}
