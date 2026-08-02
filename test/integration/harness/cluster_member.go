//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
)

// defaultClusterMemberCloseLinger is the Client.Close linger cap per member at
// teardown.
//
// The aeron default is three seconds. Nothing polls after the duty cycle stops,
// so the fixture must not wait that long per member.
const defaultClusterMemberCloseLinger = 250 * time.Millisecond

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
//   - Per member, registration order is driver Stop (inside Start), Archive
//     Stop (inside StartArchiveNode), then the slot close, which closes
//     whatever module occupies the slot.
//   - LIFO teardown per member is the slot close, then Archive Stop, then
//     driver Stop.
//   - Across the set, later slots close before earlier slots. After every
//     slot close, Cleanup LIFO stops later Archives and drivers before
//     earlier ones.
//   - Close is public and idempotent. Callers may call Close early.
//   - Invoker must not call Tick, PumpTransportUntil, TickUntil,
//     AwaitLogImages, AwaitElection, or LogSessionIDs after Close.
//     Close clears the transport client, archive control client, and
//     LogStreams.
//   - State stays readable after Close for failure dumps.
//     Nil handles report as sentinel values.
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
}

// StartClusterMemberSet starts MemberCount media drivers, one Archive server
// per driver, and one ConsensusModule per driver against a shared multicast
// cluster log.
//
// Open order uses two loops so every Archive is ready before any member opens:
//  1. Per member: harness.Start registers driver Stop; StartArchiveNode
//     registers Archive Stop; the set stores the member's cluster.Config.
//  2. Per member: successful OpenConsensusModule registers the slot close,
//     which closes whatever module occupies the slot.
//
// Per member LIFO is the slot close, then Archive Stop, then driver Stop.
// Across a three-member set, Cleanup LIFO is:
//
//	slot 2, slot 1, slot 0, archive 2, driver 2, archive 1, driver 1,
//	archive 0, driver 0
//
// Every slot closes before any Archive stops.
//
// A mid-loop open failure keeps Cleanup for every resource already started.
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

	set := &ClusterMemberSet{
		membersString: membersString,
	}

	// Loop 1: every driver, Archive, and stored config before any open.
	for i := range cfg.MemberCount {
		md := Start(t)

		controlPort := cluster.CalculatePort(
			int32(i),
			cfg.PortBase,
			cluster.ArchiveControlPortOffset,
		)
		controlChannel := "aeron:udp?endpoint=127.0.0.1:" + strconv.Itoa(controlPort)
		node := StartArchiveNode(t, ArchiveNodeConfig{
			AeronDir:       md.Dir,
			ControlChannel: controlChannel,
		})
		set.nodes = append(set.nodes, node)
		set.configs = append(set.configs, cluster.Config{
			MemberID:                      int32(i),
			ClusterMembers:                membersString,
			ClusterDir:                    t.TempDir(),
			AeronDir:                      md.Dir,
			LogChannel:                    ClusterLogChannelTemplate,
			ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
			ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
			AeronConfig: aeron.Config{
				InterServiceTimeout: ConnectedPollTimeout,
				CloseLingerDuration: defaultClusterMemberCloseLinger,
			},
		})
	}

	// Loop 2: open members after every Archive is ready.
	for i := range cfg.MemberCount {
		module, openErr := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
			Config: set.configs[i],
		})
		require.NoError(t, openErr)
		require.NotNil(t, module)
		set.modules = append(set.modules, module)

		// Register the slot close after Start and StartArchiveNode so LIFO
		// closes the module before Archive Stop and driver Stop. StopMember
		// is idempotent on an empty slot, so a test that already stopped or
		// restarted a member closes only the module that occupies the slot.
		CleanupClose(t, fmt.Sprintf("cluster member %d", i), func() error {
			return set.StopMember(i)
		})
	}
	return set
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
// or stop the nodes. Process lifetime is StartArchiveNode via t.Cleanup. Close
// does not stop Archives. A member restart leaves the Archive node in place.
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
// Behaviour: closes the module, which releases the cluster directory lock last.
// Leaves the media driver and the Archive node running, so RestartMember can
// reuse both. Does not call ArchiveNode.Stop and does not call MediaDriver.Stop.
//
// Concurrency: invoker-only. Must not run concurrently with Tick.
//
// Lifetime: the cluster directory and the Archive data directory survive.
//
// Returns an error when the module's Close reports one, or when index is out of
// range. Idempotent: a second call on an empty slot returns nil.
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
// Behaviour: opens one cluster.ConsensusModule against the retained config. The
// cluster directory, media-driver directory, and Archive control channels are
// the previous boot's. The recovery plan is non-empty when the previous boot
// wrote term entries.
//
// Concurrency: invoker-only. Must not run concurrently with Tick.
//
// Invoker must call StopMember for the same index first. RestartMember on an
// occupied slot:
//   - returns an error without opening a second module
//   - no transport connect or archive session runs for that refused open
//
// Returns an error when:
//   - index is out of range
//   - the slot is occupied
//   - the open fails
//
// On error:
//   - empty slot stays empty
//   - occupied slot is left unchanged
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
	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: s.configs[index],
	})
	if err != nil {
		return err
	}
	s.modules[index] = module
	return nil
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

// ClusterMemberState returns a one-line dump of one consensus module for failure
// messages.
//
// Behaviour: safe after Close. Nil handles and a nil module report as sentinel
// values.
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

	appendSrc := int64(-1)
	localLogRecordingID := int64(-1)
	if agent := m.Agent(); agent != nil {
		localLogRecordingID = agent.LocalLogRecordingID()
		if src := agent.AppendSource(); src != nil {
			if pos, ready := src.AppendPosition(); ready {
				appendSrc = pos
			}
		}
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

	return fmt.Sprintf(
		"member=%d state=%v complete=%v role=%v term=%d leaderID=%d "+
			"images=%d logPubSession=%d logPubPos=%d adapterPos=%d imagePos=%d "+
			"joinPos=%d imageSession=%d appendSrc=%d commit=%d notified=%d "+
			"recLogEntries=%d coldStart=%v localLogRecordingID=%d "+
			"archiveConnected=%v archiveSession=%d replayReason=%q\n",
		m.ThisMemberID(),
		m.ElectionState(),
		m.ElectionComplete(),
		m.Role(),
		m.LeadershipTermID(),
		m.LeaderMemberID(),
		images,
		logPubSession,
		logPubPos,
		adapterPos,
		imagePos,
		joinPos,
		imageSession,
		appendSrc,
		m.CommitPosition(),
		m.NotifiedCommitPosition(),
		recEntries,
		m.IsColdStart(),
		localLogRecordingID,
		archiveConnected,
		archiveSession,
		m.LogReplayReason(),
	)
}

// Close closes every module in reverse open order. Idempotent via each
// module's idempotent Close. Module Close clears each transport client,
// archive control client, and LogStreams.
//
// Does not stop Archives or media drivers. StartArchiveNode and harness.Start
// own those via t.Cleanup.
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
