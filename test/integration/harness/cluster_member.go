//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
)

// defaultClusterMemberCloseLinger is the Client.Close linger cap per member at
// teardown.
//
// The aeron default is three seconds. Nothing polls after the duty cycle stops,
// so the fixture must not wait that long per member.
const defaultClusterMemberCloseLinger = 250 * time.Millisecond

// ClusterMemberSet is a set of in-process consensus members. One media driver
// and one ConsensusModule per member. All members share one cluster members
// string.
//
// This set starts no Archive server. Product topology co-locates one Archive
// server per member. No member shares an Archive with another member. The
// caller starts the Archive server.
//
// Concurrency: invoker-only. Every method runs on the test goroutine.
//
// Lifetime:
//   - After each successful open, t.Cleanup registers that module's Close.
//   - Registration runs after harness.Start for the same member.
//   - LIFO teardown closes the module before that driver's Stop.
//   - LIFO teardown closes later members before earlier members.
//   - Close is public and idempotent. Callers may call Close early.
//   - Invoker must not call Tick, PumpTransportUntil, TickUntil,
//     AwaitLogImages, AwaitElection, or LogSessionIDs after Close.
//     Close clears the transport client and LogStreams.
//   - State stays readable after Close for failure dumps.
//     Nil handles report as sentinel values.
type ClusterMemberSet struct {
	modules       []*cluster.ConsensusModule
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

// StartClusterMemberSet starts MemberCount media drivers and opens one
// ConsensusModule per driver against a shared multicast cluster log.
//
// Open order per member:
//  1. harness.Start registers driver Stop on t.Cleanup
//  2. successful OpenConsensusModule registers module Close on t.Cleanup
//
// LIFO for that member is Close then Stop. Across the set, later members
// close before earlier members.
//
// A mid-loop open failure keeps Cleanup for every module already opened.
func StartClusterMemberSet(t testing.TB, cfg ClusterMemberSetConfig) *ClusterMemberSet {
	t.Helper()

	require.GreaterOrEqual(t, cfg.MemberCount, 1, "ClusterMemberSet: MemberCount must be >= 1")
	require.Positive(t, cfg.PortBase, "ClusterMemberSet: PortBase must be positive")
	require.NotEmpty(
		t,
		cfg.LogMulticastEndpoint,
		"ClusterMemberSet: LogMulticastEndpoint must be non-empty",
	)

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
	for i := range cfg.MemberCount {
		md := Start(t)
		module, openErr := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
			Config: cluster.Config{
				MemberID:       int32(i),
				ClusterMembers: membersString,
				ClusterDir:     t.TempDir(),
				AeronDir:       md.Dir,
				LogChannel:     ClusterLogChannelTemplate,
				AeronConfig: aeron.Config{
					InterServiceTimeout: ConnectedPollTimeout,
					CloseLingerDuration: defaultClusterMemberCloseLinger,
				},
			},
		})
		require.NoError(t, openErr)
		require.NotNil(t, module)
		set.modules = append(set.modules, module)

		// Register Close after Start's Stop Cleanup so LIFO closes the module before
		// the driver.
		CleanupClose(t, fmt.Sprintf("cluster member %d", i), module.Close)
	}
	return set
}

// Modules returns every consensus module in member-id order.
//
// Ownership: the set owns the slice. Caller must not mutate the returned slice
// (append, reorder, or replace elements).
func (s *ClusterMemberSet) Modules() []*cluster.ConsensusModule {
	return s.modules
}

// MembersString returns the shared cluster members string.
func (s *ClusterMemberSet) MembersString() string {
	return s.membersString
}

// LogSessionIDs returns each member's log publication session id in member-id
// order.
//
// Invoker must not call LogSessionIDs after Close. Close clears LogStreams.
func (s *ClusterMemberSet) LogSessionIDs() []int32 {
	sessions := make([]int32, 0, len(s.modules))
	for _, m := range s.modules {
		sessions = append(sessions, m.LogStreams().Publication().SessionID())
	}
	return sessions
}

// Leaders returns every member reporting IsLeader, in member-id order.
//
// A settled election yields exactly one element.
func (s *ClusterMemberSet) Leaders() []*cluster.ConsensusModule {
	var leaders []*cluster.ConsensusModule
	for _, m := range s.modules {
		if m.IsLeader() {
			leaders = append(leaders, m)
		}
	}
	return leaders
}

// Tick runs one duty cycle over every member. First every transport client.
// Then every agent.
//
// Two loops, not one interleaved loop. A member's consensus publication and a
// peer's consensus subscription live on different drivers. A frame written by
// one agent is visible only after the peer's client has pumped.
//
// Invoker must not call Tick after Close.
func (s *ClusterMemberSet) Tick() {
	for _, m := range s.modules {
		m.Client().DoWork()
	}
	for _, m := range s.modules {
		m.Agent().DoWork(m.NanoClock()())
	}
}

// PumpTransportUntil pumps every transport client until ok reports true or the
// deadline bound elapses. Does not call any agent.
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
		for _, m := range s.modules {
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

// AwaitLogImages pumps transport until every member's log subscription holds
// an image for every member's log publication session. Does not call any
// agent.
//
// Walks images by session id, not bare ImageCount. ImageCount can pass on
// foreign images while a peer session is still missing.
//
// Returns:
//   - true when every image is present before the bound
//   - false on timeout
//
// Invoker must not call AwaitLogImages after Close.
func (s *ClusterMemberSet) AwaitLogImages(bound time.Duration) bool {
	sessions := s.LogSessionIDs()
	return s.PumpTransportUntil(bound, func() bool {
		for _, m := range s.modules {
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

// AwaitElection runs the full duty cycle until every member reports
// ElectionComplete.
//
// Returns:
//   - true when every member completes before the bound
//   - false on timeout
//
// Invoker must not call AwaitElection after Close.
func (s *ClusterMemberSet) AwaitElection(bound time.Duration) bool {
	return s.TickUntil(bound, func() bool {
		for _, m := range s.modules {
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
	if agent := m.Agent(); agent != nil {
		if src := agent.AppendSource(); src != nil {
			appendSrc = src.AppendPosition()
		}
	}

	recEntries := 0
	if rec := m.RecordingLog(); rec != nil {
		recEntries = len(rec.Entries())
	}

	return fmt.Sprintf(
		"member=%d state=%v complete=%v role=%v term=%d leaderID=%d "+
			"images=%d logPubSession=%d logPubPos=%d adapterPos=%d imagePos=%d "+
			"joinPos=%d imageSession=%d appendSrc=%d commit=%d notified=%d "+
			"recLogEntries=%d\n",
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
	)
}

// Close closes every module in reverse open order. Idempotent via each
// module's idempotent Close.
//
// Does not stop media drivers. harness.Start owns those via t.Cleanup.
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
