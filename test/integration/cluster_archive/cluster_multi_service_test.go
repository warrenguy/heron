//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	multiServiceCount          = 2
	multiServiceMessagePayload = "multi-service-committed-message"
)

// amendMultiServiceMember installs session auth and sets ServiceCount to 2.
// Control channel and stream ids stay the cluster defaults.
func amendMultiServiceMember(cfg cluster.Config) cluster.Config {
	cfg = amendSessionAuth(cfg)
	cfg.ServiceCount = multiServiceCount
	return cfg
}

// gatedService wraps one attached invoker with a test-controlled hold. DoWork
// returns 0 while held and does not call the wrapped invoker. Held and
// released on the test goroutine only.
type gatedService struct {
	inner harness.ServiceInvoker
	held  bool
}

func (g *gatedService) DoWork(nowNs int64) int {
	if g.held {
		return 0
	}
	return g.inner.DoWork(nowNs)
}

// serviceSeat is one attached container on one member. Invoker-only. No extra
// locks.
type serviceSeat struct {
	agent    *service.Agent
	recorder *sessionRecorder
	gate     *gatedService
	checked  *boundCheckedService
}

// dumpMultiService returns member-set state plus one line per attached
// container. Each line is keyed by member id and service id.
func dumpMultiService(
	set *harness.ClusterMemberSet,
	seats [][]*serviceSeat,
) string {
	var b []byte
	if set != nil {
		b = append(b, set.State()...)
		b = append(b, '\n')
	}
	for i, memberSeats := range seats {
		var (
			commit      int64 = -1
			expectedAck int64 = -1
			waits             = -1
			ackQ0             = -1
			ackQ1             = -1
			ackFail     error
			memberID    int32 = -1
		)
		var module *cluster.ConsensusModule
		if set != nil {
			module = set.Module(i)
		}
		if module != nil {
			commit = module.CommitPosition()
			memberID = module.ThisMemberID()
			if ca := module.Agent(); ca != nil {
				expectedAck = ca.ExpectedServiceAckID()
				waits = ca.PendingServiceAckWaits()
				ackQ0 = ca.ServiceAckQueueDepth(0)
				ackQ1 = ca.ServiceAckQueueDepth(1)
				ackFail = ca.ServiceAckFailure()
			}
		}
		for sid, seat := range memberSeats {
			if seat == nil || seat.agent == nil {
				b = append(b, fmt.Sprintf(
					"service member=%d service=%d missing\n",
					memberID,
					sid,
				)...)
				continue
			}
			var violation error
			if seat.checked != nil {
				violation = seat.checked.violation
			}
			recStart, recMsgs := 0, 0
			var recOpens, recCloses []int64
			if seat.recorder != nil {
				recStart = seat.recorder.startCount
				recOpens = seat.recorder.opens
				recMsgs = len(seat.recorder.messages)
				recCloses = seat.recorder.closes
			}
			b = append(b, fmt.Sprintf(
				"service member=%d service=%d bound=%v logPos=%d commit=%d nextAck=%d "+
					"pendingJoin=%v role=%v start=%d opens=%v msgs=%d closes=%v "+
					"serviceFail=%v ackFail=%v expectedAck=%d waits=%d ackQ0=%d ackQ1=%d "+
					"violation=%v\n",
				memberID,
				sid,
				seat.agent.IsLogBound(),
				seat.agent.LogPosition(),
				commit,
				seat.agent.NextAckID(),
				seat.agent.HasPendingJoin(),
				seat.agent.Role(),
				recStart,
				recOpens,
				recMsgs,
				recCloses,
				seat.agent.ServiceFailure(),
				ackFail,
				expectedAck,
				waits,
				ackQ0,
				ackQ1,
				violation,
			)...)
		}
	}
	return string(b)
}

// requireMultiServiceAwaitTick runs TickUntil. On timeout the helper dumps
// the member set plus one line per attached container. On success the helper
// logs elapsed time.
func requireMultiServiceAwaitTick(
	t *testing.T,
	set *harness.ClusterMemberSet,
	seats [][]*serviceSeat,
	label string,
	bound time.Duration,
	pred func() bool,
) {
	t.Helper()

	start := time.Now()
	done := set.TickUntil(bound, pred)
	if !done {
		require.FailNow(
			t,
			label+" did not complete",
			"after %s (bound %s):\n%s",
			time.Since(start),
			bound,
			dumpMultiService(set, seats),
		)
	}
	t.Logf("%s elapsed=%s", label, time.Since(start))
}

// requireNoMultiServiceFailures asserts ServiceFailure and ServiceAckFailure
// are nil on every member and attached container. detail is inserted into each
// failure message.
func requireNoMultiServiceFailures(
	t *testing.T,
	set *harness.ClusterMemberSet,
	seats [][]*serviceSeat,
	detail string,
) {
	t.Helper()

	for i, memberSeats := range seats {
		m := set.Module(i)
		for sid, seat := range memberSeats {
			if err := seat.agent.ServiceFailure(); err != nil {
				require.NoError(t, err,
					"ServiceFailure%s member %d service %d:\n%s",
					detail, m.ThisMemberID(), sid, dumpMultiService(set, seats))
			}
		}
		if err := m.Agent().ServiceAckFailure(); err != nil {
			require.NoError(t, err,
				"ServiceAckFailure%s member %d:\n%s",
				detail, m.ThisMemberID(), dumpMultiService(set, seats))
		}
	}
}

// openMultiServiceCluster starts three members with two session recorder
// containers attached per member. Each container connects its own archive
// client on response stream 121 + service id. The helper then awaits log
// images and election. Containers open and attach before the first tick.
// Returned seats are indexed [member][serviceID].
func openMultiServiceCluster(t *testing.T) (*harness.ClusterMemberSet, [][]*serviceSeat) {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             harness.ClusterMultiServicePortBase,
		LogMulticastEndpoint: harness.ClusterMultiServiceLogEndpoint,
		AmendMemberConfig:    amendMultiServiceMember,
	})
	require.Len(t, set.Modules(), 3)

	seats := make([][]*serviceSeat, 3)
	for i := range 3 {
		module := set.Module(i)
		memberSeats := make([]*serviceSeat, multiServiceCount)
		for sid := range multiServiceCount {
			rec := &sessionRecorder{}
			container, err := service.Open(service.Config{
				Aeron:             module.Client(),
				Service:           rec,
				CommittedPosition: module.CommitPosition,
				ServiceID:         int32(sid),
				Archive:           &archive.Config{},
			})
			require.NoError(t, err)
			harness.CleanupClose(
				t,
				fmt.Sprintf("service container member %d service %d", i, sid),
				container.Close,
			)
			require.Equal(
				t,
				int32(121+sid),
				container.ArchiveClient().Config().EffectiveControlResponseStreamID(),
			)

			agent := container.Agent()
			checked := &boundCheckedService{agent: agent, module: module}
			gate := &gatedService{inner: checked}
			memberSeats[sid] = &serviceSeat{
				agent:    agent,
				recorder: rec,
				gate:     gate,
				checked:  checked,
			}
			set.AttachService(t, i, gate)
		}
		seats[i] = memberSeats
	}

	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)
	requireSingleLeader(t, set, "")

	return set, seats
}

// requireHeldAckRound holds one service across a requested ack, then releases.
// The peer is the other attached service. The peer must ack while heldID is
// held.
func requireHeldAckRound(
	t *testing.T,
	set *harness.ClusterMemberSet,
	seats [][]*serviceSeat,
	m *cluster.ConsensusModule,
	transport *aeron.Client,
	session *client.AeronCluster,
	heldID int,
) {
	t.Helper()

	peerID := multiServiceCount - 1 - heldID
	memberID := m.ThisMemberID()
	i := int(memberID)
	h := seats[i][heldID]
	p := seats[i][peerID]
	ca := m.Agent()
	expectedBefore := ca.ExpectedServiceAckID()
	heldNextBefore := h.agent.NextAckID()
	peerNextBefore := p.agent.NextAckID()

	requireMultiServiceAwaitTick(
		t, set, seats,
		fmt.Sprintf("services at commit member %d held %d", memberID, heldID),
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			pos := m.CommitPosition()
			return h.agent.LogPosition() == pos && p.agent.LogPosition() == pos
		}),
	)

	h.gate.held = true
	pos := m.CommitPosition()
	if err := m.RequestServiceAck(pos); err != nil {
		require.NoError(t, err,
			"RequestServiceAck member %d pos=%d held %d:\n%s",
			memberID, pos, heldID, dumpMultiService(set, seats))
	}

	requireMultiServiceAwaitTick(
		t, set, seats,
		fmt.Sprintf("peer ack member %d held %d", memberID, heldID),
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return p.agent.NextAckID() == peerNextBefore+1 &&
				ca.ServiceAckQueueDepth(int32(peerID)) == 1
		}),
	)

	pump := sessionPump(transport, session, func() bool { return false })
	for range 100 {
		set.Tick()
		_ = pump()
	}

	if got := ca.ExpectedServiceAckID(); got != expectedBefore {
		require.Equal(t, expectedBefore, got,
			"ExpectedServiceAckID while held member %d held %d:\n%s",
			memberID, heldID, dumpMultiService(set, seats))
	}
	if got := ca.PendingServiceAckWaits(); got != 1 {
		require.Equal(t, 1, got,
			"PendingServiceAckWaits while held member %d held %d:\n%s",
			memberID, heldID, dumpMultiService(set, seats))
	}
	if got := ca.ServiceAckQueueDepth(int32(heldID)); got != 0 {
		require.Equal(t, 0, got,
			"held ServiceAckQueueDepth member %d held %d:\n%s",
			memberID, heldID, dumpMultiService(set, seats))
	}
	if got := ca.ServiceAckQueueDepth(int32(peerID)); got != 1 {
		require.Equal(t, 1, got,
			"peer ServiceAckQueueDepth member %d held %d peer %d:\n%s",
			memberID, heldID, peerID, dumpMultiService(set, seats))
	}
	if got := h.agent.NextAckID(); got != heldNextBefore {
		require.Equal(t, heldNextBefore, got,
			"held NextAckID member %d held %d:\n%s",
			memberID, heldID, dumpMultiService(set, seats))
	}
	if err := ca.ServiceAckFailure(); err != nil {
		require.NoError(t, err,
			"ServiceAckFailure while held member %d held %d:\n%s",
			memberID, heldID, dumpMultiService(set, seats))
	}

	h.gate.held = false
	requireMultiServiceAwaitTick(
		t, set, seats,
		fmt.Sprintf("held ack complete member %d held %d", memberID, heldID),
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return ca.ExpectedServiceAckID() == expectedBefore+1 &&
				ca.PendingServiceAckWaits() == 0 &&
				ca.ServiceAckQueueDepth(0) == 0 &&
				ca.ServiceAckQueueDepth(1) == 0 &&
				h.agent.NextAckID() == heldNextBefore+1
		}),
	)
}

// TestClusterMultiService_ackWaitsCompleteOnlyWhenEveryServiceAcks asserts a
// requested service ack wait completes only after every service on that member
// acks.
//
// Three members run with ServiceCount 2. Two containers attach per member
// before the first tick.
//
// Open order:
//  1. join settle
//  2. committed session message
//  3. leader holds service 1 then service 0
//  4. each follower holds service 1 then service 0
//
// Asserts:
//   - after join settle, expected ack id is 1
//   - after join settle, both service queues are empty
//   - a committed session message does not move expected ack id or NextAckID
//
// After RequestServiceAck with one service DoWork held:
//   - the unheld service acks
//   - 100 further ticks keep expected ack id unchanged
//   - PendingServiceAckWaits is 1
//   - held queue depth is 0
//   - unheld queue depth is 1
//   - held NextAckID is unchanged
//   - ServiceAckFailure is nil
//
// Release completes the wait. Expected ack id and NextAckID advance. After
// both holds on a member, expected ack id and NextAckID are 3.
func TestClusterMultiService_ackWaitsCompleteOnlyWhenEveryServiceAcks(t *testing.T) {
	t.Parallel()

	set, seats := openMultiServiceCluster(t)
	leader := requireSingleLeader(t, set, "")

	requireMultiServiceAwaitTick(
		t, set, seats,
		"join settle",
		durableWorkBound,
		func() bool {
			for i, m := range set.Modules() {
				ca := m.Agent()
				if ca.ExpectedServiceAckID() != 1 || ca.PendingServiceAckWaits() != 0 {
					return false
				}
				if ca.ServiceAckQueueDepth(0) != 0 || ca.ServiceAckQueueDepth(1) != 0 {
					return false
				}
				for _, seat := range seats[i] {
					if !seat.agent.IsLogBound() ||
						seat.agent.HasPendingJoin() ||
						seat.agent.NextAckID() != 1 {
						return false
					}
				}
			}
			return true
		},
	)
	for i, m := range set.Modules() {
		wantRole := codecs.RoleFollower
		roleLabel := "follower"
		if m.IsLeader() {
			wantRole = codecs.RoleLeader
			roleLabel = "leader"
		}
		for sid, seat := range seats[i] {
			if got := seat.agent.Role(); got != wantRole {
				require.Equal(t, wantRole, got,
					"%s service role member %d service %d:\n%s",
					roleLabel, m.ThisMemberID(), sid, dumpMultiService(set, seats))
			}
			if got := seat.recorder.startCount; got != 1 {
				require.Equal(t, 1, got,
					"Start count member %d service %d:\n%s",
					m.ThisMemberID(), sid, dumpMultiService(set, seats))
			}
		}
	}
	requireNoMultiServiceFailures(t, set, seats, "")

	transport, session := openSession(t, set, leader)
	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	wantSession := session.ClusterSessionID()

	requireMultiServiceAwaitTick(
		t, set, seats,
		"session open committed",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			if leader.Agent().OpenSessionCount() != 1 {
				return false
			}
			for _, memberSeats := range seats {
				for _, seat := range memberSeats {
					if !seat.recorder.holdsOpen(wantSession) {
						return false
					}
				}
			}
			return true
		}),
	)

	payload := []byte(multiServiceMessagePayload)
	offerCtx, cancelOffer := context.WithTimeout(t.Context(), durableWorkBound)
	defer cancelOffer()
	lastKeepalive := time.Now()
	pos, offerErr := session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		},
		func() int64 {
			set.Tick()
			now := time.Now()
			if now.Sub(lastKeepalive) >= time.Second {
				_ = session.SendKeepAlive()
				lastKeepalive = now
			}
			return session.Offer(payload)
		},
	)
	if offerErr != nil {
		require.FailNow(
			t,
			"session offer failed",
			"err=%v:\n%s",
			offerErr,
			dumpMultiService(set, seats),
		)
	}
	require.Positive(t, pos)

	requireMultiServiceAwaitTick(
		t, set, seats,
		"session message delivered",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			for _, memberSeats := range seats {
				for _, seat := range memberSeats {
					if !seat.recorder.holdsOneMessageAfterOpen(wantSession, payload) {
						return false
					}
				}
			}
			return true
		}),
	)

	for i, m := range set.Modules() {
		ca := m.Agent()
		if got := ca.ExpectedServiceAckID(); got != 1 {
			require.Equal(t, int64(1), got,
				"ExpectedServiceAckID after message member %d:\n%s",
				m.ThisMemberID(), dumpMultiService(set, seats))
		}
		if got := ca.ServiceAckQueueDepth(0); got != 0 {
			require.Equal(t, 0, got,
				"ServiceAckQueueDepth(0) after message member %d:\n%s",
				m.ThisMemberID(), dumpMultiService(set, seats))
		}
		if got := ca.ServiceAckQueueDepth(1); got != 0 {
			require.Equal(t, 0, got,
				"ServiceAckQueueDepth(1) after message member %d:\n%s",
				m.ThisMemberID(), dumpMultiService(set, seats))
		}
		for sid, seat := range seats[i] {
			if got := seat.agent.NextAckID(); got != 1 {
				require.Equal(t, int64(1), got,
					"NextAckID after message member %d service %d:\n%s",
					m.ThisMemberID(), sid, dumpMultiService(set, seats))
			}
		}
	}

	ackMembers := make([]*cluster.ConsensusModule, 0, len(set.Modules()))
	ackMembers = append(ackMembers, leader)
	for _, m := range set.Modules() {
		if m != nil && !m.IsLeader() {
			ackMembers = append(ackMembers, m)
		}
	}
	for _, m := range ackMembers {
		requireHeldAckRound(t, set, seats, m, transport, session, 1)
		requireHeldAckRound(t, set, seats, m, transport, session, 0)

		if got := m.Agent().ExpectedServiceAckID(); got != 3 {
			require.Equal(t, int64(3), got,
				"ExpectedServiceAckID after both holds member %d:\n%s",
				m.ThisMemberID(), dumpMultiService(set, seats))
		}
		i := int(m.ThisMemberID())
		for sid, seat := range seats[i] {
			if got := seat.agent.NextAckID(); got != 3 {
				require.Equal(t, int64(3), got,
					"NextAckID after both holds member %d service %d:\n%s",
					m.ThisMemberID(), sid, dumpMultiService(set, seats))
			}
		}
	}

	require.NoError(t, session.Close())
	const closeBound = 30 * time.Second
	requireMultiServiceAwaitTick(
		t, set, seats,
		"session registry drained",
		closeBound,
		func() bool {
			transport.DoWork()
			ca := leader.Agent()
			if ca.OpenSessionCount() != 0 ||
				ca.PendingSessionCount() != 0 ||
				ca.ClosingSessionCount() != 0 {
				return false
			}
			for _, memberSeats := range seats {
				for _, seat := range memberSeats {
					if !seat.recorder.holdsClose(wantSession) {
						return false
					}
				}
			}
			return true
		},
	)

	for i, memberSeats := range seats {
		for sid, seat := range memberSeats {
			if seat.checked.violation != nil {
				require.NoError(t, seat.checked.violation,
					"commit bound member %d service %d:\n%s",
					set.Module(i).ThisMemberID(), sid, dumpMultiService(set, seats))
			}
		}
	}
	requireNoMultiServiceFailures(t, set, seats, " final")
}
