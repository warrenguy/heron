//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/harness"
)

const serviceMessagePayload = "service-committed-message"

// amendServiceMember installs session auth and ServiceCount 1.
// Control channel and stream ids stay cluster defaults.
func amendServiceMember(cfg cluster.Config) cluster.Config {
	cfg = amendSessionAuth(cfg)
	cfg.ServiceCount = 1
	return cfg
}

// boundCheckedService ticks one service agent and records the first
// commit-bound violation. Reads CommitPosition after the agent tick. Nothing
// runs between the tick and the read on the test goroutine.
type boundCheckedService struct {
	agent     *service.Agent
	module    *cluster.ConsensusModule
	violation error
}

func (b *boundCheckedService) DoWork(nowNs int64) int {
	n := b.agent.DoWork(nowNs)
	if b.violation != nil || !b.agent.IsLogBound() {
		return n
	}
	logPos := b.agent.LogPosition()
	commit := b.module.CommitPosition()
	if logPos > commit {
		b.violation = fmt.Errorf(
			"service log position %d exceeds commit position %d",
			logPos,
			commit,
		)
	}
	return n
}

// recordedMessage is one SessionMessage callback with a copied payload.
type recordedMessage struct {
	sessionID int64
	payload   []byte
}

// sessionRecorder records ClusteredService callbacks. Invoker-only. No
// locking. Copies payload bytes. Callback slices are transient.
type sessionRecorder struct {
	startCount int
	opens      []int64
	messages   []recordedMessage
	closes     []int64
	order      []string
}

func (s *sessionRecorder) Start(service.Cluster) error {
	s.startCount++
	return nil
}

func (s *sessionRecorder) SessionOpen(sessionID, _ int64) {
	s.opens = append(s.opens, sessionID)
	s.order = append(s.order, "open")
}

func (s *sessionRecorder) SessionClose(
	sessionID, _ int64,
	_ clusterwire.CloseReason,
) {
	s.closes = append(s.closes, sessionID)
	s.order = append(s.order, "close")
}

func (s *sessionRecorder) SessionMessage(sessionID, _ int64, payload []byte) {
	s.messages = append(s.messages, recordedMessage{
		sessionID: sessionID,
		payload:   bytes.Clone(payload),
	})
	s.order = append(s.order, "message")
}

func (s *sessionRecorder) TimerEvent(_, _ int64) {}

func (s *sessionRecorder) holdsOpen(sessionID int64) bool {
	return slices.Contains(s.opens, sessionID)
}

func (s *sessionRecorder) holdsClose(sessionID int64) bool {
	return slices.Contains(s.closes, sessionID)
}

// holdsOneMessageAfterOpen reports exactly one message for sessionID with
// payload, recorded after that session's open.
func (s *sessionRecorder) holdsOneMessageAfterOpen(
	sessionID int64,
	payload []byte,
) bool {
	if !s.holdsOpen(sessionID) || len(s.messages) != 1 {
		return false
	}
	msg := s.messages[0]
	if msg.sessionID != sessionID || !bytes.Equal(msg.payload, payload) {
		return false
	}
	openIdx := slices.Index(s.order, "open")
	msgIdx := slices.Index(s.order, "message")
	return openIdx >= 0 && msgIdx > openIdx
}

// serviceMember is one member's recorder, agent, and bound-check wrapper.
type serviceMember struct {
	rec     *sessionRecorder
	agent   *service.Agent
	checked *boundCheckedService
}

// serviceCluster holds one started member set and one serviceMember per slot.
type serviceCluster struct {
	set     *harness.ClusterMemberSet
	members []serviceMember
}

// serviceState dumps one member's service and ack getters for await failures.
func serviceState(
	index int,
	mem serviceMember,
	module *cluster.ConsensusModule,
) string {
	var (
		commit      int64 = -1
		expectedAck int64 = -1
		waits             = -1
		ackQ              = -1
		ackFail     error
		memberID    int32 = -1
	)
	if module != nil {
		commit = module.CommitPosition()
		memberID = module.ThisMemberID()
		if ca := module.Agent(); ca != nil {
			expectedAck = ca.ExpectedServiceAckID()
			waits = ca.PendingServiceAckWaits()
			ackQ = ca.ServiceAckQueueDepth(0)
			ackFail = ca.ServiceAckFailure()
		}
	}

	return fmt.Sprintf(
		"service member=%d index=%d bound=%v logPos=%d commit=%d nextAck=%d "+
			"expectedAck=%d pendingJoin=%v pendingAcks=%d readyAcks=%d waits=%d "+
			"ackQ0=%d role=%v start=%d opens=%v msgs=%d closes=%v "+
			"serviceFail=%v ackFail=%v violation=%v\n",
		memberID,
		index,
		mem.agent.IsLogBound(),
		mem.agent.LogPosition(),
		commit,
		mem.agent.NextAckID(),
		expectedAck,
		mem.agent.HasPendingJoin(),
		mem.agent.PendingCoordinationAcks(),
		mem.agent.ReadyCoordinationAcks(),
		waits,
		ackQ,
		mem.agent.Role(),
		mem.rec.startCount,
		mem.rec.opens,
		len(mem.rec.messages),
		mem.rec.closes,
		mem.agent.ServiceFailure(),
		ackFail,
		mem.checked.violation,
	)
}

// dump returns the member-set state and serviceState for every member.
func (sc serviceCluster) dump() string {
	var b []byte
	if sc.set != nil {
		b = append(b, sc.set.State()...)
		b = append(b, '\n')
	}
	for i, mem := range sc.members {
		var module *cluster.ConsensusModule
		if sc.set != nil {
			module = sc.set.Module(i)
		}
		b = append(b, serviceState(i, mem, module)...)
	}
	return string(b)
}

// requireServiceAwaitTick runs TickUntil. Dumps set plus service state on
// timeout. Logs elapsed on success.
func requireServiceAwaitTick(
	t *testing.T,
	sc serviceCluster,
	label string,
	bound time.Duration,
	pred func() bool,
) {
	t.Helper()

	start := time.Now()
	done := sc.set.TickUntil(bound, pred)
	if !done {
		require.FailNow(
			t,
			label+" did not complete",
			"after %s (bound %s):\n%s",
			time.Since(start),
			bound,
			sc.dump(),
		)
	}
	t.Logf("%s elapsed=%s", label, time.Since(start))
}

// requireNoServiceFailure asserts ServiceFailure and ServiceAckFailure are nil
// on the member at index. detail is inserted into each failure message.
func requireNoServiceFailure(
	t *testing.T,
	sc serviceCluster,
	index int,
	detail string,
) {
	t.Helper()

	m := sc.set.Module(index)
	if err := sc.members[index].agent.ServiceFailure(); err != nil {
		require.NoError(t, err,
			"ServiceFailure%s member %d:\n%s", detail, m.ThisMemberID(), sc.dump())
	}
	if err := m.Agent().ServiceAckFailure(); err != nil {
		require.NoError(t, err,
			"ServiceAckFailure%s member %d:\n%s", detail, m.ThisMemberID(), sc.dump())
	}
}

// requireNoServiceFailures asserts ServiceFailure and ServiceAckFailure are
// nil on every member. detail is inserted into each failure message.
func requireNoServiceFailures(t *testing.T, sc serviceCluster, detail string) {
	t.Helper()

	for i := range sc.members {
		requireNoServiceFailure(t, sc, i, detail)
	}
}

// openServiceCluster starts three members with one session recorder
// container attached per member, then awaits log images and election.
// Containers open and attach before the first tick.
func openServiceCluster(t *testing.T) serviceCluster {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             harness.ClusterServiceAckPortBase,
		LogMulticastEndpoint: harness.ClusterServiceAckLogEndpoint,
		AmendMemberConfig:    amendServiceMember,
	})
	require.Len(t, set.Modules(), 3)

	members := make([]serviceMember, 3)
	for i := range 3 {
		rec := &sessionRecorder{}
		module := set.Module(i)
		container, err := service.Open(service.Config{
			Aeron:             module.Client(),
			Service:           rec,
			CommittedPosition: module.CommitPosition,
		})
		require.NoError(t, err)
		harness.CleanupClose(t, fmt.Sprintf("service container %d", i), container.Close)

		agent := container.Agent()
		checked := &boundCheckedService{agent: agent, module: module}
		members[i] = serviceMember{rec: rec, agent: agent, checked: checked}
		set.AttachService(t, i, checked)
	}

	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)

	return serviceCluster{set: set, members: members}
}

// TestClusterService_deliversCommittedLogAndAcksOnRequest asserts a committed
// session message reaches every attached service. A requested service ack then
// completes on every member.
func TestClusterService_deliversCommittedLogAndAcksOnRequest(t *testing.T) {
	t.Parallel()

	sc := openServiceCluster(t)
	set := sc.set
	leader := requireSingleLeader(t, set, "")

	requireServiceAwaitTick(
		t, sc,
		"join settle",
		durableWorkBound,
		func() bool {
			for i, m := range set.Modules() {
				agent := sc.members[i].agent
				ca := m.Agent()
				if !agent.IsLogBound() || agent.HasPendingJoin() || agent.NextAckID() != 1 {
					return false
				}
				if ca.ExpectedServiceAckID() != 1 || ca.PendingServiceAckWaits() != 0 {
					return false
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
		if got := sc.members[i].agent.Role(); got != wantRole {
			require.Equal(t, wantRole, got,
				"%s service role member %d:\n%s",
				roleLabel, m.ThisMemberID(), sc.dump())
		}
		if got := sc.members[i].rec.startCount; got != 1 {
			require.Equal(t, 1, got,
				"Start count member %d:\n%s",
				m.ThisMemberID(), sc.dump())
		}
	}
	requireNoServiceFailures(t, sc, "")

	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnect(
		t, set, leader, transport, []byte(sessionSmokeCredentials),
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	wantSession := session.ClusterSessionID()

	requireServiceAwaitTick(
		t, sc,
		"session open committed",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			if leader.Agent().OpenSessionCount() != 1 {
				return false
			}
			for _, mem := range sc.members {
				if !mem.rec.holdsOpen(wantSession) {
					return false
				}
			}
			return true
		}),
	)

	payload := []byte(serviceMessagePayload)
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
			sc.dump(),
		)
	}
	require.Positive(t, pos)

	requireServiceAwaitTick(
		t, sc,
		"session message delivered",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			for _, mem := range sc.members {
				if !mem.rec.holdsOneMessageAfterOpen(wantSession, payload) {
					return false
				}
			}
			return true
		}),
	)

	for i, m := range set.Modules() {
		if got := sc.members[i].agent.NextAckID(); got != 1 {
			require.Equal(t, int64(1), got,
				"NextAckID after message member %d:\n%s",
				m.ThisMemberID(), sc.dump())
		}
		if got := m.Agent().ExpectedServiceAckID(); got != 1 {
			require.Equal(t, int64(1), got,
				"ExpectedServiceAckID after message member %d:\n%s",
				m.ThisMemberID(), sc.dump())
		}
		if got := m.Agent().ServiceAckQueueDepth(0); got != 0 {
			require.Equal(t, 0, got,
				"ServiceAckQueueDepth after message member %d:\n%s",
				m.ThisMemberID(), sc.dump())
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
		i := int(m.ThisMemberID())
		agent := sc.members[i].agent
		requireServiceAwaitTick(
			t, sc,
			fmt.Sprintf("service at commit member %d", m.ThisMemberID()),
			durableWorkBound,
			sessionPump(transport, session, func() bool {
				return agent.LogPosition() == m.CommitPosition()
			}),
		)
		ackPos := m.CommitPosition()
		if err := m.RequestServiceAck(ackPos); err != nil {
			require.NoError(t, err,
				"RequestServiceAck member %d pos=%d:\n%s",
				m.ThisMemberID(), ackPos, sc.dump())
		}
		requireServiceAwaitTick(
			t, sc,
			fmt.Sprintf("requested ack member %d", m.ThisMemberID()),
			durableWorkBound,
			sessionPump(transport, session, func() bool {
				ca := m.Agent()
				return ca.ExpectedServiceAckID() == 2 &&
					ca.PendingServiceAckWaits() == 0 &&
					agent.NextAckID() == 2
			}),
		)
		requireNoServiceFailure(t, sc, i, " after request")
	}

	require.NoError(t, session.Close())
	const closeBound = 30 * time.Second
	requireServiceAwaitTick(
		t, sc,
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
			for _, mem := range sc.members {
				if !mem.rec.holdsClose(wantSession) {
					return false
				}
			}
			return true
		},
	)

	for i, mem := range sc.members {
		if mem.checked.violation != nil {
			require.NoError(t, mem.checked.violation,
				"commit bound member %d:\n%s",
				set.Module(i).ThisMemberID(), sc.dump())
		}
	}
	requireNoServiceFailures(t, sc, " final")
}
