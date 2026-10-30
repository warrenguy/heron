//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/service"
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

// nilSessionID marks a recordedMessage whose SessionMessage session was nil.
const nilSessionID int64 = -1

// recordedMessage is one SessionMessage callback with a copied payload.
type recordedMessage struct {
	sessionID int64
	payload   []byte
}

// recordedClose is one SessionClose callback.
type recordedClose struct {
	sessionID int64
	reason    clusterwire.CloseReason
}

// sessionRecorder records ClusteredService callbacks. Invoker-only. No
// locking. Copies payload bytes. Callback slices are transient.
//
// With echo set, SessionMessage offers the payload back to its session and
// records the Offer result in echoResults. A close message does not echo.
//
// With closePayload set, a message with that payload and a non-nil session
// is a close message. It calls session.Close and records the result in
// closeResults.
type sessionRecorder struct {
	startCount int
	opens      []int64
	messages   []recordedMessage
	closes     []recordedClose
	order      []string

	echo        bool
	echoResults []int64

	closePayload []byte
	closeResults []bool
}

func (s *sessionRecorder) Start(service.Cluster) error {
	s.startCount++
	return nil
}

func (s *sessionRecorder) SessionOpen(session service.ClientSession, _ int64) {
	s.opens = append(s.opens, session.ID())
	s.order = append(s.order, "open")
}

func (s *sessionRecorder) SessionClose(
	session service.ClientSession,
	_ int64,
	reason clusterwire.CloseReason,
) {
	s.closes = append(s.closes, recordedClose{sessionID: session.ID(), reason: reason})
	s.order = append(s.order, "close")
}

// SessionMessage records the message. A close payload then closes the
// session. With echo set, it records echo result 0 for a nil session.
func (s *sessionRecorder) SessionMessage(
	session service.ClientSession,
	_ int64,
	payload []byte,
) {
	sessionID := nilSessionID
	if session != nil {
		sessionID = session.ID()
	}
	s.messages = append(s.messages, recordedMessage{
		sessionID: sessionID,
		payload:   bytes.Clone(payload),
	})
	s.order = append(s.order, "message")
	if s.closePayload != nil && session != nil && bytes.Equal(payload, s.closePayload) {
		s.closeResults = append(s.closeResults, session.Close())
		return
	}
	if !s.echo {
		return
	}
	var result int64
	if session != nil {
		result = session.Offer(payload)
	}
	s.echoResults = append(s.echoResults, result)
}

func (s *sessionRecorder) TimerEvent(_, _ int64) {}

func (s *sessionRecorder) holdsOpen(sessionID int64) bool {
	return slices.Contains(s.opens, sessionID)
}

func (s *sessionRecorder) holdsClose(sessionID int64) bool {
	return slices.ContainsFunc(s.closes, func(c recordedClose) bool {
		return c.sessionID == sessionID
	})
}

// holdsCloseWithReason reports a SessionClose for sessionID with reason.
func (s *sessionRecorder) holdsCloseWithReason(
	sessionID int64,
	reason clusterwire.CloseReason,
) bool {
	return slices.Contains(s.closes, recordedClose{sessionID: sessionID, reason: reason})
}

// holdsMessagesAfterOpen reports the session open, then exactly one message
// per payload in payload order, every message for sessionID.
func (s *sessionRecorder) holdsMessagesAfterOpen(sessionID int64, payloads ...[]byte) bool {
	if !s.holdsOpen(sessionID) || !s.messagesAre(sessionID, payloads) {
		return false
	}
	want := append([]string{"open"}, slices.Repeat([]string{"message"}, len(payloads))...)
	return slices.Equal(s.order, want)
}

func (s *sessionRecorder) holdsMessages(sessionID int64, payloads ...[]byte) bool {
	if s.holdsOpen(sessionID) || !s.messagesAre(sessionID, payloads) {
		return false
	}
	return slices.Equal(s.order, slices.Repeat([]string{"message"}, len(payloads)))
}

func (s *sessionRecorder) messagesAre(sessionID int64, payloads [][]byte) bool {
	if len(s.messages) != len(payloads) {
		return false
	}
	for i, msg := range s.messages {
		if msg.sessionID != sessionID || !bytes.Equal(msg.payload, payloads[i]) {
			return false
		}
	}
	return true
}

// serviceMember is one member's recorder, agent, bound-check wrapper, and
// optional service container. A stop path may Close the container before test
// cleanup.
type serviceMember struct {
	rec       *sessionRecorder
	agent     *service.Agent
	checked   *boundCheckedService
	container *service.Container
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
			"responseAdds=%d responsePubs=%d echoResults=%v serviceFail=%v ackFail=%v "+
			"violation=%v\n",
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
		mem.agent.PendingResponsePublicationAdds(),
		mem.agent.ResponsePublicationCount(),
		mem.rec.echoResults,
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

// requireServiceAwaitTick runs TickUntil and dumps set plus service state on
// timeout. The helper logs elapsed time on success. detail, when present,
// prefixes the timeout dump.
func requireServiceAwaitTick(
	t *testing.T,
	sc serviceCluster,
	label string,
	bound time.Duration,
	pred func() bool,
	detail ...func() string,
) {
	t.Helper()

	start := time.Now()
	done := sc.set.TickUntil(bound, pred)
	if !done {
		msg := sc.dump()
		if len(detail) > 0 && detail[0] != nil {
			msg = detail[0]() + "\n" + msg
		}
		require.FailNow(
			t,
			label+" did not complete",
			"after %s (bound %s):\n%s",
			time.Since(start),
			bound,
			msg,
		)
	}
	t.Logf("%s elapsed=%s", label, time.Since(start))
}

func requireMessagesAfterOpen(
	t *testing.T,
	sc serviceCluster,
	transport *aeron.Client,
	session *client.AeronCluster,
	label string,
	sessionID int64,
	payloads ...[]byte,
) {
	t.Helper()

	requireServiceAwaitTick(
		t, sc,
		label,
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			for _, mem := range sc.members {
				if !mem.rec.holdsMessagesAfterOpen(sessionID, payloads...) {
					return false
				}
			}
			return true
		}),
	)
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
	return startServiceCluster(
		t,
		harness.ClusterServiceAckPortBase,
		harness.ClusterServiceAckLogEndpoint,
		amendServiceMember,
	)
}

// startServiceCluster starts three members with one session recorder
// container per member. Each container connects its own archive client on the
// default service streams. Containers open and attach before the first tick.
// The helper then awaits log images and election.
func startServiceCluster(
	t *testing.T,
	portBase int,
	logEndpoint string,
	amend func(cluster.Config) cluster.Config,
) serviceCluster {
	t.Helper()
	return startWrappedServiceCluster(t, portBase, logEndpoint, amend, nil)
}

// wrapRecorder returns the clustered service a container runs around one
// session recorder. Nil runs the recorder itself.
type wrapRecorder func(rec *sessionRecorder) service.ClusteredService

// startWrappedServiceCluster is startServiceCluster with each recorder
// wrapped by wrap.
func startWrappedServiceCluster(
	t *testing.T,
	portBase int,
	logEndpoint string,
	amend func(cluster.Config) cluster.Config,
	wrap wrapRecorder,
) serviceCluster {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          3,
		PortBase:             portBase,
		LogMulticastEndpoint: logEndpoint,
		AmendMemberConfig:    amend,
	})
	require.Len(t, set.Modules(), 3)

	members := make([]serviceMember, 3)
	for i := range 3 {
		members[i] = openServiceMember(t, set, i, wrap, fmt.Sprintf("service container %d", i))
	}

	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)

	return serviceCluster{set: set, members: members}
}

func requireServicesJoined(t *testing.T, sc serviceCluster) {
	t.Helper()

	requireServiceAwaitTick(
		t, sc,
		"join settle",
		durableWorkBound,
		func() bool {
			for i, m := range sc.set.Modules() {
				agent := sc.members[i].agent
				ca := m.Agent()
				if !agent.IsLogBound() || agent.HasPendingJoin() || agent.NextAckID() != 2 {
					return false
				}
				if ca.ExpectedServiceAckID() != 2 || ca.PendingServiceAckWaits() != 0 {
					return false
				}
			}
			return true
		},
	)
}

// TestClusterService_deliversCommittedLogAndAcksOnRequest asserts a committed
// session message reaches every attached service. Each service echoes the
// message to its session. The client receives exactly one echo, from the
// leader. Each follower echo returns MockedOffer. A requested service ack then
// completes on every member. A second session then sends a message on which
// every service closes that session. Every module and service drops the
// session with SERVICE_ACTION. The client gets SessionEvent CLOSED.
func TestClusterService_deliversCommittedLogAndAcksOnRequest(t *testing.T) {
	t.Parallel()

	sc := openServiceCluster(t)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	for _, mem := range sc.members {
		mem.rec.echo = true
	}

	requireServicesJoined(t, sc)
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

	var echoes []recordedMessage
	egress := &client.EgressHandlers{
		MessageBytesFunc: func(clusterSessionID, _ int64, payload []byte, _ *aeron.Header) {
			echoes = append(echoes, recordedMessage{
				sessionID: clusterSessionID,
				payload:   bytes.Clone(payload),
			})
		},
	}
	transport, session := connectServiceSessionWithEgress(t, sc, leader, egress)
	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	wantSession := session.ClusterSessionID()
	require.Equal(t, 1, leader.Agent().OpenSessionCount(), sc.dump())

	payload := []byte(serviceMessagePayload)
	offerServiceMessage(t, sc, session, payload)
	requireMessagesAfterOpen(
		t, sc, transport, session,
		"session message delivered",
		wantSession,
		payload,
	)

	requireServiceAwaitTick(
		t, sc,
		"echo received",
		durableWorkBound,
		sessionPump(transport, session, func() bool { return len(echoes) > 0 }),
	)
	wantEcho := []recordedMessage{{sessionID: wantSession, payload: payload}}
	require.Equal(t, wantEcho, echoes, sc.dump())
	for i, m := range set.Modules() {
		results := sc.members[i].rec.echoResults
		require.Len(t, results, 1, "echo results member %d:\n%s", m.ThisMemberID(), sc.dump())
		if m.IsLeader() {
			require.Positive(t, results[0], "leader echo member %d:\n%s", m.ThisMemberID(), sc.dump())
			continue
		}
		require.Equal(t, service.MockedOffer, results[0],
			"follower echo member %d:\n%s", m.ThisMemberID(), sc.dump())
	}

	for i, m := range set.Modules() {
		if got := sc.members[i].agent.NextAckID(); got != 2 {
			require.Equal(t, int64(2), got,
				"NextAckID after message member %d:\n%s",
				m.ThisMemberID(), sc.dump())
		}
		if got := m.Agent().ExpectedServiceAckID(); got != 2 {
			require.Equal(t, int64(2), got,
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
				return ca.ExpectedServiceAckID() == 3 &&
					ca.PendingServiceAckWaits() == 0 &&
					agent.NextAckID() == 3
			}),
		)
		requireNoServiceFailure(t, sc, i, " after request")
	}
	require.Equal(t, wantEcho, echoes, "echoes after service acks:\n%s", sc.dump())

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

	requireServiceCloseSession(t, sc, leader)

	for i, mem := range sc.members {
		if mem.checked.violation != nil {
			require.NoError(t, mem.checked.violation,
				"commit bound member %d:\n%s",
				set.Module(i).ThisMemberID(), sc.dump())
		}
	}
	requireNoServiceFailures(t, sc, " final")

	// Container Close closes the owned archive client. The archive client does
	// not own the transport client.
	for i, mem := range sc.members {
		archiveClient := mem.container.ArchiveClient()
		require.NotNil(t, archiveClient, "member %d", i)
		require.False(t, archiveClient.IsClosed(), "member %d", i)
		archiveCfg := archiveClient.Config()
		require.False(t, archiveCfg.OwnsAeronClient(), "member %d", i)
		require.Equal(t, int32(121), archiveCfg.EffectiveControlResponseStreamID(), "member %d", i)

		require.NoError(t, mem.container.Close())
		require.True(t, archiveClient.IsClosed(), "member %d", i)
		require.Nil(t, mem.container.ArchiveClient(), "member %d", i)
	}
}

const (
	serviceSecondPayload = "service-second-message"
	serviceThirdPayload  = "service-third-message"
	serviceClosePayload  = "service-close-session"
)

// requireServiceCloseSession connects a second session to leader. It offers
// serviceClosePayload, on which every service closes that session. It then
// awaits the drain on every module and service, and the client close.
//
// Every service close request must return true, followers included.
func requireServiceCloseSession(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
) {
	t.Helper()

	closePayload := []byte(serviceClosePayload)
	for _, mem := range sc.members {
		mem.rec.closePayload = closePayload
	}
	var closedDetails []string
	egress := &client.EgressHandlers{SessionEventFunc: closedDetailHandler(&closedDetails)}
	transport, session := connectServiceSessionWithEgress(t, sc, leader, egress)
	sessionID := session.ClusterSessionID()
	offerServiceMessage(t, sc, session, closePayload)

	wantClosed := []string{"SERVICE_ACTION"}
	requireServiceAwaitTick(
		t, sc,
		"service close drained",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return servicesClosedSession(sc, -1, sessionID) &&
				slices.Equal(closedDetails, wantClosed) && session.IsClosed()
		}),
		func() string { return fmt.Sprintf("client closed events=%v", closedDetails) },
	)

	for i, m := range sc.set.Modules() {
		require.Equal(t, []bool{true}, sc.members[i].rec.closeResults,
			"close results member %d:\n%s", m.ThisMemberID(), sc.dump())
	}
	requireNoServiceFailures(t, sc, " after service close")
}

// closedDetailHandler returns a SessionEventFunc that appends the detail of
// each EventCodeClosed event to details.
func closedDetailHandler(
	details *[]string,
) func(int64, int64, int64, int32, client.EventCode, []byte) {
	return func(
		_, _, _ int64,
		_ int32,
		code client.EventCode,
		detail []byte,
	) {
		if code == client.EventCodeClosed {
			*details = append(*details, string(detail))
		}
	}
}

// servicesClosedSession reports whether every module other than skipIndex
// drained its sessions and its service recorded a SERVICE_ACTION close of
// sessionID.
func servicesClosedSession(sc serviceCluster, skipIndex int, sessionID int64) bool {
	for i, m := range sc.set.Modules() {
		if i == skipIndex || m == nil {
			continue
		}
		ca := m.Agent()
		if ca.OpenSessionCount() != 0 ||
			ca.PendingSessionCount() != 0 ||
			ca.ClosingSessionCount() != 0 {
			return false
		}
		rec := sc.members[i].rec
		if !rec.holdsCloseWithReason(sessionID, clusterwire.CloseReason_SERVICE_ACTION) {
			return false
		}
	}
	return true
}

// connectServiceSession is connectServiceSessionWithEgress with no egress
// handlers.
func connectServiceSession(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
) (*aeron.Client, *client.AeronCluster) {
	t.Helper()
	return connectServiceSessionWithEgress(t, sc, leader, &client.EgressHandlers{})
}

// connectServiceSessionWithEgress connects one session to leader with egress
// as the client egress handler. It awaits the open on every attached service
// and no response publication add in flight on any service.
//
// Ownership: the test owns Close on the transport and the session.
func connectServiceSessionWithEgress(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	egress client.EgressHandler,
) (*aeron.Client, *client.AeronCluster) {
	t.Helper()

	transport, session := openSessionWithEgress(t, sc.set, leader, egress)
	sessionID := session.ClusterSessionID()

	requireServiceAwaitTick(
		t, sc,
		"session open committed",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			for _, mem := range sc.members {
				if !mem.rec.holdsOpen(sessionID) || mem.agent.PendingResponsePublicationAdds() != 0 {
					return false
				}
			}
			return true
		}),
	)
	return transport, session
}

// offerServiceMessage offers payload on session until the ingress accepts it.
func offerServiceMessage(
	t *testing.T,
	sc serviceCluster,
	session *client.AeronCluster,
	payload []byte,
) {
	t.Helper()

	offerTransferSessionMessage(t, sc.set, &transferPump{session: session}, payload, sc.dump)
}

// openServiceMember opens one session recorder container on the client of the
// module at index, wrapped by wrap. It attaches the container to the set.
func openServiceMember(
	t *testing.T,
	set *harness.ClusterMemberSet,
	index int,
	wrap wrapRecorder,
	label string,
) serviceMember {
	t.Helper()

	rec := &sessionRecorder{}
	var app service.ClusteredService = rec
	if wrap != nil {
		app = wrap(rec)
	}
	module := set.Module(index)
	container, err := service.Open(service.Config{
		Aeron:             module.Client(),
		Service:           app,
		CommittedPosition: module.CommitPosition,
		Archive:           &archive.Config{},
		Recovery:          &service.RecoveryConfig{},
	})
	require.NoError(t, err)
	harness.CleanupClose(t, label, container.Close)

	agent := container.Agent()
	checked := &boundCheckedService{agent: agent, module: module}
	set.AttachService(t, index, checked)
	return serviceMember{
		rec:       rec,
		agent:     agent,
		checked:   checked,
		container: container,
	}
}

// restartServiceMember stops the member at index and closes its service
// container. It restarts the member, then opens and attaches a fresh session
// recorder container, wrapped by wrap, on the new module's client.
func restartServiceMember(t *testing.T, sc serviceCluster, index int, wrap wrapRecorder) {
	t.Helper()

	require.NoError(t, sc.set.StopMember(index))
	closeServiceTransferContainer(t, sc, index)
	require.NoError(t, sc.set.RestartMember(index))
	requireWarmRestartOpen(t, sc.set, index, "restarted member")
	sc.members[index] = openServiceMember(
		t, sc.set, index, wrap, fmt.Sprintf("restarted service container %d", index),
	)
}

// requireRestoredSession asserts agent loaded a snapshot at actionPosition
// that holds sessionID, and rec saw no SessionOpen for it. Call it before
// the next tick. A tick can move LogPosition past the loaded position.
// Returns the restored session.
func requireRestoredSession(
	t *testing.T,
	agent *service.Agent,
	rec *sessionRecorder,
	actionPosition, sessionID int64,
	dump func() string,
) service.ClientSession {
	t.Helper()

	require.Equal(t, actionPosition, agent.LogPosition(), "log position:\n%s", dump())
	restored, ok := agent.ClientSession(sessionID)
	require.True(t, ok, "restored session %d:\n%s", sessionID, dump())
	require.Equal(t, sessionID, restored.ID(), dump())
	require.False(t, rec.holdsOpen(sessionID), "restored session open:\n%s", dump())
	return restored
}

// TestClusterService_restartedMemberServiceConsumes asserts a fresh service
// container on a restarted follower replays the committed log, then consumes
// a live message.
func TestClusterService_restartedMemberServiceConsumes(t *testing.T) {
	t.Parallel()

	sc := startServiceCluster(
		t,
		harness.ClusterServiceRestartPortBase,
		harness.ClusterServiceRestartLogEndpoint,
		amendServiceMember,
	)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	requireServicesJoined(t, sc)

	transport, session := connectServiceSession(t, sc, leader)
	sessionID := session.ClusterSessionID()
	first := []byte(serviceMessagePayload)
	offerServiceMessage(t, sc, session, first)
	requireMessagesAfterOpen(
		t, sc, transport, session,
		"first message delivered",
		sessionID,
		first,
	)

	followerIndex := firstNonLeaderIndex(set, leader.ThisMemberID())
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", sc.dump())
	restartServiceMember(t, sc, followerIndex, nil)
	requireServiceAwaitTick(
		t, sc,
		"restarted member election",
		durableElectBound,
		sessionPump(transport, session, func() bool {
			requireNoServiceFailure(t, sc, followerIndex, " during restart")
			return set.Module(followerIndex).ElectionComplete()
		}),
	)

	second := []byte(serviceSecondPayload)
	offerServiceMessage(t, sc, session, second)
	restarted := set.Module(followerIndex)
	requireServiceAwaitTick(
		t, sc,
		"restarted service consumes",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			requireNoServiceFailure(t, sc, followerIndex, " during restart")
			rec := sc.members[followerIndex].rec
			return rec.holdsMessagesAfterOpen(sessionID, first, second) &&
				restarted.Agent().PendingServiceAckWaits() == 0
		}),
	)
	requireNoServiceFailure(t, sc, followerIndex, " after restart")
	require.NoError(t, sc.members[followerIndex].checked.violation, sc.dump())
}

// TestClusterService_restartedMemberServiceConsumesFromSnapshot asserts a
// restarted follower service loads its snapshot. The service consumes only
// the log after the snapshot position.
func TestClusterService_restartedMemberServiceConsumesFromSnapshot(t *testing.T) {
	t.Parallel()

	sc := startServiceCluster(
		t,
		harness.ClusterServiceSnapshotRestartPortBase,
		harness.ClusterServiceSnapshotRestartLogEndpoint,
		amendServiceMember,
	)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	requireServicesJoined(t, sc)

	transport, session := connectServiceSession(t, sc, leader)
	sessionID := session.ClusterSessionID()
	first := []byte(serviceMessagePayload)
	offerServiceMessage(t, sc, session, first)
	requireMessagesAfterOpen(
		t, sc, transport, session,
		"first message delivered",
		sessionID,
		first,
	)

	_, actionPosition := requestSnapshotAction(
		t, sc, leader, &transferPump{transport: transport, session: session}, "snapshot",
	)
	requireServiceAwaitTick(
		t, sc,
		"snapshot on every member",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			requireNoServiceFailures(t, sc, " during snapshot")
			return snapshotTakenOnEveryMember(t, set, actionPosition, sc.dump)
		}),
	)

	second := []byte(serviceSecondPayload)
	offerServiceMessage(t, sc, session, second)
	requireMessagesAfterOpen(
		t, sc, transport, session,
		"second message delivered",
		sessionID,
		first, second,
	)

	followerIndex := firstNonLeaderIndex(set, leader.ThisMemberID())
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", sc.dump())
	restartServiceMember(t, sc, followerIndex, nil)

	restarted := sc.members[followerIndex]
	requireRestoredSession(t, restarted.agent, restarted.rec, actionPosition, sessionID, sc.dump)

	third := []byte(serviceThirdPayload)
	offerServiceMessage(t, sc, session, third)
	restartedModule := set.Module(followerIndex)
	requireServiceAwaitTick(
		t, sc,
		"restarted service consumes from snapshot",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			requireNoServiceFailure(t, sc, followerIndex, " during restart")
			return restarted.rec.holdsMessages(sessionID, second, third) &&
				restartedModule.Agent().PendingServiceAckWaits() == 0
		}),
	)
	requireNoServiceFailure(t, sc, followerIndex, " after restart")
	require.NoError(t, restarted.checked.violation, sc.dump())
}

const (
	rebuildNearTimerID int64 = 1
	rebuildFarTimerID  int64 = 2
	rebuildNearDelay         = int64(100 * time.Millisecond)
	rebuildFarDelay          = int64(time.Hour)
)

// timerScheduler wraps a session recorder. On its first session message it
// schedules a near timer and a far timer from the message timestamp. It
// stores each TimerEvent id. Invoker-only.
type timerScheduler struct {
	*sessionRecorder

	cluster          service.Cluster
	scheduled        bool
	scheduleFailures int
	timerEvents      []int64
}

func (s *timerScheduler) Start(c service.Cluster) error {
	s.cluster = c
	return s.sessionRecorder.Start(c)
}

func (s *timerScheduler) SessionMessage(
	session service.ClientSession,
	timestamp int64,
	payload []byte,
) {
	s.sessionRecorder.SessionMessage(session, timestamp, payload)
	if s.scheduled {
		return
	}
	s.scheduled = true
	if !s.cluster.ScheduleTimer(rebuildNearTimerID, timestamp+rebuildNearDelay) {
		s.scheduleFailures++
	}
	if !s.cluster.ScheduleTimer(rebuildFarTimerID, timestamp+rebuildFarDelay) {
		s.scheduleFailures++
	}
}

func (s *timerScheduler) TimerEvent(correlationID, _ int64) {
	s.timerEvents = append(s.timerEvents, correlationID)
}

// TestClusterService_restartRebuildsModuleState asserts a restarted follower
// rebuilds its module sessions and timers from the replayed log.
func TestClusterService_restartRebuildsModuleState(t *testing.T) {
	t.Parallel()

	schedulers := make(map[*sessionRecorder]*timerScheduler)
	wrap := func(rec *sessionRecorder) service.ClusteredService {
		s := &timerScheduler{sessionRecorder: rec}
		schedulers[rec] = s
		return s
	}
	sc := startWrappedServiceCluster(
		t,
		harness.ClusterServiceRebuildPortBase,
		harness.ClusterServiceRebuildLogEndpoint,
		amendServiceMember,
		wrap,
	)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	requireServicesJoined(t, sc)

	transport, session := connectServiceSession(t, sc, leader)
	offerServiceMessage(t, sc, session, []byte(serviceMessagePayload))

	// Every service reads the near TimerEvent, so every member committed it.
	requireServiceAwaitTick(
		t, sc,
		"near timer fired",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			for _, mem := range sc.members {
				s := schedulers[mem.rec]
				if !slices.Contains(s.timerEvents, rebuildNearTimerID) {
					return false
				}
			}
			return leader.Agent().ScheduledTimerCount() == 1
		}),
	)
	for _, mem := range sc.members {
		require.Zero(t, schedulers[mem.rec].scheduleFailures, sc.dump())
	}

	followerIndex := firstNonLeaderIndex(set, leader.ThisMemberID())
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", sc.dump())
	restartServiceMember(t, sc, followerIndex, wrap)
	restarted := set.Module(followerIndex)

	// Phase 1: the election replay learns the session.
	requireServiceAwaitTick(
		t, sc,
		"restarted member election",
		durableElectBound,
		sessionPump(transport, session, restarted.ElectionComplete),
	)
	require.Equal(t, 1, restarted.Agent().OpenSessionCount(), sc.dump())

	// Phase 2: the restarted service schedules both timers again. The replayed
	// near TimerEvent leaves only the far timer.
	target := leader.CommitPosition()
	requireServiceAwaitTick(
		t, sc,
		"restarted service at leader commit",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			requireNoServiceFailure(t, sc, followerIndex, " during restart")
			return sc.members[followerIndex].agent.LogPosition() >= target
		}),
	)
	for range 3 {
		set.Tick()
	}
	require.Zero(t, schedulers[sc.members[followerIndex].rec].scheduleFailures, sc.dump())
	require.Equal(t, int64(1), restarted.Agent().ScheduledTimerCount(), sc.dump())
	require.Zero(t, restarted.Agent().ExpiredTimerCount(), sc.dump())
	requireNoServiceFailure(t, sc, followerIndex, " after restart")
}
