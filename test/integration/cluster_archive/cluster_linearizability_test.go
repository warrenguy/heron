//go:build integration && archive_live

package cluster_archive_test

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/service"
	"github.com/warrenguy/luas/internal/linearizability"
	"github.com/warrenguy/luas/test/integration/harness"
)

const (
	linearizabilityMembers = 3
	linearizabilityClients = 3
	linearizabilityKeys    = 8

	// getMixCeiling and appendMixCeiling split a 0-99 draw: get, then append, then put.
	getMixCeiling    = 40
	appendMixCeiling = 85

	steadyReturnMin  = 100
	steadyReturnMax  = 200
	steadyReturnSpan = steadyReturnMax - steadyReturnMin + 1

	termReturnCount = 20
	minReturnedOps  = 480

	offerWait = 30 * time.Second
	replyWait = 10 * time.Second
	maxPace   = 5 * time.Millisecond

	checkBudget = 30 * time.Second
)

// keyValueService applies each logged request to its store. The leader sends
// the reply.
type keyValueService struct {
	*sessionRecorder

	store      *linearizability.Store
	reply      []byte
	replyFails int
	decodeErr  error
}

func newKeyValueService(rec *sessionRecorder) *keyValueService {
	return &keyValueService{
		sessionRecorder: rec,
		store:           linearizability.NewStore(),
	}
}

func (s *keyValueService) SessionMessage(
	session service.ClientSession,
	timestamp int64,
	payload []byte,
) {
	s.sessionRecorder.SessionMessage(session, timestamp, payload)

	if s.decodeErr != nil {
		return
	}

	opID, in, err := linearizability.DecodeRequest(payload)
	if err != nil {
		s.decodeErr = err
		return
	}

	out := s.store.Apply(in)

	if session == nil {
		return
	}

	s.reply = linearizability.EncodeReply(s.reply[:0], opID, out)
	if session.Offer(s.reply) < 0 {
		s.replyFails++
	}
}

// historyClient is one session and its single in-flight operation.
type historyClient struct {
	index         int
	session       *client.AeronCluster
	rng           *rand.Rand
	lastKeepalive time.Time

	openID   int
	offered  bool
	request  []byte
	offerBy  time.Time
	replyBy  time.Time
	nextCall time.Time

	closed       bool
	hasEvent     bool
	storedCode   client.EventCode
	storedDetail string
	replyErr     error
	termReturns  map[int64]int
}

// linearizabilityRun drives the key-value workload and the history.
type linearizabilityRun struct {
	t        testing.TB
	rng      *rand.Rand
	seed     uint64
	sc       serviceCluster
	services []*keyValueService
	history  *linearizability.History
	nextOpID int
	pump     *transferPump
	clients  []*historyClient

	callsPaused bool
	checked     bool
}

func newLinearizabilityRun(t testing.TB, seed uint64) *linearizabilityRun {
	t.Helper()

	started := time.Now()
	run := &linearizabilityRun{
		t:           t,
		rng:         rand.New(rand.NewPCG(seed, 0)),
		seed:        seed,
		callsPaused: true,
		history: linearizability.NewHistory(func() int64 {
			return time.Since(started).Nanoseconds()
		}),
	}
	t.Cleanup(run.visualizePartial)

	return run
}

// linearizabilitySeed reads LUAS_LINEARIZABILITY_SEED. linearizabilitySeed draws
// a seed from crypto/rand when the variable is empty.
func linearizabilitySeed(t testing.TB) uint64 {
	t.Helper()

	text := os.Getenv("LUAS_LINEARIZABILITY_SEED")
	if text == "" {
		var buf [8]byte
		_, err := cryptorand.Read(buf[:])
		require.NoError(t, err)

		return binary.LittleEndian.Uint64(buf[:])
	}

	seed, err := strconv.ParseUint(text, 10, 64)
	require.NoError(t, err)

	return seed
}

func (r *linearizabilityRun) open() {
	r.t.Helper()

	r.sc = startWrappedServiceCluster(
		r.t,
		linearizabilityMembers,
		harness.ClusterLinearizabilityPortBase,
		harness.ClusterLinearizabilityLogEndpoint,
		amendServiceTransferMember,
		r.appendService,
	)
	requireServicesJoined(r.t, r.sc)
	requireSingleLeader(r.t, r.sc.set, "")
}

func (r *linearizabilityRun) appendService(rec *sessionRecorder) service.ClusteredService {
	svc := newKeyValueService(rec)
	r.services = append(r.services, svc)

	return svc
}

func (r *linearizabilityRun) wrapService(index int) wrapRecorder {
	return func(rec *sessionRecorder) service.ClusteredService {
		svc := newKeyValueService(rec)
		r.services[index] = svc

		return svc
	}
}

func (r *linearizabilityRun) connectClients() {
	r.t.Helper()

	leader := requireSingleLeader(r.t, r.sc.set, "")
	transport := startSessionClientTransport(r.t)
	r.pump = &transferPump{transport: transport}
	startTransferClientKeepAlive(r.t, r.pump)

	ports := harness.FreeUDPPorts(r.t, linearizabilityClients)

	for i, port := range ports {
		r.connectClient(leader, i, port)
	}

	r.runUntil(durableWorkBound, "sessions open", r.hasOpenSessions)
	r.callsPaused = false
}

func (r *linearizabilityRun) connectClient(leader *cluster.ConsensusModule, index, port int) {
	r.t.Helper()

	clientRun := &historyClient{
		index:       index,
		openID:      -1,
		rng:         rand.New(rand.NewPCG(r.seed, uint64(index)+1)),
		termReturns: make(map[int64]int),
	}
	result, _, err := connectTransferSessionWithEgress(
		r.t,
		r.sc.set,
		leader,
		r.pump,
		[]byte(sessionSmokeCredentials),
		port,
		clientRun.handler(r),
		r.sc.dump,
	)
	require.NoError(r.t, err, "client %d connect:\n%s", index, r.sc.dump())

	session, err := client.NewAeronCluster(result)
	require.NoError(r.t, err, "client %d session:\n%s", index, r.sc.dump())
	harness.CleanupClose(r.t, "session", session.Close)

	r.pump.mu.Lock()
	_ = session.SendKeepAlive()
	r.pump.mu.Unlock()

	clientRun.session = session
	clientRun.lastKeepalive = time.Now()
	r.clients = append(r.clients, clientRun)
}

func (c *historyClient) handler(r *linearizabilityRun) *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageFunc: func(
			_ int64,
			_ int64,
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			_ *aeron.Header,
		) {
			c.handleMessage(r, buffer, offset, length)
		},
		NewLeaderFunc: func(_, _ int64, _ int32, _ []byte) {
			c.handleNewLeader(r)
		},
		SessionEventFunc: func(
			_, _, _ int64,
			_ int32,
			code client.EventCode,
			detail []byte,
		) {
			c.handleSessionEvent(r, code, detail)
		},
	}
}

func (c *historyClient) handleMessage(
	r *linearizabilityRun,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) {
	raw := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(raw)-length {
		c.replyErr = fmt.Errorf(
			"client %d reply span offset %d length %d",
			c.index, offset, length,
		)

		return
	}

	header, ok := codecs.TryDecodeSessionMessageHeaderAt(raw, offset-client.SessionHeaderLength())
	if !ok {
		c.replyErr = fmt.Errorf("client %d session header", c.index)
		return
	}

	opID, out, err := linearizability.DecodeReply(raw[offset : offset+length])
	if err != nil {
		c.replyErr = fmt.Errorf("client %d reply: %w", c.index, err)
		return
	}

	if err := r.history.Return(int(opID), out, header.LeadershipTermID); err != nil {
		c.replyErr = fmt.Errorf("client %d return %d: %w", c.index, opID, err)
		return
	}

	if c.openID != int(opID) {
		return
	}

	c.termReturns[header.LeadershipTermID]++
	c.clearOpen()
	c.pace()
}

func (c *historyClient) handleNewLeader(r *linearizabilityRun) {
	if c.offered {
		c.dropOpen(r)
	}
}

func (c *historyClient) handleSessionEvent(
	r *linearizabilityRun,
	code client.EventCode,
	detail []byte,
) {
	if code == client.EventCodeClosed {
		c.closed = true
		c.storedDetail = string(detail)
		c.dropOpen(r)

		return
	}

	if isFailingSessionEvent(c.storedCode) {
		return
	}

	c.hasEvent = true
	c.storedCode = code
	c.storedDetail = string(detail)
}

func (r *linearizabilityRun) hasOpenSessions() bool {
	for _, c := range r.clients {
		sessionID := c.session.ClusterSessionID()
		if sessionID < 0 {
			return false
		}

		for _, mem := range r.sc.members {
			if mem.rec == nil || mem.agent == nil {
				return false
			}

			if !mem.rec.holdsOpen(sessionID) || mem.agent.PendingResponsePublicationAdds() != 0 {
				return false
			}
		}
	}

	return true
}

func (r *linearizabilityRun) steady() {
	r.t.Helper()

	extra := steadyReturnMin + r.rng.IntN(steadyReturnSpan)
	target := r.history.Counts().Returned.Total() + extra
	r.t.Logf("steady target %d", target)
	r.runUntil(durableWorkBound, "steady returns", func() bool {
		return r.history.Counts().Returned.Total() >= target
	})
}

func (r *linearizabilityRun) faultDown(crash bool) {
	r.t.Helper()

	leader := requireSingleLeader(r.t, r.sc.set, "")
	preTerm := leader.LeadershipTermID()
	index := int(leader.ThisMemberID())

	if crash {
		r.t.Logf("crash leader index %d term %d", index, preTerm)
		err := r.sc.set.CrashMember(index)
		var terminal *archive.ControlTerminalError
		require.ErrorAs(r.t, err, &terminal, "CrashMember:\n%s", r.sc.dump())
	} else {
		r.t.Logf("stop leader index %d term %d", index, preTerm)
		require.NoError(r.t, r.sc.set.StopMember(index), "StopMember:\n%s", r.sc.dump())
	}

	closeServiceTransferContainer(r.t, r.sc, index)
	r.awaitNewTermReturns(index, preTerm)
	r.recoverPaused(
		"services at commit",
		func() { restartServiceMember(r.t, r.sc, index, r.wrapService(index)) },
		r.areServicesCaughtUp,
	)
}

func (r *linearizabilityRun) faultHold() {
	r.t.Helper()

	leader := requireSingleLeader(r.t, r.sc.set, "")
	preTerm := leader.LeadershipTermID()
	index := int(leader.ThisMemberID())
	r.t.Logf("hold leader index %d term %d", index, preTerm)
	r.sc.set.HoldAgent(index)
	term := r.awaitNewTermReturns(index, preTerm)
	r.recoverPaused(
		"released follower at commit",
		func() { r.sc.set.ReleaseAgent(index) },
		func(floor int64) bool { return r.isReleasedCaughtUp(index, term, floor) },
	)
}

// recoverPaused runs restore, then awaits caughtUp at the live commit floor.
// New calls stay paused until caughtUp holds.
func (r *linearizabilityRun) recoverPaused(
	label string,
	restore func(),
	caughtUp func(floor int64) bool,
) {
	r.t.Helper()

	r.callsPaused = true
	floor := r.liveCommitFloor()
	require.Positive(r.t, floor, "commit floor:\n%s", r.sc.dump())
	restore()
	r.runUntil(durableWorkBound, label, func() bool {
		return caughtUp(floor)
	})
	r.callsPaused = false
}

func (r *linearizabilityRun) awaitNewTermReturns(stoppedIndex int, preTerm int64) int64 {
	r.t.Helper()

	var term int64
	r.runUntil(durableReElectBound, "new term returns", func() bool {
		leader := r.successorLeader(stoppedIndex, preTerm)
		if leader == nil || !leader.ElectionComplete() {
			return false
		}

		term = leader.LeadershipTermID()

		for _, c := range r.clients {
			if c.termReturns[term] < termReturnCount {
				return false
			}
		}

		return true
	})

	return term
}

func (r *linearizabilityRun) successorLeader(
	stoppedIndex int,
	preTerm int64,
) *cluster.ConsensusModule {
	var found *cluster.ConsensusModule

	for i, module := range r.sc.set.Modules() {
		if module == nil || i == stoppedIndex || !module.IsLeader() {
			continue
		}

		if module.LeadershipTermID() <= preTerm {
			continue
		}

		if found != nil {
			return nil
		}

		found = module
	}

	return found
}

func (r *linearizabilityRun) liveCommitFloor() int64 {
	var floor int64

	for _, module := range r.sc.set.Modules() {
		if module == nil {
			continue
		}

		floor = max(floor, module.CommitPosition())
	}

	return floor
}

func hasServiceReachedCommit(
	agent *service.Agent,
	module *cluster.ConsensusModule,
	floor int64,
) bool {
	if module == nil || module.CommitPosition() < floor {
		return false
	}

	if agent == nil || !agent.IsLogBound() || agent.HasPendingJoin() {
		return false
	}

	return agent.LogPosition() >= module.CommitPosition()
}

func (r *linearizabilityRun) areServicesCaughtUp(floor int64) bool {
	for i, module := range r.sc.set.Modules() {
		if !hasServiceReachedCommit(r.sc.members[i].agent, module, floor) {
			return false
		}
	}

	return true
}

func (r *linearizabilityRun) isReleasedCaughtUp(index int, term, floor int64) bool {
	module := r.sc.set.Module(index)
	agent := r.sc.members[index].agent

	if module == nil || module.LeadershipTermID() != term || module.Role() != codecs.RoleFollower {
		return false
	}

	if agent == nil || agent.Role() != codecs.RoleFollower {
		return false
	}

	return hasServiceReachedCommit(agent, module, floor)
}

func (r *linearizabilityRun) drain() {
	r.t.Helper()

	r.callsPaused = true
	r.runUntil(durableWorkBound, "drain", func() bool {
		for _, c := range r.clients {
			if c.openID >= 0 {
				return false
			}
		}

		return true
	})
}

func (r *linearizabilityRun) check() {
	r.t.Helper()

	for _, c := range r.clients {
		require.False(r.t, c.session.IsClosed(), "client %d closed\n%s", c.index, r.sc.dump())
		require.Equal(r.t, -1, c.openID, "client %d still open", c.index)

		if c.hasEvent {
			r.t.Logf("client %d event %v detail=%q", c.index, c.storedCode, c.storedDetail)
		}
	}

	for i, svc := range r.services {
		if svc == nil {
			continue
		}

		require.NoError(r.t, svc.decodeErr, "service %d", i)
	}

	counts := r.history.Counts()
	r.t.Logf(
		"returned=%d abandoned=%d discarded=%d late=%d replyFails=%d",
		counts.Returned.Total(),
		counts.Abandoned.Total(),
		counts.Discarded.Total(),
		counts.Late.Total(),
		r.replyFailTotal(),
	)
	require.GreaterOrEqual(r.t, counts.Returned.Total(), minReturnedOps, "returned operations")

	ops := r.history.Operations()
	require.True(r.t, hasNonEmptyGet(ops), "returned get with a non-empty value")
	r.checked = true
	linearizability.RequireLinearizable(r.t, ops, checkBudget)

	mutated := mutateOneRead(r.t, ops)
	result := linearizability.Check(mutated, checkBudget)
	require.Equal(r.t, porcupine.Illegal, result, "mutated history")
}

func isFailingSessionEvent(code client.EventCode) bool {
	return code == client.EventCodeError ||
		code == client.EventCodeAuthenticationRejected
}

func (r *linearizabilityRun) replyFailTotal() int {
	total := 0

	for _, svc := range r.services {
		if svc != nil {
			total += svc.replyFails
		}
	}

	return total
}

func (r *linearizabilityRun) pollClients() {
	r.pump.mu.Lock()
	defer r.pump.mu.Unlock()

	r.pump.transport.DoWork()

	for _, c := range r.clients {
		c.session.PollEgress()
		sendKeepAliveIfDue(c.session, &c.lastKeepalive)
		r.failClient(c)
		c.advance(r, time.Now())
	}
}

func (r *linearizabilityRun) failClient(c *historyClient) {
	r.t.Helper()

	if c.replyErr != nil {
		require.NoError(
			r.t,
			c.replyErr,
			"client %d reply:\n%s\n%s",
			c.index,
			r.clientState(),
			r.sc.dump(),
		)
	}

	if c.closed {
		require.FailNow(
			r.t,
			"session closed",
			"client %d detail=%q\n%s\n%s",
			c.index,
			c.storedDetail,
			r.clientState(),
			r.sc.dump(),
		)
	}

	if !isFailingSessionEvent(c.storedCode) {
		return
	}

	require.FailNow(
		r.t,
		"client event",
		"client %d code=%v detail=%q\n%s\n%s",
		c.index,
		c.storedCode,
		c.storedDetail,
		r.clientState(),
		r.sc.dump(),
	)
}

func (r *linearizabilityRun) failServices() {
	r.t.Helper()

	failIfServiceTransferStall(r.t, r.sc, -1)

	for i, svc := range r.services {
		if r.sc.set.Module(i) == nil || svc == nil || svc.decodeErr == nil {
			continue
		}

		require.NoError(r.t, svc.decodeErr, "service %d decode:\n%s\n%s", i, r.clientState(), r.sc.dump())
	}
}

func (r *linearizabilityRun) runUntil(bound time.Duration, label string, pred func() bool) {
	r.t.Helper()

	requireServiceAwaitTick(r.t, r.sc, label, bound, func() bool {
		r.pollClients()
		r.failServices()
		return pred()
	}, r.clientState)
}

func (c *historyClient) advance(r *linearizabilityRun, now time.Time) {
	if c.openID >= 0 {
		c.advanceOpen(r, now)
		return
	}

	if r.callsPaused || now.Before(c.nextCall) {
		return
	}

	c.begin(r, now)
}

func (c *historyClient) advanceOpen(r *linearizabilityRun, now time.Time) {
	if !c.offered {
		if now.After(c.offerBy) {
			c.dropOpen(r)
			return
		}

		c.tryOffer(now)
		return
	}

	if now.After(c.replyBy) {
		c.dropOpen(r)
	}
}

func (c *historyClient) begin(r *linearizabilityRun, now time.Time) {
	opID := r.nextOpID
	in := c.drawInput(opID)
	id := r.history.Call(c.index, in)
	require.Equal(r.t, opID, id, "operation id")
	r.nextOpID++

	c.request = linearizability.EncodeRequest(c.request[:0], int64(id), in)
	c.openID = id
	c.offered = false
	c.offerBy = now.Add(offerWait)
	c.tryOffer(now)
}

func (c *historyClient) tryOffer(now time.Time) {
	if c.session.Offer(c.request) <= 0 {
		return
	}

	c.offered = true
	c.replyBy = now.Add(replyWait)
}

func (c *historyClient) dropOpen(r *linearizabilityRun) {
	if c.openID < 0 {
		return
	}

	if c.offered {
		r.history.Abandon(c.openID)
	} else {
		r.history.Discard(c.openID)
	}

	c.clearOpen()
	c.pace()
}

func (c *historyClient) clearOpen() {
	c.openID = -1
	c.offered = false
}

func (c *historyClient) pace() {
	wait := time.Duration(c.rng.IntN(int(maxPace) + 1))
	c.nextCall = time.Now().Add(wait)
}

func (c *historyClient) drawInput(opID int) linearizability.Input {
	key := "k" + strconv.Itoa(c.rng.IntN(linearizabilityKeys))
	token := strconv.Itoa(opID) + ","
	roll := c.rng.IntN(100)

	switch {
	case roll < getMixCeiling:
		return linearizability.Input{Kind: linearizability.OpGet, Key: key}
	case roll < appendMixCeiling:
		return linearizability.Input{Kind: linearizability.OpAppend, Key: key, Value: token}
	default:
		return linearizability.Input{Kind: linearizability.OpPut, Key: key, Value: token}
	}
}

func (r *linearizabilityRun) clientState() string {
	var b strings.Builder
	counts := r.history.Counts()
	fmt.Fprintf(
		&b,
		"seed=%d paused=%v returned=%d abandoned=%d discarded=%d late=%d\n",
		r.seed,
		r.callsPaused,
		counts.Returned.Total(),
		counts.Abandoned.Total(),
		counts.Discarded.Total(),
		counts.Late.Total(),
	)

	for _, c := range r.clients {
		fmt.Fprintf(
			&b,
			"client=%d open=%d offered=%v closed=%v state=%s term=%d event=%v detail=%q err=%v returns=%v\n",
			c.index,
			c.openID,
			c.offered,
			c.closed,
			c.session.SessionState(),
			c.session.LeadershipTermID(),
			c.storedCode,
			c.storedDetail,
			c.replyErr,
			c.termReturns,
		)
	}

	for i, svc := range r.services {
		if svc == nil {
			continue
		}

		fmt.Fprintf(&b, "service=%d decodeErr=%v replyFails=%d\n", i, svc.decodeErr, svc.replyFails)
	}

	return b.String()
}

func (r *linearizabilityRun) visualizePartial() {
	if !r.t.Failed() || r.checked {
		return
	}

	ops := r.history.Operations()
	result, info := porcupine.CheckOperationsVerbose(linearizability.Model(), ops, checkBudget)
	path := filepath.Join(r.t.ArtifactDir(), "linearizability.html")
	if err := porcupine.VisualizePath(linearizability.Model(), info, path); err != nil {
		r.t.Logf("partial history result %s visualization: %v", result, err)
		return
	}

	r.t.Logf("partial history result %s visualization %s", result, path)
}

func operationValue(op porcupine.Operation) (linearizability.Input, linearizability.Output, bool) {
	in, inOK := op.Input.(linearizability.Input)
	out, outOK := op.Output.(linearizability.Output)
	if !inOK || !outOK || out.Unknown {
		return linearizability.Input{}, linearizability.Output{}, false
	}

	return in, out, true
}

func hasNonEmptyGet(ops []porcupine.Operation) bool {
	for _, op := range ops {
		in, out, ok := operationValue(op)
		if ok && in.Kind == linearizability.OpGet && out.Value != "" {
			return true
		}
	}

	return false
}

func mutateOneRead(t testing.TB, ops []porcupine.Operation) []porcupine.Operation {
	t.Helper()

	cloned := slices.Clone(ops)

	for i, op := range cloned {
		in, out, ok := operationValue(op)
		if !ok {
			continue
		}

		readable := in.Kind == linearizability.OpAppend ||
			(in.Kind == linearizability.OpGet && out.Value != "")
		if !readable {
			continue
		}

		out.Value = "x"
		op.Output = out
		cloned[i] = op

		return cloned
	}

	require.FailNow(t, "mutate history", "no returned append or non-empty get")

	return nil
}

// TestClusterLinearizability_keyValueAcrossLeaderFaults asserts that get, put,
// and append replies stay linearizable across a leader stop, crash, and hold.
func TestClusterLinearizability_keyValueAcrossLeaderFaults(t *testing.T) {
	seed := linearizabilitySeed(t)
	t.Logf("seed=%d", seed)
	t.Parallel()

	run := newLinearizabilityRun(t, seed)
	run.open()
	run.connectClients()
	run.steady()
	run.faultDown(false)
	run.steady()
	run.faultDown(true)
	run.steady()
	run.faultHold()
	run.drain()
	run.check()
}
