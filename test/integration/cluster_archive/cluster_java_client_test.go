//go:build integration && archive_live

package cluster_archive_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	javaClientCredentials   = "heron-java-client-credentials"
	javaClientChallenge     = "heron-java-client-challenge"
	javaClientResponse      = "heron-java-client-response"
	javaClientWrongResponse = "heron-java-client-wrong-response"
	javaClientPrincipal     = "heron-java-client-principal"
	javaClientPayloadPrefix = "jc"
	javaClientRejectDetail  = "ERROR - session failed authentication"
	javaClientRejectClass   = "AuthenticationException"
)

type javaClientAuths struct {
	byID map[int32]*harness.ChallengeAuthenticator
}

func (a *javaClientAuths) amend(cfg cluster.Config) cluster.Config {
	auth := harness.NewChallengeAuthenticator(
		[]byte(javaClientCredentials),
		[]byte(javaClientChallenge),
		[]byte(javaClientResponse),
		[]byte(javaClientPrincipal),
	)
	a.byID[cfg.MemberID] = auth
	cfg.Authenticator = auth
	cfg.ServiceCount = 1
	cfg.SessionTimeoutNs = transferSessionTimeoutNs
	return cfg
}

// TestClusterJavaClient_followsLeaderStop asserts a Java client session on a
// Go cluster continues across a leader stop.
func TestClusterJavaClient_followsLeaderStop(t *testing.T) {
	t.Parallel()

	auths := &javaClientAuths{byID: map[int32]*harness.ChallengeAuthenticator{}}
	sc := startServiceCluster(
		t,
		harness.ClusterJavaClientPortBase,
		harness.ClusterJavaClientLogEndpoint,
		auths.amend,
	)
	requireServicesJoined(t, sc)
	leader := requireSingleLeader(t, sc.set, "")
	driver := harness.Start(t)
	endpoints := leader.Topology().IngressEndpoints()

	mainPort := harness.ClientEgressPort(harness.ClusterJavaClientPortBase)
	rejectPort := mainPort + 1
	prePos := leader.LogPublisher().Position()
	reject := harness.StartJavaClusterClient(t, javaClientConfig(
		driver.Dir,
		endpoints,
		javaClientEgressChannel(rejectPort),
		javaClientWrongResponse,
	))
	awaitJavaClientReject(t, sc, leader, reject, prePos)

	client := harness.StartJavaClusterClient(t, javaClientConfig(
		driver.Dir,
		endpoints,
		javaClientEgressChannel(mainPort),
		javaClientResponse,
	))
	events := &javaClientEvents{t: t, sc: sc, client: client}
	sessionID, term := awaitJavaClientConnect(t, sc, events, leader)
	auth := auths.byID[leader.ThisMemberID()]
	require.NotNil(t, auth, sc.dump())
	require.Equal(t, 2, auth.ChallengesSent(), "challenges\n%s\n%s", client.Dump(), sc.dump())
	require.Equal(t, 2, auth.ResponsesSeen(), "responses\n%s\n%s", client.Dump(), sc.dump())

	awaitJavaClientOpen(t, sc, events, leader, sessionID)
	awaitJavaClientMessages(t, sc, events, leader, -1, sessionID, 1, 3, 0, 0)

	preTerm := term
	preCommit := leader.CommitPosition()
	leaderIndex := int(leader.ThisMemberID())
	require.NoError(t, sc.set.StopMember(leaderIndex))
	closeServiceTransferContainer(t, sc, leaderIndex)
	requireServiceAwaitTick(
		t, sc,
		"re-elect after leader stop",
		durableReElectBound,
		func() bool {
			events.poll(false)
			return reElectionReady(sc.set, leaderIndex, preTerm)
		},
		events.client.Dump,
	)
	newLeader := requireReElectedLeader(t, sc.set, leaderIndex, preTerm)
	recoveryCommit := newLeader.CommitPosition()
	failIfCommitBelowPreStop(t, recoveryCommit, preCommit, recoveryCommit, sc.dump)

	awaitJavaClientNewLeader(t, sc, events, newLeader, sessionID, preTerm)
	awaitJavaClientMessages(
		t, sc, events, newLeader, leaderIndex, sessionID, 4, 6, preCommit, recoveryCommit,
	)

	client.RequestClose()
	awaitJavaClientClose(t, sc, events, newLeader, leaderIndex, sessionID, 6)
}

func javaClientEgressChannel(port int) string {
	return "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(port)
}

func javaClientConfig(
	aeronDir, endpoints, egress, response string,
) harness.JavaClusterClientConfig {
	return harness.JavaClusterClientConfig{
		AeronDir:          aeronDir,
		IngressChannel:    cluster.DefaultIngressChannel,
		IngressEndpoints:  endpoints,
		EgressChannel:     egress,
		Credentials:       javaClientCredentials,
		Challenge:         javaClientChallenge,
		ChallengeResponse: response,
		MessageTimeout:    transferMessageTimeout,
		NewLeaderTimeout:  transferNewLeaderTimeout,
		PayloadPrefix:     javaClientPayloadPrefix,
	}
}

func awaitJavaClientReject(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	client *harness.JavaClusterClient,
	prePos int64,
) {
	t.Helper()

	var sawChallenge bool
	var sawError bool
	requireServiceAwaitTick(t, sc, "reject client", durableWorkBound, func() bool {
		for {
			ev, ok := client.PollEvent()
			if !ok {
				break
			}
			switch ev.Kind {
			case "CHALLENGE":
				if ev.Fields["match"] != "true" {
					failJavaClient(t, sc, client, "reject challenge "+ev.Line)
				}
				sawChallenge = true
			case "ERROR":
				if ev.Fields["class"] != javaClientRejectClass ||
					ev.Fields["detail"] != javaClientRejectDetail {
					failJavaClient(t, sc, client, "reject error "+ev.Line)
				}
				sawError = true
			case "CONNECTED":
				failJavaClient(t, sc, client, "reject client connected "+ev.Line)
			default:
				failJavaClient(t, sc, client, "reject client event "+ev.Line)
			}
		}
		code, exited := client.ExitCode()
		if exited && (!sawChallenge || !sawError) {
			failJavaClient(t, sc, client, fmt.Sprintf(
				"reject client exit %d before challenge and error", code,
			))
		}
		if !sawChallenge || !sawError || !exited {
			return false
		}
		if code != 1 {
			failJavaClient(t, sc, client, fmt.Sprintf("reject client exit %d", code))
		}
		pos := leader.LogPublisher().Position()
		if pos != prePos {
			failJavaClient(t, sc, client, fmt.Sprintf("log position changed %d to %d", prePos, pos))
		}
		return areClusterSessionsIdle(sc.set)
	}, client.Dump)
}

func awaitJavaClientConnect(
	t *testing.T,
	sc serviceCluster,
	events *javaClientEvents,
	leader *cluster.ConsensusModule,
) (int64, int64) {
	t.Helper()

	var connected harness.JavaClusterClientEvent
	var sawChallenge bool
	requireServiceAwaitTick(t, sc, "java client connect", durableWorkBound, func() bool {
		events.poll(false)
		for _, ev := range events.seen {
			switch ev.Kind {
			case "CHALLENGE":
				if ev.Fields["match"] != "true" {
					events.fail("challenge " + ev.Line)
				}
				sawChallenge = true
			case "CONNECTED":
				connected = ev
			}
		}
		return sawChallenge && connected.Kind == "CONNECTED"
	}, events.client.Dump)
	sessionID := events.fieldInt(connected, "session")
	term := events.fieldInt(connected, "term")
	leaderID := events.fieldInt(connected, "leader")
	require.GreaterOrEqual(t, sessionID, int64(0), events.lineDump(connected.Line))
	require.Equal(t, leader.LeadershipTermID(), term, "term\n%s\n%s", events.client.Dump(), sc.dump())
	require.Equal(
		t,
		int64(leader.ThisMemberID()),
		leaderID,
		"leader\n%s\n%s",
		events.client.Dump(),
		sc.dump(),
	)
	return sessionID, term
}

func awaitJavaClientOpen(
	t *testing.T,
	sc serviceCluster,
	events *javaClientEvents,
	leader *cluster.ConsensusModule,
	sessionID int64,
) {
	t.Helper()

	requireServiceAwaitTick(t, sc, "session open committed", durableWorkBound, func() bool {
		events.poll(false)
		if leader.Agent().OpenSessionCount() != 1 {
			return false
		}
		for _, mem := range sc.members {
			if !mem.rec.holdsOpen(sessionID) {
				return false
			}
		}
		return true
	}, events.client.Dump)
}

func awaitJavaClientMessages(
	t *testing.T,
	sc serviceCluster,
	events *javaClientEvents,
	leader *cluster.ConsensusModule,
	skipIndex int,
	sessionID int64,
	first, last int,
	preCommit, recoveryCommit int64,
) {
	t.Helper()

	pre := leader.LogPublisher().Position()
	seenBefore := len(events.seen)
	events.client.Send(last - first + 1)
	payloads := javaClientPayloads(last)
	wantFirst := strconv.Itoa(first)
	wantLast := strconv.Itoa(last)
	var commitTarget int64
	requireServiceAwaitTick(t, sc, "messages "+wantFirst+"-"+wantLast, durableWorkBound, func() bool {
		events.poll(false)
		ev, ok := events.lastFrom("SENT", seenBefore)
		if !ok {
			return false
		}
		if ev.Fields["first"] != wantFirst || ev.Fields["last"] != wantLast {
			events.fail("SENT " + ev.Line)
		}
		failIfRecorderSequenceCorrupt(t, sc, skipIndex, sessionID, events.client.Dump, payloads...)
		failIfCommitBelowPreStop(t, leader.CommitPosition(), preCommit, recoveryCommit, sc.dump)
		if !survivorServicesHoldSequence(sc, skipIndex, sessionID, payloads...) {
			return false
		}
		if commitTarget == 0 {
			commitTarget = leader.LogPublisher().Position()
			if commitTarget <= pre {
				events.fail(fmt.Sprintf("log position %d did not advance past %d", commitTarget, pre))
			}
		}
		if preCommit != 0 && leader.CommitPosition() <= recoveryCommit {
			return false
		}
		return liveMembersReachCommit(sc.set, []int{skipIndex}, commitTarget)
	}, events.client.Dump)
}

func awaitJavaClientNewLeader(
	t *testing.T,
	sc serviceCluster,
	events *javaClientEvents,
	newLeader *cluster.ConsensusModule,
	sessionID, preTerm int64,
) {
	t.Helper()

	var found harness.JavaClusterClientEvent
	requireServiceAwaitTick(t, sc, "new leader", durableWorkBound, func() bool {
		events.poll(false)
		ev, ok := events.last("NEW_LEADER")
		if !ok {
			return false
		}
		found = ev
		return newLeader.Agent().OpenSessionCount() == 1
	}, events.client.Dump)
	dump := events.lineDump(found.Line)
	require.Equal(t, sessionID, events.fieldInt(found, "session"), dump)
	require.Greater(t, events.fieldInt(found, "term"), preTerm, dump)
	require.Equal(t, int64(newLeader.ThisMemberID()), events.fieldInt(found, "leader"), dump)
	require.Equal(t, newLeader.Topology().IngressEndpoints(), found.Fields["endpoints"], dump)
}

func awaitJavaClientClose(
	t *testing.T,
	sc serviceCluster,
	events *javaClientEvents,
	newLeader *cluster.ConsensusModule,
	skipIndex int,
	sessionID int64,
	last int,
) {
	t.Helper()

	payloads := javaClientPayloads(last)
	requireServiceAwaitTick(t, sc, "session closed", sessionCloseBound, func() bool {
		events.poll(true)
		if _, ok := events.last("CLOSED"); !ok {
			return false
		}
		code, exited := events.client.ExitCode()
		if !exited {
			return false
		}
		if code != 0 {
			events.fail(fmt.Sprintf("client exit %d", code))
		}
		for i, mem := range sc.members {
			if i == skipIndex || sc.set.Module(i) == nil {
				continue
			}
			complete, corrupt := recorderSessionStatus(
				mem.rec,
				sessionID,
				clusterwire.CloseReason_CLIENT_ACTION,
				payloads...,
			)
			if corrupt {
				events.fail(fmt.Sprintf("close sequence member %d", sc.set.Module(i).ThisMemberID()))
			}
			if !complete {
				return false
			}
		}
		return isSessionRegistryIdle(newLeader.Agent())
	}, events.client.Dump)
}

func javaClientPayloads(last int) [][]byte {
	out := make([][]byte, 0, last)
	for seq := 1; seq <= last; seq++ {
		out = append(out, harness.JavaClusterClientPayload(javaClientPayloadPrefix, seq))
	}
	return out
}

func areClusterSessionsIdle(set *harness.ClusterMemberSet) bool {
	for _, module := range set.Modules() {
		if module != nil && !isSessionRegistryIdle(module.Agent()) {
			return false
		}
	}
	return true
}

func isSessionRegistryIdle(agent *consensus.Agent) bool {
	return agent.OpenSessionCount() == 0 &&
		agent.PendingSessionCount() == 0 &&
		agent.ClosingSessionCount() == 0
}

func failJavaClient(
	t *testing.T,
	sc serviceCluster,
	client *harness.JavaClusterClient,
	label string,
) {
	t.Helper()
	require.FailNow(t, label, "%s\n%s", client.Dump(), sc.dump())
}

type javaClientEvents struct {
	t      *testing.T
	sc     serviceCluster
	client *harness.JavaClusterClient
	seen   []harness.JavaClusterClientEvent
}

func (e *javaClientEvents) poll(allowClose bool) {
	e.t.Helper()
	e.collect(allowClose)
	code, exited := e.client.ExitCode()
	if !exited {
		return
	}
	if allowClose && code == 0 {
		if _, sawClosed := e.last("CLOSED"); sawClosed {
			return
		}
	}
	e.fail(fmt.Sprintf("client exit %d", code))
}

func (e *javaClientEvents) collect(allowClose bool) {
	e.t.Helper()
	for {
		ev, ok := e.client.PollEvent()
		if !ok {
			break
		}
		e.seen = append(e.seen, ev)
		switch ev.Kind {
		case "CLOSED":
			if !allowClose {
				e.fail("client event " + ev.Line)
			}
		case "ERROR", "MESSAGE", "SESSION_EVENT":
			e.fail("client event " + ev.Line)
		}
	}
}

func (e *javaClientEvents) last(kind string) (harness.JavaClusterClientEvent, bool) {
	return e.lastFrom(kind, 0)
}

func (e *javaClientEvents) lastFrom(kind string, from int) (harness.JavaClusterClientEvent, bool) {
	for i := len(e.seen) - 1; i >= from; i-- {
		if e.seen[i].Kind == kind {
			return e.seen[i], true
		}
	}
	return harness.JavaClusterClientEvent{}, false
}

func (e *javaClientEvents) fail(label string) {
	e.t.Helper()
	failJavaClient(e.t, e.sc, e.client, label)
}

func (e *javaClientEvents) fieldInt(ev harness.JavaClusterClientEvent, key string) int64 {
	e.t.Helper()
	raw, ok := ev.Fields[key]
	if !ok {
		e.fail("missing " + key + " in " + ev.Line)
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		e.fail("bad " + key + " in " + ev.Line)
	}
	return n
}

func (e *javaClientEvents) lineDump(line string) string {
	return line + "\n" + e.client.Dump() + "\n" + e.sc.dump()
}
