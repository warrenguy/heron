//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/luas/test/integration/harness"
)

const (
	mixedJavaClientCredentials   = "luas-java-client-credentials"
	mixedJavaClientChallenge     = "luas-java-client-challenge"
	mixedJavaClientResponse      = "luas-java-client-response"
	mixedJavaClientPrincipal     = "luas-java-client-principal"
	mixedJavaClientPayloadPrefix = "jc"
	mixedJavaClientMessageCount  = 3
)

var mixedJavaFollowerMemberIDs = []int{0, 1}

// TestClusterMixedGoLeaderJavaClient opens one Java client session on the Go
// appointed leader. Both Java followers decode the open, the messages, and the
// close. Runs only under LUAS_CLUSTER_MIXED_GO_LEADER_JAVA_CLIENT_COMPOSE.
//
// Do not add t.Parallel. The test is the single owner of compose-fixed
// MixedAeronDir*, MixedClusterDir*, PortBase 9000, and MixedLogEndpoint.
func TestClusterMixedGoLeaderJavaClient(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeGoLeaderJavaClientEnvKey,
		clusterjava.MixedComposeSkipHintGoLeaderJavaClient,
	)

	requireMixedAeronDirsPresent(t, mixedJavaFollowerMemberIDs, clusterjava.MixedAeronDirGo)
	java := awaitJavaModuleState(t, nil, mixedJavaFollowerMemberIDs, clusterjava.MixedStartupBudget)

	auth := harness.NewChallengeAuthenticator(
		[]byte(mixedJavaClientCredentials),
		[]byte(mixedJavaClientChallenge),
		[]byte(mixedJavaClientResponse),
		[]byte(mixedJavaClientPrincipal),
	)
	cfg := buildMixedGoLeaderConfig(t)
	cfg.Authenticator = auth
	f := openMixedGoMember(t, cfg)
	writeReady(t)

	awaitGoLeaderElection(t, f, java, clusterjava.MixedElectionBudget)
	awaitJavaServiceSawTermStart(t, f, java, clusterjava.MixedElectionBudget)

	ingress, err := clusterjava.MixedComposeClientIngressEndpoints()
	require.NoError(t, err)
	egress, err := clusterjava.MixedComposeClientEgressChannel()
	require.NoError(t, err)
	client := harness.StartJavaClusterClient(t, harness.JavaClusterClientConfig{
		AeronDir:          clusterjava.MixedAeronDirGo,
		IngressChannel:    cluster.DefaultIngressChannel,
		IngressEndpoints:  ingress,
		EgressChannel:     egress,
		Credentials:       mixedJavaClientCredentials,
		Challenge:         mixedJavaClientChallenge,
		ChallengeResponse: mixedJavaClientResponse,
		MessageTimeout:    clusterjava.MixedConnectBudget,
		NewLeaderTimeout:  clusterjava.MixedCommitBudget,
		PayloadPrefix:     mixedJavaClientPayloadPrefix,
	})
	events := &mixedJavaClientEvents{t: t, f: f, java: java, client: client}

	sessionID := awaitMixedJavaClientConnect(events)
	payloads := mixedJavaClientPayloads()
	awaitMixedJavaClientOpen(events, sessionID)
	awaitMixedJavaClientMessages(events, sessionID, payloads)

	client.RequestClose()
	awaitMixedJavaClientClose(events, sessionID, payloads)

	streams := f.module.LogStreams()
	require.NotNil(t, streams)
	pub := streams.Publication()
	require.NotNil(t, pub)
	sub := streams.Subscription()
	require.NotNil(t, sub)

	require.NoError(t, f.module.Close())
	require.True(t, pub.IsClosed(), "log publication must report closed after Close")
	require.True(t, sub.IsClosed(), "log subscription must report closed after Close")

	markMixedSmokeCompleted(t)
}

func awaitMixedJavaClientConnect(events *mixedJavaClientEvents) int64 {
	t, f := events.t, events.f
	t.Helper()

	start := time.Now()
	ok := f.tickUntil(clusterjava.MixedConnectBudget, func() bool {
		events.poll(false)
		_, sawChallenge := events.last("CHALLENGE")
		_, sawConnected := events.last("CONNECTED")
		return sawChallenge && sawConnected
	})
	if !ok {
		events.fail(fmt.Sprintf(
			"java client did not connect after %s (budget %s)",
			time.Since(start),
			clusterjava.MixedConnectBudget,
		))
	}

	connected, _ := events.last("CONNECTED")
	sessionID := events.fieldInt(connected, "session")
	term := events.fieldInt(connected, "term")
	leaderID := events.fieldInt(connected, "leader")
	require.GreaterOrEqual(t, sessionID, int64(0), connected.Line)
	require.Equal(t, f.module.LeadershipTermID(), term, "term\n%s", events.client.Dump())
	require.Equal(t, int64(clusterjava.MixedGoMemberID), leaderID, "leader\n%s", events.client.Dump())
	return sessionID
}

func awaitMixedJavaClientOpen(events *mixedJavaClientEvents, sessionID int64) {
	f := events.f
	events.t.Helper()

	want := wantSessionProbe(sessionID, nil, false)
	start := time.Now()
	ok := f.tickUntil(clusterjava.MixedCommitBudget, func() bool {
		events.poll(false)
		if f.module.Agent().OpenSessionCount() != 1 {
			return false
		}

		return mixedJavaFollowerProbesMatch(events, want)
	})
	if !ok {
		events.fail(fmt.Sprintf(
			"session open did not commit after %s (budget %s)\n%s",
			time.Since(start),
			clusterjava.MixedCommitBudget,
			mixedSessionProbeDump(),
		))
	}
}

func awaitMixedJavaClientMessages(
	events *mixedJavaClientEvents,
	sessionID int64,
	payloads [][]byte,
) {
	t, f, java := events.t, events.f, events.java
	t.Helper()

	publisher := f.module.LogPublisher()
	require.NotNil(t, publisher, dumpMixedState(f.module, java))
	pre := publisher.Position()
	seenBefore := len(events.seen)
	events.client.Send(mixedJavaClientMessageCount)
	want := wantSessionProbe(sessionID, payloads, false)
	wantLast := strconv.Itoa(mixedJavaClientMessageCount)
	var commitTarget int64

	start := time.Now()
	ok := f.tickUntil(clusterjava.MixedCommitBudget, func() bool {
		events.poll(false)
		sent, saw := events.lastFrom("SENT", seenBefore)
		if !saw {
			return false
		}
		if sent.Fields["first"] != "1" || sent.Fields["last"] != wantLast {
			events.fail("SENT " + sent.Line)
		}
		if !mixedJavaFollowerProbesMatch(events, want) {
			return false
		}
		if commitTarget == 0 {
			commitTarget = publisher.Position()
			if commitTarget <= pre {
				events.fail(fmt.Sprintf("log position %d did not advance past %d", commitTarget, pre))
			}
		}

		current, allocated := javaCommitPositions(java)
		return allocated &&
			f.module.CommitPosition() >= commitTarget &&
			slices.Min(current) >= commitTarget
	})
	if !ok {
		current, _ := javaCommitPositions(java)
		events.fail(fmt.Sprintf(
			"session messages did not commit after %s (budget %s): goCommit=%d target=%d java=%v\n%s",
			time.Since(start),
			clusterjava.MixedCommitBudget,
			f.module.CommitPosition(),
			commitTarget,
			current,
			mixedSessionProbeDump(),
		))
	}
}

func awaitMixedJavaClientClose(
	events *mixedJavaClientEvents,
	sessionID int64,
	payloads [][]byte,
) {
	f := events.f
	events.t.Helper()

	want := wantSessionProbe(sessionID, payloads, true)
	start := time.Now()
	ok := f.tickUntil(clusterjava.MixedCommitBudget, func() bool {
		events.poll(true)
		if _, saw := events.last("CLOSED"); !saw {
			return false
		}
		code, exited := events.client.ExitCode()
		if !exited {
			return false
		}
		if code != 0 {
			events.fail(fmt.Sprintf("client exit %d", code))
		}
		if !mixedJavaFollowerProbesMatch(events, want) {
			return false
		}
		agent := f.module.Agent()
		return agent.OpenSessionCount() == 0 &&
			agent.PendingSessionCount() == 0 &&
			agent.ClosingSessionCount() == 0
	})
	if !ok {
		agent := f.module.Agent()
		events.fail(fmt.Sprintf(
			"session close did not drain after %s (budget %s): open=%d pending=%d closing=%d\n%s",
			time.Since(start),
			clusterjava.MixedCommitBudget,
			agent.OpenSessionCount(),
			agent.PendingSessionCount(),
			agent.ClosingSessionCount(),
			mixedSessionProbeDump(),
		))
	}
}

func mixedJavaClientPayloads() [][]byte {
	out := make([][]byte, mixedJavaClientMessageCount)
	for i := range out {
		out[i] = harness.JavaClusterClientPayload(mixedJavaClientPayloadPrefix, i+1)
	}
	return out
}

func mixedJavaFollowerProbesMatch(
	events *mixedJavaClientEvents,
	want []clusterjava.SessionProbeEvent,
) bool {
	for _, memberID := range mixedJavaFollowerMemberIDs {
		probe, ready := events.readProbe(memberID)
		if !ready || !reflect.DeepEqual(probe, want) {
			return false
		}
	}
	return true
}

func wantSessionProbe(
	sessionID int64,
	payloads [][]byte,
	closed bool,
) []clusterjava.SessionProbeEvent {
	want := []clusterjava.SessionProbeEvent{{
		Kind:      clusterjava.SessionProbeKindOpen,
		SessionID: sessionID,
		Principal: []byte(mixedJavaClientPrincipal),
	}}

	for _, payload := range payloads {
		want = append(want, clusterjava.SessionProbeEvent{
			Kind:      clusterjava.SessionProbeKindMessage,
			SessionID: sessionID,
			Payload:   payload,
		})
	}

	if closed {
		want = append(want, clusterjava.SessionProbeEvent{
			Kind:      clusterjava.SessionProbeKindClose,
			SessionID: sessionID,
			Reason:    clusterwire.CloseReason_CLIENT_ACTION.String(),
		})
	}
	return want
}

func mixedSessionProbeDump() string {
	var b strings.Builder
	for _, memberID := range mixedJavaFollowerMemberIDs {
		path := clusterjava.SessionProbePath(memberID)
		fmt.Fprintf(&b, "sessionProbe member=%d path=%s", memberID, path)
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(&b, " err=%v\n", err)
			continue
		}
		fmt.Fprintf(&b, " content=%q\n", string(data))
	}
	return b.String()
}

type mixedJavaClientEvents struct {
	t      testing.TB
	f      *mixedMemberFixture
	java   []*harness.DriverCounters
	client *harness.JavaClusterClient
	seen   []harness.JavaClusterClientEvent
}

func (e *mixedJavaClientEvents) poll(allowClose bool) {
	e.t.Helper()
	for {
		ev, ok := e.client.PollEvent()
		if !ok {
			break
		}
		e.seen = append(e.seen, ev)
		switch ev.Kind {
		case "ERROR", "MESSAGE", "SESSION_EVENT", "NEW_LEADER":
			e.fail("client event " + ev.Line)
		case "CLOSED":
			if !allowClose {
				e.fail("client event " + ev.Line)
			}
		case "CHALLENGE":
			if ev.Fields["match"] != "true" {
				e.fail("challenge " + ev.Line)
			}
		}
	}
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

func (e *mixedJavaClientEvents) readProbe(memberID int) ([]clusterjava.SessionProbeEvent, bool) {
	e.t.Helper()
	data, err := os.ReadFile(clusterjava.SessionProbePath(memberID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false
		}
		e.fail(fmt.Sprintf("session probe member %d: %v", memberID, err))
	}
	events, err := clusterjava.ParseSessionProbe(data)
	if err != nil {
		e.fail(fmt.Sprintf("session probe member %d: %v content=%q", memberID, err, data))
	}
	return events, true
}

func (e *mixedJavaClientEvents) last(kind string) (harness.JavaClusterClientEvent, bool) {
	return e.lastFrom(kind, 0)
}

func (e *mixedJavaClientEvents) lastFrom(
	kind string,
	from int,
) (harness.JavaClusterClientEvent, bool) {
	for i := len(e.seen) - 1; i >= from; i-- {
		if e.seen[i].Kind == kind {
			return e.seen[i], true
		}
	}
	return harness.JavaClusterClientEvent{}, false
}

func (e *mixedJavaClientEvents) fieldInt(ev harness.JavaClusterClientEvent, key string) int64 {
	e.t.Helper()
	raw, ok := ev.Fields[key]
	if !ok {
		e.fail("missing " + key + " in " + ev.Line)
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		e.fail(key + " in " + ev.Line)
		return 0
	}
	return n
}

func (e *mixedJavaClientEvents) fail(label string) {
	e.t.Helper()
	require.FailNow(e.t, label, "%s\n%s", e.client.Dump(), dumpMixedState(e.f.module, e.java))
}
