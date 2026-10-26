//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/test/integration/harness"
)

// egressEcho is one client echo with the term from its SessionMessageHeader.
// leadershipTermID is -1 when the header did not decode.
type egressEcho struct {
	sessionID        int64
	leadershipTermID int64
	payload          []byte
}

// egressRecorder keeps each client echo and close detail. Invoker-only:
// PollEgress runs on the test goroutine.
type egressRecorder struct {
	echoes        []egressEcho
	closedDetails []string
}

// handler returns client egress handlers that append to r. MessageFunc
// decodes the SessionMessageHeader that precedes the payload offset.
func (r *egressRecorder) handler() *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageFunc: func(
			sessionID, _ int64,
			buffer atomicbuffer.AtomicBuffer,
			offset, length int,
			_ *aeron.Header,
		) {
			raw := buffer.Bytes()
			term := int64(-1)
			headerOffset := offset - client.SessionHeaderLength()
			if header, ok := codecs.TryDecodeSessionMessageHeaderAt(raw, headerOffset); ok {
				term = header.LeadershipTermID
			}
			r.echoes = append(r.echoes, egressEcho{
				sessionID:        sessionID,
				leadershipTermID: term,
				payload:          bytes.Clone(raw[offset : offset+length]),
			})
		},
		SessionEventFunc: closedDetailHandler(&r.closedDetails),
	}
}

// formatEgressState prints the client session state, each echo, and the
// closed details for a timeout dump.
func formatEgressState(egress *egressRecorder, session *client.AeronCluster) string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"clientState=%s clientTerm=%d closed=%q\n",
		session.SessionState(),
		session.LeadershipTermID(),
		egress.closedDetails,
	)
	for i, echo := range egress.echoes {
		fmt.Fprintf(
			&b,
			"echo[%d] session=%d term=%d payload=%q\n",
			i,
			echo.sessionID,
			echo.leadershipTermID,
			echo.payload,
		)
	}
	return b.String()
}

// serviceEgressReconnected reports whether egress settled after a leader
// change:
//   - the service at newIndex is a bound leader with no pending join and
//     holds one response publication
//   - the service at followerIndex is a bound follower and holds none
//   - neither service has a response add in flight
func serviceEgressReconnected(sc serviceCluster, newIndex, followerIndex int) bool {
	leader := sc.members[newIndex].agent
	if !leader.IsLogBound() || leader.HasPendingJoin() || leader.Role() != codecs.RoleLeader {
		return false
	}
	if leader.PendingResponsePublicationAdds() != 0 || leader.ResponsePublicationCount() != 1 {
		return false
	}
	follower := sc.members[followerIndex].agent
	if !follower.IsLogBound() || follower.Role() != codecs.RoleFollower {
		return false
	}
	return follower.PendingResponsePublicationAdds() == 0 &&
		follower.ResponsePublicationCount() == 0
}

// TestClusterServiceEgress_repliesAcrossLeaderStop asserts a client receives
// echoes and a close across a leader stop.
func TestClusterServiceEgress_repliesAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	// Phase: open.
	sc := startServiceCluster(
		t,
		harness.ClusterServiceEgressPortBase,
		harness.ClusterServiceEgressLogEndpoint,
		amendServiceTransferMember,
	)
	set := sc.set
	for _, mem := range sc.members {
		mem.rec.echo = true
	}
	requireServicesJoined(t, sc)
	requireNoServiceFailures(t, sc, "")
	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())

	// Phase: connect.
	egress := &egressRecorder{}
	pump, session := openTransferSession(
		t, sc, leader,
		harness.ClientEgressPort(harness.ClusterServiceEgressPortBase),
		egress.handler(),
	)
	sessionID := session.ClusterSessionID()
	egressDetail := func() string { return formatEgressState(egress, session) }

	// Phase: session open committed. Only the leader holds a publication.
	requireServiceAwaitTick(
		t, sc,
		"session open committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			if leader.Agent().OpenSessionCount() != 1 {
				return false
			}
			for i, mem := range sc.members {
				if !mem.rec.holdsOpen(sessionID) || mem.agent.PendingResponsePublicationAdds() != 0 {
					return false
				}
				want := 0
				if i == leaderIndex {
					want = 1
				}
				if mem.agent.ResponsePublicationCount() != want {
					return false
				}
			}
			return true
		}),
	)

	// Phase: echo before stop.
	first := []byte(serviceMessagePayload)
	preTerm := leader.LeadershipTermID()
	offerTransferSessionMessage(t, set, pump, first, sc.dump)
	requireServiceAwaitTick(
		t, sc,
		"echo before stop",
		durableWorkBound,
		pump.sessionPump(func() bool {
			for _, mem := range sc.members {
				if !mem.rec.holdsMessagesAfterOpen(sessionID, first) {
					return false
				}
			}
			return len(egress.echoes) != 0
		}),
		egressDetail,
	)
	firstEcho := egressEcho{sessionID: sessionID, leadershipTermID: preTerm, payload: first}
	require.Equal(t, []egressEcho{firstEcho}, egress.echoes,
		"echoes before stop:\n%s\n%s", egressDetail(), sc.dump())
	require.Equal(t, preTerm, session.LeadershipTermID(), sc.dump())
	for i, mem := range sc.members {
		if i == leaderIndex {
			require.Len(t, mem.rec.echoResults, 1,
				"leader echo results index %d:\n%s", i, sc.dump())
			require.Positive(t, mem.rec.echoResults[0],
				"leader echo result index %d:\n%s", i, sc.dump())
			continue
		}
		require.Equal(t, []int64{service.MockedOffer}, mem.rec.echoResults,
			"follower echo results index %d:\n%s", i, sc.dump())
	}

	// Phase: services at commit.
	requireServicesAtCommit(t, sc, pump)
	preCommit := leader.CommitPosition()
	logFloors := snapshotServiceLogPositions(sc)

	// Phase: stop the leader.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	closeServiceTransferContainer(t, sc, leaderIndex)

	// Phase: re-elect and recover the session.
	newLeader := requireTransferSessionRecovery(
		t, sc, pump, session, leaderIndex, preTerm, preCommit, logFloors,
	)
	newIndex := int(newLeader.ThisMemberID())
	followerIndex := firstNonLeaderIndex(set, newLeader.ThisMemberID())
	require.GreaterOrEqual(t, followerIndex, 0, "no surviving follower:\n%s", sc.dump())
	survivors := []int{newIndex, followerIndex}

	// Phase: egress reconnect.
	requireServiceAwaitTick(
		t, sc,
		"egress reconnect",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			return serviceEgressReconnected(sc, newIndex, followerIndex)
		}),
	)

	// Phase: echo after leader change.
	second := []byte(serviceSecondPayload)
	offerTransferSessionMessage(t, set, pump, second, sc.dump)
	requireServiceAwaitTick(
		t, sc,
		"echo after leader change",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			failIfRecorderSequenceCorrupt(t, sc, leaderIndex, sessionID, egressDetail, first, second)
			return survivorServicesHoldSequence(sc, leaderIndex, sessionID, first, second) &&
				len(egress.echoes) >= 2
		}),
		egressDetail,
	)
	newTerm := newLeader.LeadershipTermID()
	require.Greater(t, newTerm, preTerm, sc.dump())
	require.Equal(t, newTerm, session.LeadershipTermID(), sc.dump())
	secondEcho := egressEcho{sessionID: sessionID, leadershipTermID: newTerm, payload: second}
	require.Equal(t, []egressEcho{firstEcho, secondEcho}, egress.echoes,
		"echoes after leader change:\n%s\n%s", egressDetail(), sc.dump())
	newResults := sc.members[newIndex].rec.echoResults
	require.Len(t, newResults, 2, "new leader echo results:\n%s", sc.dump())
	require.Equal(t, service.MockedOffer, newResults[0],
		"new leader echo result before stop:\n%s", sc.dump())
	require.Positive(t, newResults[1], "new leader echo result after stop:\n%s", sc.dump())
	followerResults := sc.members[followerIndex].rec.echoResults
	require.Equal(t, []int64{service.MockedOffer, service.MockedOffer}, followerResults,
		"follower echo results:\n%s", sc.dump())

	// Phase: service close across leader change. failIfRecorderSequenceCorrupt
	// fails on a close, so this wait omits it.
	closePayload := []byte(serviceClosePayload)
	for _, i := range survivors {
		sc.members[i].rec.closePayload = closePayload
	}
	offerTransferSessionMessage(t, set, pump, closePayload, sc.dump)
	wantClosed := []string{"SERVICE_ACTION"}
	requireServiceAwaitTick(
		t, sc,
		"service close across leader change",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			return servicesClosedSession(sc, leaderIndex, sessionID) &&
				slices.Equal(egress.closedDetails, wantClosed) && session.IsClosed()
		}),
		egressDetail,
	)
	for _, i := range survivors {
		mem := sc.members[i]
		require.Equal(t, []bool{true}, mem.rec.closeResults,
			"close results index %d:\n%s", i, sc.dump())
		require.Len(t, mem.rec.echoResults, 2,
			"echo results after close index %d:\n%s", i, sc.dump())
		require.Zero(t, mem.agent.ResponsePublicationCount(),
			"publications after close index %d:\n%s", i, sc.dump())
		require.Zero(t, mem.agent.SessionCount(),
			"service sessions after close index %d:\n%s", i, sc.dump())
		requireNoServiceFailure(t, sc, i, " after close")
	}
}
