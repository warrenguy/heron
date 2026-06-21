//go:build testhooks

package client

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	asyncConnectIPCChannel     = "aeron:ipc"
	asyncConnectPollTimeout    = 5 * time.Second
	asyncConnectHarnessDoWorkN = 4
)

type asyncConnectHarnessResponder func(*aeron.AsyncConnectTestHarness)

func baseAsyncConnectContext(
	client *aeron.Client,
	ingressEndpoints string,
) ClusterContext {
	ingressChannel := asyncConnectIPCChannel
	if ingressEndpoints != "" {
		ingressChannel = "aeron:udp?ttl=16"
	}
	return ClusterContext{
		IngressChannel:   ingressChannel,
		EgressChannel:    asyncConnectIPCChannel,
		IngressEndpoints: ingressEndpoints,
		Aeron:            client,
		MessageTimeout:   30 * time.Second,
	}
}

func setupAsyncConnect(
	t *testing.T,
	ingressEndpoints string,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ac, err := NewAsyncConnect(baseAsyncConnectContext(harness.Client, ingressEndpoints))
	require.NoError(t, err)
	return harness, ac
}

func setupAsyncConnectWithContext(
	t *testing.T,
	mutate func(ClusterContext) ClusterContext,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := mutate(baseAsyncConnectContext(harness.Client, ""))
	ac, err := NewAsyncConnect(ctx)
	require.NoError(t, err)
	return harness, ac
}

func attachEgressImageIfNeeded(t *testing.T, sub *aeron.Subscription) {
	t.Helper()
	if sub == nil || sub.IsConnected() {
		return
	}
	image := aeron.NewTestImageForSubscriptionHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
}

func withAsyncConnectEgressTerm(
	t *testing.T,
	ac *AsyncConnect,
	fn func(*testing.T, atomic.AtomicBuffer, int64),
) {
	t.Helper()

	correlation := ac.CorrelationIDForTest()
	if correlation == conductor.NullCorrelationID {
		return
	}
	fn(t, egressTermBufferForAsyncConnect(t, ac), correlation)
}

func encodeOKSessionEventWireForConnect(
	t *testing.T,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
) []byte {
	t.Helper()

	fields := defaultSessionEventFields(detail)
	fields.CorrelationID = correlationID
	fields.LeaderMemberID = leaderMemberID
	return encodeSessionEventWireWithFields(
		t,
		egressTestClusterSessionID,
		fields,
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func writeOKSessionEventForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeOKSessionEventWireForConnect(t, correlationID, leaderMemberID, detail),
	)
}

func appendOKSessionEventForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
) {
	t.Helper()

	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeOKSessionEventWireForConnect(t, correlationID, leaderMemberID, detail),
	)
}

func writeSessionEventWithHeartbeatForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
	heartbeatNs int64,
) {
	t.Helper()

	fields := defaultSessionEventFields([]byte("heartbeat-detail"))
	fields.CorrelationID = correlationID
	fields.LeaderHeartbeatTimeoutNs = heartbeatNs
	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventWireWithFields(
			t,
			egressTestClusterSessionID,
			fields,
			gensbe.SessionEventSbeSchemaVersion,
		),
	)
}

func pollAsyncConnectWithResponder(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	respond asyncConnectHarnessResponder,
	onStep func(ac *AsyncConnect),
) (*ConnectResult, error) {
	t.Helper()

	deadline := time.Now().Add(asyncConnectPollTimeout)
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		respond(harness)
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		if onStep != nil {
			onStep(ac)
		}
		result, err := ac.Poll()
		if result != nil || err != nil {
			return result, err
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNowf(
		t,
		"async connect poll timed out",
		"state=%v step=%v correlation=%v",
		ac.State(),
		ac.Step(),
		ac.CorrelationIDForTest(),
	)
	return nil, nil
}

func pollAsyncConnectUntilSelectiveMembers(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	members []aeron.ExclusiveMemberAdd,
	onStep func(ac *AsyncConnect),
) (*ConnectResult, error) {
	t.Helper()

	return pollAsyncConnectWithResponder(t, ac, harness, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondSubscriptionAndExclusiveMemberAdds(members)
	}, onStep)
}

func pollAsyncConnectUntil(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	onStep func(ac *AsyncConnect),
) (*ConnectResult, error) {
	t.Helper()

	return pollAsyncConnectWithResponder(t, ac, harness, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	}, onStep)
}

func pollUntilConnectPollResponse(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
	respond asyncConnectHarnessResponder,
) {
	t.Helper()

	deadline := time.Now().Add(asyncConnectPollTimeout)
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		respond(harness)
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.State() == ConnectStatePollResponse &&
			ac.CorrelationIDForTest() != conductor.NullCorrelationID {
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNowf(
		t,
		"async connect poll timed out",
		"state=%v step=%v correlation=%v",
		ac.State(),
		ac.Step(),
		ac.CorrelationIDForTest(),
	)
}

func egressTermBufferForAsyncConnect(t *testing.T, ac *AsyncConnect) atomic.AtomicBuffer {
	t.Helper()

	sub := ac.EgressSubscriptionForTest()
	require.NotNil(t, sub)
	for i := range 8 {
		if sub.IsConnected() {
			break
		}
		image, ok := sub.ImageAt(0)
		if ok && image != nil {
			return aeron.ImageTermBufferForHooks(image)
		}
		image = aeron.NewTestImageForSubscriptionHooks(t, sub, int64(i))
		require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	}
	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	return aeron.ImageTermBufferForHooks(image)
}

func TestAsyncConnectTestHarness_offerExclusivePoll(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	regID, err := harness.Client.OfferAddExclusivePublication(
		DefaultIngressStreamID,
		asyncConnectIPCChannel,
	)
	require.NoError(t, err)

	var ep *aeron.ExclusivePublication
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		harness.RespondPendingAdds()
		var done bool
		ep, done, err = harness.Client.PollAddExclusivePublication(regID)
		if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, err)
	require.NotNil(t, ep)
	require.True(t, ep.IsConnected())
	require.Positive(t, ep.Offer([]byte{1, 2, 3}))
}

func TestAsyncConnect_happyPathIPC(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	var sessionEventWritten bool
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		if sessionEventWritten {
			return
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeSessionEventForConnect(t, termBuffer, correlation)
				sessionEventWritten = true
			},
		)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, egressTestClusterSessionID, result.ClusterSessionID)
	require.NotNil(t, result.IngressPublication)
	require.NotNil(t, result.EgressSubscription)
	require.False(t, result.OwnsAeronClient)
}

func TestAsyncConnect_challengeRoundTrip(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(5 * time.Second)
	var result *ConnectResult
	var pollErr error
	challengeWritten := false
	challengeResponseOffered := false
	sessionEventWritten := false
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		harness.RespondPendingAdds()
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())

		if challengeWritten && ac.State() == ConnectStateSendMessage {
			challengeResponseOffered = true
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				switch {
				case !challengeWritten && ac.State() == ConnectStatePollResponse:
					writeChallengeForConnect(t, termBuffer, correlation)
					challengeWritten = true
				case challengeResponseOffered && !sessionEventWritten &&
					ac.State() == ConnectStatePollResponse:
					appendSessionEventForConnect(t, termBuffer, correlation)
					sessionEventWritten = true
				}
			},
		)

		result, pollErr = ac.Poll()
		if pollErr != nil || result != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, pollErr)
	require.NotNil(t, result)
	require.True(t, challengeWritten)
	require.True(t, challengeResponseOffered)
	require.True(t, sessionEventWritten)
	require.Equal(t, ConnectStateDone, ac.State())
}

func TestAsyncConnect_pollResponse_newLeaderWithActiveCorrelation(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})

	writeRedirectNewLeaderForConnect(
		t,
		egressTermBufferForAsyncConnect(t, ac),
		1,
		"1=host-c:20112",
	)
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())
	require.Equal(t, conductor.NullCorrelationID, ac.CorrelationIDForTest())

	leader, ok := ac.memberIngressForTest(1)
	require.True(t, ok)
	require.Equal(t, "host-c:20112", leader.Endpoint)
	require.NotEqual(t, conductor.NullCorrelationID, ac.MemberIngressRegIDForTest(1))
}

func TestAsyncConnect_redirectRebuildsMap(t *testing.T) {
	const (
		member0RegID int64 = 3001
		member1RegID int64 = 3002
		member2RegID int64 = 3003
	)
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	harness.SetCountersReaderForRegs(t, member0RegID, member1RegID, member2RegID)
	t.Cleanup(func() { _ = ac.Close() })

	member0Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, false)
	member1Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, true)
	member2Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, true)

	initialMembers := []aeron.ExclusiveMemberAdd{
		{
			EndpointSubstring: "host-a:20110",
			LogPath:           member0Log,
			RegistrationID:    member0RegID,
			CounterIndex:      0,
		},
		{
			EndpointSubstring: "host-b:20111",
			LogPath:           member1Log,
			RegistrationID:    member1RegID,
			CounterIndex:      1,
		},
	}
	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondSubscriptionAndExclusiveMemberAdds(initialMembers)
	})

	writeRedirectNewLeaderForConnect(
		t,
		egressTermBufferForAsyncConnect(t, ac),
		2,
		"2=host-c:20112",
	)
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())
	_, ok := ac.memberIngressForTest(0)
	require.False(t, ok)
	_, ok = ac.memberIngressForTest(1)
	require.False(t, ok)
	leader, ok := ac.memberIngressForTest(2)
	require.True(t, ok)
	require.Equal(t, "host-c:20112", leader.Endpoint)
	require.NotEqual(t, conductor.NullCorrelationID, ac.MemberIngressRegIDForTest(2))

	harness.SetCountersReaderForRegs(t, member2RegID)

	var sessionEventWritten bool
	result, err := pollAsyncConnectWithResponder(
		t,
		ac,
		harness,
		func(h *aeron.AsyncConnectTestHarness) {
			h.RespondExclusiveAdds([]string{member2Log}, []int64{member2RegID})
		},
		func(ac *AsyncConnect) {
			withAsyncConnectEgressTerm(
				t,
				ac,
				func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
					if sessionEventWritten {
						return
					}
					appendOKSessionEventForConnect(
						t,
						termBuffer,
						correlation,
						2,
						[]byte("redirect-ok"),
					)
					sessionEventWritten = true
				},
			)
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(2), result.LeaderMemberID)
	leader, ok = result.MemberIngressMap.Lookup(2)
	require.True(t, ok)
	require.Equal(t, "host-c:20112", leader.Endpoint)
	require.NotNil(t, leader.Publication)
	require.True(t, leader.Publication.IsConnected())
	_, ok = result.MemberIngressMap.Lookup(0)
	require.False(t, ok)
	_, ok = result.MemberIngressMap.Lookup(1)
	require.False(t, ok)
}

func TestAsyncConnect_timeout(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := baseAsyncConnectContext(harness.Client, "")
	ctx.MessageTimeout = 5 * time.Millisecond
	ac, err := NewAsyncConnect(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(200 * time.Millisecond)
	var pollErr error
	for time.Now().Before(deadline) && pollErr == nil {
		harness.DoWork()
		harness.RespondPendingAdds()
		_, pollErr = ac.Poll()
	}
	require.Error(t, pollErr)
	require.ErrorContains(t, pollErr, "cluster client: connect timeout")
}

func TestNewAsyncConnect_udpEgressRejected(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := baseAsyncConnectContext(harness.Client, "")
	ctx.EgressChannel = "aeron:udp?endpoint=localhost:0"
	_, err := NewAsyncConnect(ctx)
	require.EqualError(
		t,
		err,
		"cluster client: UDP egress response channel not supported; use ipc or ipcx",
	)
}

func TestAsyncConnect_ownedClientClosedOnError(t *testing.T) {
	ctx := ClusterContext{
		IngressChannel: asyncConnectIPCChannel,
		EgressChannel:  asyncConnectIPCChannel,
		AeronDir:       t.TempDir(),
	}
	_, err := NewAsyncConnect(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "aeron:")
}

func TestAsyncConnect_closeReleasesPartialResources(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")

	deadline := time.Now().Add(2 * time.Second)
	var pendingRegID int64
	for time.Now().Before(deadline) {
		harness.DoWork()
		harness.RespondSubscriptionAdd()
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.State() != ConnectStateCreateIngressPublications {
			continue
		}
		pendingRegID = ac.MemberIngressRegIDForTest(0)
		if pendingRegID == conductor.NullCorrelationID {
			pendingRegID = ac.MemberIngressRegIDForTest(1)
		}
		if pendingRegID != conductor.NullCorrelationID {
			break
		}
	}
	require.Equal(t, ConnectStateCreateIngressPublications, ac.State())
	require.NotEqual(t, conductor.NullCorrelationID, pendingRegID)
	_, ok := harness.PendingResult(pendingRegID)
	require.True(t, ok)

	require.NoError(t, ac.Close())
	_, ok = harness.PendingResult(pendingRegID)
	require.False(t, ok)
	require.Equal(t, 0, harness.PublicationCount())
}

func TestAsyncConnect_egressSubscriptionRetryNotReady(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	var firstOffer int64
	egressSubscriptionRetryNotReadyHook = func() bool { return true }
	t.Cleanup(func() { egressSubscriptionRetryNotReadyHook = nil })

	pollAddEgressSubscriptionHook = func(
		c *aeron.Client,
		registrationID int64,
	) (*aeron.Subscription, bool, error) {
		if firstOffer == 0 {
			firstOffer = registrationID
			return nil, false, nil
		}
		return c.PollAddSubscription(registrationID)
	}
	t.Cleanup(func() { pollAddEgressSubscriptionHook = nil })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEqual(t, conductor.NullCorrelationID, firstOffer)
	_, ok := harness.PendingResult(firstOffer)
	require.False(t, ok)
}

func TestAsyncConnect_pollResponse_ignoresMismatchedCorrelation(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	stalled := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		harness.RespondPendingAdds()
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		if ac.State() == ConnectStatePollResponse && !stalled {
			termBuffer := egressTermBufferForAsyncConnect(t, ac)
			writeSessionEventForConnect(t, termBuffer, ac.CorrelationIDForTest()+999)
			stalled = true
		}
		result, err := ac.Poll()
		require.NoError(t, err)
		require.Nil(t, result, "unexpected connect completion on mismatched correlation")
		if stalled && ac.State() == ConnectStatePollResponse {
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNow(t, "expected to remain in poll response after mismatched correlation")
}

func TestAsyncConnect_concludeConnect_setsNewLeaderTimeout(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	const heartbeatNs = int64(250_000_000)
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})
	require.NoError(t, err)
	require.Equal(t, 2*time.Duration(heartbeatNs), result.Context.NewLeaderTimeout)
}

func TestAsyncConnect_awaitPublicationConnected_firstConnectedMember(t *testing.T) {
	disconnectedPub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, false)
	require.False(t, disconnectedPub.IsConnected())
	connectedPub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)
	require.True(t, connectedPub.IsConnected())

	memberMap := MemberIngressMap{
		0: {MemberID: 0, Endpoint: "host-a:20110", Publication: disconnectedPub},
		1: {MemberID: 1, Endpoint: "host-b:20111", Publication: connectedPub},
	}
	pub := firstConnectedIngressPublicationForTest(memberMap)
	require.Same(t, connectedPub, pub)
}

func TestAsyncConnect_awaitPublicationConnected_selectiveTwoMemberConnect(t *testing.T) {
	const (
		member0RegID int64 = 3001
		member1RegID int64 = 3002
	)
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	harness.SetCountersReaderForRegs(t, member0RegID, member1RegID)
	t.Cleanup(func() { _ = ac.Close() })

	member0Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, false)
	member1Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, true)

	sawAwaitConnected := false
	sawMember1Selected := false
	result, err := pollAsyncConnectUntilSelectiveMembers(t, ac, harness, []aeron.ExclusiveMemberAdd{
		{
			EndpointSubstring: "host-a:20110",
			LogPath:           member0Log,
			RegistrationID:    member0RegID,
			CounterIndex:      0,
		},
		{
			EndpointSubstring: "host-b:20111",
			LogPath:           member1Log,
			RegistrationID:    member1RegID,
			CounterIndex:      1,
		},
	}, func(ac *AsyncConnect) {
		if ac.State() == ConnectStateAwaitPublicationConnected {
			sawAwaitConnected = true
		}
		if ac.State() == ConnectStateSendMessage && !sawMember1Selected {
			member0Pub := ac.MemberIngressPublicationForTest(0)
			member1Pub := ac.MemberIngressPublicationForTest(1)
			require.NotNil(t, member1Pub)
			require.True(t, member1Pub.IsConnected())
			if member0Pub != nil {
				require.False(t, member0Pub.IsConnected())
			}
			sawMember1Selected = true
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeOKSessionEventForConnect(t, termBuffer, correlation, 1, []byte("selective-member-ok"))
			},
		)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, sawAwaitConnected)
	require.True(t, sawMember1Selected)

	member1, ok := result.MemberIngressMap.Lookup(1)
	require.True(t, ok)
	require.NotNil(t, member1.Publication)
	require.True(t, member1.Publication.IsConnected())
	require.Equal(t, int32(1), result.LeaderMemberID)
	require.Same(t, member1.Publication, result.IngressPublication)
}

func TestAsyncConnect_awaitPublicationConnected_deterministicMemberOrder(t *testing.T) {
	member0Pub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)
	member1Pub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)

	memberMap := MemberIngressMap{
		1: {MemberID: 1, Endpoint: "host-b:20111", Publication: member1Pub},
		0: {MemberID: 0, Endpoint: "host-a:20110", Publication: member0Pub},
	}
	pub := firstConnectedIngressPublicationForTest(memberMap)
	require.Same(t, member0Pub, pub)
}

func TestAsyncConnect_awaitPublicationConnected_multiMemberIPC(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeOKSessionEventForConnect(t, termBuffer, correlation, 0, []byte("multi-member-ok"))
			},
		)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(0), result.LeaderMemberID)
}

func TestAsyncConnect_successTransfersOwnershipWithoutClose(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, ConnectStateDone, ac.State())

	require.NotNil(t, result.EgressSubscription)
	require.True(t, result.EgressSubscription.IsConnected())
	require.NotNil(t, result.IngressPublication)
	require.True(t, result.IngressPublication.IsConnected())

	_, pollErr := ac.Poll()
	require.EqualError(t, pollErr, "cluster client: async connect already completed")
	require.NoError(t, ac.Close())
	_, pollErr = ac.Poll()
	require.EqualError(t, pollErr, "cluster client: async connect already completed")
}

func TestAsyncConnect_pollGuards(t *testing.T) {
	var ac *AsyncConnect
	_, err := ac.Poll()
	require.EqualError(t, err, "cluster client: nil async connect")

	harness, ac := setupAsyncConnect(t, "")
	require.NoError(t, ac.Close())
	_, err = ac.Poll()
	require.EqualError(t, err, "cluster client: async connect closed")

	ac, err = NewAsyncConnect(baseAsyncConnectContext(harness.Client, ""))
	require.NoError(t, err)
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	_, err = ac.Poll()
	require.EqualError(t, err, "cluster client: async connect already completed")
}

func TestAsyncConnect_channelOnlyIngressPublicationError(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(2 * time.Second)
	var pollErr error
	for time.Now().Before(deadline) && pollErr == nil {
		harness.DoWork()
		harness.RespondSubscriptionAdd()
		if ac.MemberIngressRegIDForTest(0) != conductor.NullCorrelationID {
			require.NoError(
				t,
				harness.Fixture().Transmit(
					conductor.OnError,
					conductor.ErrorResponseBytes(
						ac.MemberIngressRegIDForTest(0),
						11,
						"ingress publication rejected",
					),
				),
			)
		}
		_, pollErr = ac.Poll()
	}
	require.Error(t, pollErr)
	require.ErrorContains(t, pollErr, "ingress publication rejected")
}

func TestAsyncConnect_ingressPublicationErrorDuringAwait(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(8 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		harness.RespondPendingAdds()
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		_, err = ac.Poll()
		require.NoError(t, err)
		if ac.State() == ConnectStatePollResponse &&
			ac.CorrelationIDForTest() != conductor.NullCorrelationID {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, ConnectStatePollResponse, ac.State())
	correlation := ac.CorrelationIDForTest()
	writeSessionEventRedirectForConnect(
		t,
		egressTermBufferForAsyncConnect(t, ac),
		correlation,
		"1=host-c:20112",
	)
	_, err = ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())

	pendingReg := ac.MemberIngressRegIDForTest(1)
	require.NotEqual(t, conductor.NullCorrelationID, pendingReg)
	require.NoError(
		t,
		harness.Fixture().Transmit(
			conductor.OnError,
			conductor.ErrorResponseBytes(pendingReg, 11, "member ingress rejected"),
		),
	)
	_, pollErr := ac.Poll()
	require.Error(t, pollErr)
	require.ErrorContains(t, pollErr, "member ingress rejected")
}

func TestAsyncConnect_pollResponse_sessionEventErrors(t *testing.T) {
	cases := []struct {
		name       string
		code       gensbe.EventCode
		detail     []byte
		wantPrefix string
	}{
		{
			name:       "authentication_rejected",
			code:       gensbe.EventCode_AUTHENTICATION_REJECTED,
			detail:     []byte("bad credentials"),
			wantPrefix: "cluster client: authentication rejected",
		},
		{
			name:       "error",
			code:       gensbe.EventCode_ERROR,
			detail:     []byte("connect failed"),
			wantPrefix: "cluster client: connect error",
		},
		{
			name:       "unknown_code",
			code:       gensbe.EventCode(99),
			detail:     []byte("unknown"),
			wantPrefix: "cluster client: unexpected session event code 99",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness, ac := setupAsyncConnect(t, "")
			t.Cleanup(func() { _ = ac.Close() })

			_, pollErr := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
				if ac.State() != ConnectStatePollResponse {
					return
				}
				withAsyncConnectEgressTerm(
					t,
					ac,
					func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
						fields := defaultSessionEventFields(tc.detail)
						fields.CorrelationID = correlation
						fields.Code = tc.code
						writeUnfragmentedWireToTerm(
							t,
							termBuffer,
							encodeSessionEventWireWithFields(
								t,
								egressTestClusterSessionID,
								fields,
								gensbe.SessionEventSbeSchemaVersion,
							),
						)
					},
				)
			})
			require.Error(t, pollErr)
			require.ErrorContains(t, pollErr, tc.wantPrefix)
		})
	}
}

func TestAsyncConnect_pollResponse_sessionEventRedirectCode(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	redirected := false
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				if !redirected {
					writeSessionEventRedirectForConnect(t, termBuffer, correlation, "1=host-b:20111")
					redirected = true
					return
				}
				writeOKSessionEventForConnect(t, termBuffer, correlation, 1, []byte("redirect-ok"))
			},
		)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(1), result.LeaderMemberID)
}

func TestAsyncConnect_concludeConnect_preservesExplicitNewLeaderTimeout(t *testing.T) {
	const explicit = 9 * time.Second
	harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
		ctx.NewLeaderTimeout = explicit
		return ctx
	})
	t.Cleanup(func() { _ = ac.Close() })

	const heartbeatNs = int64(250_000_000)
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})
	require.NoError(t, err)
	require.Equal(t, explicit, result.Context.NewLeaderTimeout)
}

func TestAsyncConnect_ownedClientClosedOnMemberMapError(t *testing.T) {
	harness := aeron.NewAsyncConnectTestHarness(t)
	var closedClient *aeron.Client
	ownedClientClosedOnMemberMapErrorHook = func(client *aeron.Client) {
		closedClient = client
	}
	t.Cleanup(func() { ownedClientClosedOnMemberMapErrorHook = nil })

	newOwnedConnectClientHook = func(ClusterContext) (*aeron.Client, error) {
		return harness.Client, nil
	}
	t.Cleanup(func() { newOwnedConnectClientHook = nil })

	ctx := ClusterContext{
		IngressChannel:   "aeron:udp?ttl=16",
		EgressChannel:    asyncConnectIPCChannel,
		IngressEndpoints: "not-valid-endpoints",
		AeronDir:         t.TempDir(),
		MessageTimeout:   time.Second,
	}
	_, err := NewAsyncConnect(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "cluster client:")
	require.NotNil(t, closedClient)
	require.True(t, aeron.ClientIsClosedForTest(closedClient))
}

func setupAsyncConnectEgressReady(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		harness.RespondSubscriptionAdd()
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.EgressSubscriptionForTest() != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NotNil(t, ac.EgressSubscriptionForTest())
	attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
}

func TestAsyncConnect_prepareConnectRequest_wireLengthGuard(t *testing.T) {
	const (
		connectRequestClientInfo = "name=" + defaultClusterClientName +
			" " + clusterConnectClientLabel
		connectRequestClientInfoLen = len(connectRequestClientInfo)
		connectRequestIPCChannel    = len(asyncConnectIPCChannel)
	)
	require.Equal(t, connectRequestIPCChannel, sessionConnectResponseChannelMaxLen)

	t.Run("credentials_over_derived_max_fail_per_field_guard", func(t *testing.T) {
		harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx.CredentialsSupplier = oversizedCredentialsSupplier{
				encodedLen: maxEncodedCredentialsLen + 1,
			}
			return ctx
		})
		t.Cleanup(func() { _ = ac.Close() })
		setupAsyncConnectEgressReady(t, harness, ac)

		err := ac.PrepareConnectRequestForTest()
		require.Error(t, err)
		require.ErrorContains(t, err, "cluster client: encoded credentials exceed max length")
		require.ErrorContains(t, err, strconv.Itoa(maxEncodedCredentialsLen))
		require.Equal(t, 0, ac.ConnectWireLengthForTest())
	})

	t.Run("credentials_fitting_total_wire_encode", func(t *testing.T) {
		harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx.CredentialsSupplier = oversizedCredentialsSupplier{
				encodedLen: maxEncodedCredentialsLen,
			}
			return ctx
		})
		t.Cleanup(func() { _ = ac.Close() })
		setupAsyncConnectEgressReady(t, harness, ac)

		require.NoError(t, ac.PrepareConnectRequestForTest())
		require.Equal(
			t,
			sessionConnectRequestWireLen(
				gensbe.SessionConnectRequestSbeSchemaVersion,
				connectRequestIPCChannel,
				maxEncodedCredentialsLen,
				connectRequestClientInfoLen,
			),
			ac.ConnectWireLengthForTest(),
		)
		require.LessOrEqual(t, ac.ConnectWireLengthForTest(), connectWireBufferSize)
	})
}

func TestAsyncConnect_prepareChallengeResponse_wireLengthGuard(t *testing.T) {
	t.Run("credentials_over_derived_max_fail_per_field_guard", func(t *testing.T) {
		_, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx.CredentialsSupplier = oversizedCredentialsSupplier{
				challengeLen: maxChallengeResponseLen + 1,
			}
			return ctx
		})
		t.Cleanup(func() { _ = ac.Close() })
		ac.encodedChallenge = []byte("challenge")
		ac.clusterSessionID = egressTestClusterSessionID

		err := ac.PrepareChallengeResponseForTest()
		require.Error(t, err)
		require.ErrorContains(t, err, "cluster client: challenge response credentials exceed max length")
		require.ErrorContains(t, err, strconv.Itoa(maxChallengeResponseLen))
		require.Equal(t, 0, ac.ConnectWireLengthForTest())
	})

	t.Run("credentials_fitting_total_wire_encode", func(t *testing.T) {
		_, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
			ctx.CredentialsSupplier = oversizedCredentialsSupplier{
				challengeLen: maxChallengeResponseLen,
			}
			return ctx
		})
		t.Cleanup(func() { _ = ac.Close() })
		ac.encodedChallenge = []byte("challenge")
		ac.clusterSessionID = egressTestClusterSessionID

		require.NoError(t, ac.PrepareChallengeResponseForTest())
		require.Equal(
			t,
			challengeResponseWireLen(maxChallengeResponseLen),
			ac.ConnectWireLengthForTest(),
		)
		require.LessOrEqual(t, ac.ConnectWireLengthForTest(), connectWireBufferSize)
	})
}

func TestAsyncConnect_encodedCredentialsTooLarge(t *testing.T) {
	harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
		ctx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: maxEncodedCredentialsLen + 1,
		}
		return ctx
	})
	t.Cleanup(func() { _ = ac.Close() })

	_, pollErr := pollAsyncConnectUntil(t, ac, harness, nil)
	require.Error(t, pollErr)
	require.ErrorContains(t, pollErr, "cluster client: encoded credentials exceed max length")
}

func TestAsyncConnect_challengeResponseCredentialsTooLarge(t *testing.T) {
	harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
		ctx.CredentialsSupplier = oversizedCredentialsSupplier{
			challengeLen: maxChallengeResponseLen + 1,
		}
		return ctx
	})
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(asyncConnectPollTimeout)
	var pollErr error
	challengeWritten := false
	for time.Now().Before(deadline) && pollErr == nil {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		harness.RespondPendingAdds()
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				if !challengeWritten && ac.State() == ConnectStatePollResponse {
					writeChallengeForConnect(t, termBuffer, correlation)
					challengeWritten = true
				}
			},
		)
		_, pollErr = ac.Poll()
	}
	require.True(t, challengeWritten)
	require.Error(t, pollErr)
	require.ErrorContains(
		t,
		pollErr,
		"cluster client: challenge response credentials exceed max length",
	)
}

func TestAsyncConnect_concludeConnect_leaderMissingFromIngressMap(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	const missingLeaderID int32 = 5
	_, pollErr := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeOKSessionEventForConnect(
					t,
					termBuffer,
					correlation,
					missingLeaderID,
					[]byte("missing-leader"),
				)
			},
		)
	})
	require.Error(t, pollErr)
	require.ErrorContains(
		t,
		pollErr,
		"cluster client: leader member id 5 missing from connect ingress map",
	)
}

func TestAsyncConnect_createIngressPublications_memberDriverErrorTolerance(t *testing.T) {
	const (
		member0RegID int64 = 3001
		member1RegID int64 = 3002
	)
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	harness.SetCountersReaderForRegs(t, member0RegID, member1RegID)
	t.Cleanup(func() { _ = ac.Close() })

	member1Log := aeron.MemberExclusiveLogPath(t, harness.MediaDriverDir, true)

	member0Rejected := false
	respondMember1 := func(h *aeron.AsyncConnectTestHarness) {
		h.RespondSubscriptionAndExclusiveMemberAdds([]aeron.ExclusiveMemberAdd{
			{
				EndpointSubstring: "host-b:20111",
				LogPath:           member1Log,
				RegistrationID:    member1RegID,
				CounterIndex:      1,
			},
		})
	}
	pollMember1Connect := func(ac *AsyncConnect) {
		if !member0Rejected {
			if regID := ac.MemberIngressRegIDForTest(0); regID != conductor.NullCorrelationID {
				require.NoError(
					t,
					harness.Fixture().Transmit(
						conductor.OnError,
						conductor.ErrorResponseBytes(regID, 11, "member 0 ingress rejected"),
					),
				)
				member0Rejected = true
				for range asyncConnectHarnessDoWorkN {
					harness.DoWork()
				}
			}
		}
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
				writeOKSessionEventForConnect(t, termBuffer, correlation, 1, []byte("member1-ok"))
			},
		)
	}
	result, err := pollAsyncConnectWithResponder(
		t, ac, harness, respondMember1, pollMember1Connect,
	)
	require.True(t, member0Rejected)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(1), result.LeaderMemberID)
	member1, ok := result.MemberIngressMap.Lookup(1)
	require.True(t, ok)
	require.NotNil(t, member1.Publication)
	require.Same(t, member1.Publication, result.IngressPublication)
}

func TestAsyncConnect_createIngressPublications_recoversWhenPeerAlreadyFailed(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)
	require.Equal(t, ConnectStateCreateIngressPublications, ac.State())

	member0, ok := ac.memberIngressForTest(0)
	require.True(t, ok)
	member0.PublicationErr = errors.New("member 0 ingress rejected")
	member0.RegistrationID = conductor.NullCorrelationID

	member1Pub := aeron.NewTestExclusivePublicationForHooks(t)
	memberIngressAsyncPollAddHook = func(
		ingress *MemberIngress,
		ctx ClusterContext,
		client *aeron.Client,
	) (bool, error) {
		require.Equal(t, int32(1), ingress.MemberID)
		ingress.RegistrationID = conductor.NullCorrelationID
		ingress.Publication = member1Pub
		return true, nil
	}
	t.Cleanup(func() { memberIngressAsyncPollAddHook = nil })

	_, err := ac.pollCreateIngressPublications()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())
	require.Same(t, member1Pub, ac.MemberIngressPublicationForTest(1))
}

func TestAsyncConnect_concludeConnect_normalizesPreV13Heartbeat(t *testing.T) {
	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writePreV13SessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.LeaderHeartbeatTimeoutNs)
}

func TestAsyncConnect_concludeConnect_notBlockedByDeadline(t *testing.T) {
	var now int64 = 1_000_000_000
	harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
		ctx.AeronContext.NanoClock = func() int64 { return now }
		ctx.MessageTimeout = time.Second
		return ctx
	})
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(asyncConnectPollTimeout)
	var result *ConnectResult
	var pollErr error
	sessionEventWritten := false
	for time.Now().Before(deadline) {
		for range asyncConnectHarnessDoWorkN {
			harness.DoWork()
		}
		harness.RespondPendingAdds()
		attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
		if !sessionEventWritten && ac.State() == ConnectStatePollResponse {
			withAsyncConnectEgressTerm(
				t,
				ac,
				func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
					writeSessionEventForConnect(t, termBuffer, correlation)
					sessionEventWritten = true
				},
			)
		}
		result, pollErr = ac.Poll()
		if ac.State() == ConnectStateConcludeConnect {
			now += 2 * time.Second.Nanoseconds()
			result, pollErr = ac.Poll()
			break
		}
		if result != nil || pollErr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, sessionEventWritten)
	require.NoError(t, pollErr)
	require.NotNil(t, result)
	require.Equal(t, ConnectStateDone, ac.State())
}

func TestAsyncConnect_happyPathNonExclusiveIngress(t *testing.T) {
	exclusive := false
	harness, ac := setupAsyncConnectWithContext(t, func(ctx ClusterContext) ClusterContext {
		ctx.IngressExclusive = &exclusive
		return ctx
	})
	t.Cleanup(func() { _ = ac.Close() })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	_, ok := result.IngressPublication.(*aeron.Publication)
	require.True(t, ok)
}

type oversizedCredentialsSupplier struct {
	encodedLen   int
	challengeLen int
}

func (supplier oversizedCredentialsSupplier) EncodedCredentials() []byte {
	if supplier.encodedLen == 0 {
		return nil
	}
	return make([]byte, supplier.encodedLen)
}

func (supplier oversizedCredentialsSupplier) RespondToChallenge([]byte) []byte {
	if supplier.challengeLen == 0 {
		return nil
	}
	return make([]byte, supplier.challengeLen)
}
