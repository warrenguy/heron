//go:build testhooks

package client

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

const (
	asyncConnectIPCChannel     = "aeron:ipc"
	asyncConnectPollTimeout    = 5 * time.Second
	asyncConnectHarnessDoWorkN = 4

	// Representative large credential payloads below codec layout caps.
	connectEncodedCredentialsPastLayoutMax  = 1245
	challengeResponseCredentialsAtLayoutMax = 1372

	// pollSendMessageOversizeWireCredentialsLen yields ~8500-byte connect wire (> pub max).
	pollSendMessageOversizeWireCredentialsLen = 8430

	connectRequestClientInfo    = "name=" + defaultClusterClientName + " " + clusterConnectClientLabel
	connectRequestClientInfoLen = len(connectRequestClientInfo)
	connectRequestIPCChannelLen = len(asyncConnectIPCChannel)

	// connectWireBufferCloseHighWaterCredentialsLen establishes cap > wireLen without Offer.
	connectWireBufferCloseHighWaterCredentialsLen = 1500
	// connectWireBufferPollHighWaterCredentialsLen must fit test-harness publication Offer.
	connectWireBufferPollHighWaterCredentialsLen = 200
	connectWireBufferSlackChallengeLen           = 32
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
	return setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		return baseAsyncConnectContext(clusterCtx.Aeron, ingressEndpoints)
	})
}

func setupAsyncConnectWithContext(
	t *testing.T,
	mutate func(ClusterContext) ClusterContext,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	clusterCtx := mutate(baseAsyncConnectContext(harness.Client, ""))
	ac, err := NewAsyncConnect(t.Context(), clusterCtx)
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
	fn func(*testing.T, atomicbuffer.AtomicBuffer, int64),
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
		clusterwire.SessionEventSbeSchemaVersion,
	)
}

func writeOKSessionEventWireToTerm(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
	write func(fragmentTestHelper, atomicbuffer.AtomicBuffer, []byte),
) {
	t.Helper()

	write(
		t,
		termBuffer,
		encodeOKSessionEventWireForConnect(t, correlationID, leaderMemberID, detail),
	)
}

func writeOKSessionEventForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
) {
	t.Helper()

	writeOKSessionEventWireToTerm(
		t,
		termBuffer,
		correlationID,
		leaderMemberID,
		detail,
		writeUnfragmentedWireToTerm,
	)
}

func appendOKSessionEventForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
	leaderMemberID int32,
	detail []byte,
) {
	t.Helper()

	writeOKSessionEventWireToTerm(
		t,
		termBuffer,
		correlationID,
		leaderMemberID,
		detail,
		appendUnfragmentedWireToTerm,
	)
}

func writeSessionEventWithHeartbeatForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
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
			clusterwire.SessionEventSbeSchemaVersion,
		),
	)
}

func asyncConnectHarnessStep(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
	respond asyncConnectHarnessResponder,
) {
	t.Helper()

	for range asyncConnectHarnessDoWorkN {
		harness.DoWork()
	}
	respond(harness)
	attachEgressImageIfNeeded(t, ac.EgressSubscriptionForTest())
}

func failAsyncConnectPollTimeout(t *testing.T, ac *AsyncConnect) {
	t.Helper()

	require.FailNowf(
		t,
		"async connect poll timed out",
		"state=%v step=%v correlation=%v",
		ac.State(),
		ac.Step(),
		ac.CorrelationIDForTest(),
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
		asyncConnectHarnessStep(t, harness, ac, respond)
		if onStep != nil {
			onStep(ac)
		}
		result, err := ac.Poll()
		if result != nil || err != nil {
			return result, err
		}
		time.Sleep(time.Millisecond)
	}
	failAsyncConnectPollTimeout(t, ac)
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
		asyncConnectHarnessStep(t, harness, ac, respond)
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.State() == ConnectStatePollResponse &&
			ac.CorrelationIDForTest() != conductor.NullCorrelationID {
			return
		}
		time.Sleep(time.Millisecond)
	}
	failAsyncConnectPollTimeout(t, ac)
}

type asyncConnectChallengeRoundTripState struct {
	challengeWritten         bool
	challengeResponseOffered bool
	sessionEventWritten      bool
}

func (state *asyncConnectChallengeRoundTripState) onStep(
	t *testing.T,
	ac *AsyncConnect,
) {
	t.Helper()

	if state.challengeWritten && ac.State() == ConnectStateSendMessage {
		state.challengeResponseOffered = true
	}
	withAsyncConnectEgressTerm(
		t,
		ac,
		func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
			switch {
			case !state.challengeWritten && ac.State() == ConnectStatePollResponse:
				writeChallengeForConnect(t, termBuffer, correlation)
				state.challengeWritten = true
			case state.challengeResponseOffered && !state.sessionEventWritten &&
				ac.State() == ConnectStatePollResponse:
				appendSessionEventForConnect(t, termBuffer, correlation)
				state.sessionEventWritten = true
			}
		},
	)
}

func requireConnectRequestWireLength(
	t *testing.T,
	ac *AsyncConnect,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) {
	t.Helper()

	wantWireLen, ok := codecs.SessionConnectRequestWireLen(
		clusterwire.SessionConnectRequestSbeSchemaVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	require.True(t, ok)
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())
}

func requireChallengeResponseWireLength(
	t *testing.T,
	ac *AsyncConnect,
	encodedCredentialsLen int,
) {
	t.Helper()

	wantWireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.ChallengeResponseSbeBlockLength,
		encodedCredentialsLen,
	)
	require.True(t, ok)
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())
}

func requireConnectWireSlackCleared(t *testing.T, backing []byte, wireLen int, msg string) {
	t.Helper()

	for i := wireLen; i < len(backing); i++ {
		require.Zero(t, backing[i], msg, i)
	}
}

func egressTermBufferForAsyncConnect(t *testing.T, ac *AsyncConnect) atomicbuffer.AtomicBuffer {
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
	t.Parallel()

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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(t, ac, writeSessionEventForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, egressTestClusterSessionID, result.ClusterSessionID)
	require.NotNil(t, result.IngressPublication)
	require.NotNil(t, result.EgressSubscription)
	require.False(t, result.OwnsAeronClient)
}

func TestAsyncConnect_challengeRoundTrip(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	var roundTrip asyncConnectChallengeRoundTripState
	result, pollErr := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		roundTrip.onStep(t, ac)
	})
	require.NoError(t, pollErr)
	require.NotNil(t, result)
	require.True(t, roundTrip.challengeWritten)
	require.True(t, roundTrip.challengeResponseOffered)
	require.True(t, roundTrip.sessionEventWritten)
	require.Equal(t, ConnectStateDone, ac.State())
}

func TestAsyncConnect_pollResponse_newLeaderWithActiveCorrelation(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
				func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	clusterCtx := baseAsyncConnectContext(harness.Client, "")
	clusterCtx.MessageTimeout = 5 * time.Millisecond
	ac, err := NewAsyncConnect(t.Context(), clusterCtx)
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

func TestAsyncConnect_handshakeTimeoutCloseReleasesEgress(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)
	require.NotNil(t, ac.EgressSubscriptionForTest())

	ac.deadlineNs = ac.nanoClock() - 1
	_, pollErr := ac.Poll()
	require.ErrorContains(t, pollErr, "cluster client: connect timeout")
	require.NotNil(t, ac.EgressSubscriptionForTest())

	require.NoError(t, ac.Close())
	_, err := ac.Poll()
	require.EqualError(t, err, "cluster client: async connect closed")
	require.Nil(t, ac.EgressSubscriptionForTest())
	require.False(t, aeron.ClientIsClosedForTest(harness.Client))
}

func TestNewAsyncConnect_udpEgressAppliesRejoinFalse(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	clusterCtx := baseAsyncConnectContext(harness.Client, "")
	clusterCtx.EgressChannel = asyncConnectUDPEgressWildcardChannel
	ac, err := NewAsyncConnect(t.Context(), clusterCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })
	require.Equal(
		t,
		asyncConnectUDPEgressWildcardWithRejoin,
		ac.ClusterContextForTest().EgressChannel,
	)
}

func TestConcludeClusterContext_invalidEgressChannel(t *testing.T) {
	t.Parallel()

	clusterCtx := baseAsyncConnectContext(nil, "")
	clusterCtx.EgressChannel = "not-a-channel"
	_, _, _, err := ConcludeClusterContextForTest(t.Context(), clusterCtx)
	require.EqualError(t, err, "cluster client: invalid egress channel: not-a-channel")
}

const (
	asyncConnectUDPEgressWildcardChannel    = "aeron:udp?endpoint=localhost:0"
	asyncConnectUDPEgressWildcardWithRejoin = asyncConnectUDPEgressWildcardChannel +
		"|rejoin=false"

	asyncConnectUDPEgressChannelStatusCounterID int32 = 3
	asyncConnectUDPEgressBindAddress                  = "127.0.0.1:54321"
	asyncConnectUDPEgressResolvedChannel              = "aeron:udp?endpoint=" +
		asyncConnectUDPEgressBindAddress + "|rejoin=false"
	errEgressCannotResolveResponseChannel = "cluster client: egress subscription cannot " +
		"resolve response channel"
)

func asyncConnectContextWithUDPEgressWildcard(clusterCtx ClusterContext) ClusterContext {
	clusterCtx.EgressChannel = asyncConnectUDPEgressWildcardChannel
	return clusterCtx
}

type asyncConnectUDPEgressCounters struct {
	reader *cnc.CountersReader
	meta   []byte
	values []byte
}

func newAsyncConnectUDPEgressCounters(
	t *testing.T,
	bindAddress string,
) *asyncConnectUDPEgressCounters {
	t.Helper()

	meta := make([]byte, cnc.CounterMetadataLength*5)
	values := make([]byte, cnc.CounterValueLength*5)
	cnc.WriteCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		cnc.DriverPublisherLimitTypeID,
		aeron.AsyncConnectTestIngressRegID,
		1<<40,
	)
	require.NoError(t, cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		1,
		cnc.CounterRecordAllocated,
		cnc.DriverSendChannelStatusTypeID,
		asyncConnectIPCChannel,
		aeron.AsyncConnectTestIngressRegID,
		cnc.ChannelEndpointStatusActive,
	))
	// Slot 2 must be allocated so CNC scan reaches subscription channel-status id 3.
	cnc.WriteCounterRecord(
		meta,
		values,
		2,
		cnc.CounterRecordAllocated,
		cnc.ClientHeartbeatTypeID,
		1,
		0,
	)
	require.NoError(t, cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		asyncConnectUDPEgressChannelStatusCounterID,
		cnc.CounterRecordAllocated,
		cnc.DriverReceiveChannelStatusTypeID,
		asyncConnectUDPEgressWildcardWithRejoin,
		aeron.TestHookSubscriptionRegistrationID,
		cnc.ChannelEndpointStatusActive,
	))
	if bindAddress != "" {
		require.NoError(t, cnc.WriteLocalSocketAddressCounterRecord(
			meta,
			values,
			4,
			cnc.CounterRecordAllocated,
			aeron.TestHookSubscriptionRegistrationID,
			asyncConnectUDPEgressChannelStatusCounterID,
			bindAddress,
			cnc.ChannelEndpointStatusActive,
		))
	}

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	return &asyncConnectUDPEgressCounters{
		reader: reader,
		meta:   meta,
		values: values,
	}
}

func (fixture *asyncConnectUDPEgressCounters) setBindAddress(
	t *testing.T,
	bindAddress string,
) {
	t.Helper()

	require.NoError(t, cnc.WriteLocalSocketAddressCounterRecord(
		fixture.meta,
		fixture.values,
		4,
		cnc.CounterRecordAllocated,
		aeron.TestHookSubscriptionRegistrationID,
		asyncConnectUDPEgressChannelStatusCounterID,
		bindAddress,
		cnc.ChannelEndpointStatusActive,
	))
}

func installAsyncConnectUDPEgressCounters(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	fixture *asyncConnectUDPEgressCounters,
) {
	t.Helper()

	harness.Fixture().Conductor.SetCountersReaderForTest(fixture.reader)
}

func setupAsyncConnectUDPEgressAwait(
	t *testing.T,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect, *asyncConnectUDPEgressCounters) {
	t.Helper()

	harness, ac := setupAsyncConnectWithContext(t, asyncConnectContextWithUDPEgressWildcard)
	t.Cleanup(func() { _ = ac.Close() })

	fixture := newAsyncConnectUDPEgressCounters(t, "")
	installAsyncConnectUDPEgressCounters(t, harness, fixture)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		harness.RespondPendingAdds()
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.State() == ConnectStateAwaitPublicationConnected {
			require.Same(
				t,
				fixture.reader,
				aeron.SubscriptionCountersForHooks(ac.EgressSubscriptionForTest()),
			)
			return harness, ac, fixture
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNow(t, "timed out waiting for AWAIT_PUBLICATION_CONNECTED")
	return nil, nil, nil
}

func setupAsyncConnectUDPEgressPrepareConnect(
	t *testing.T,
	bindAddress string,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness, ac := setupAsyncConnectWithContext(t, asyncConnectContextWithUDPEgressWildcard)
	t.Cleanup(func() { _ = ac.Close() })

	fixture := newAsyncConnectUDPEgressCounters(t, bindAddress)
	installAsyncConnectUDPEgressCounters(t, harness, fixture)
	setupAsyncConnectEgressReady(t, harness, ac)
	require.Same(t, fixture.reader, aeron.SubscriptionCountersForHooks(ac.EgressSubscriptionForTest()))
	return harness, ac
}

func setupAsyncConnectIPCPrepareConnect(
	t *testing.T,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)
	return harness, ac
}

func requireConnectRequestResponseChannel(t *testing.T, ac *AsyncConnect, want string) {
	t.Helper()

	decoded, ok := codecs.TryDecodeByTemplate(
		ac.ConnectWireBufferForTest()[:ac.ConnectWireLengthForTest()],
		0,
		codecs.ClusterDecodeMaxVarLen,
	)
	require.True(t, ok)
	fields, ok := decoded.Fields.(codecs.SessionConnectRequestFields)
	require.True(t, ok)
	require.Equal(t, want, string(fields.ResponseChannel))
}

func TestAsyncConnect_pollAwaitPublicationConnected_udpWildcardResolves(t *testing.T) {
	t.Parallel()

	harness, ac, fixture := setupAsyncConnectUDPEgressAwait(t)

	harness.DoWork()
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())

	fixture.setBindAddress(t, asyncConnectUDPEgressBindAddress)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		_, err = ac.Poll()
		require.NoError(t, err)
		if ac.State() == ConnectStateSendMessage {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, ConnectStateSendMessage, ac.State())
	requireConnectRequestResponseChannel(t, ac, asyncConnectUDPEgressResolvedChannel)
}

func TestAsyncConnect_prepareConnectRequest_udpUnresolvedResponseChannel(t *testing.T) {
	t.Parallel()

	_, ac := setupAsyncConnectUDPEgressPrepareConnect(t, "")

	err := ac.PrepareConnectRequestForTest()
	require.EqualError(t, err, errEgressCannotResolveResponseChannel)
}

func TestAsyncConnect_prepareConnectRequest_udpResolvedResponseChannel(t *testing.T) {
	t.Parallel()

	_, ac := setupAsyncConnectUDPEgressPrepareConnect(t, asyncConnectUDPEgressBindAddress)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	requireConnectRequestResponseChannel(t, ac, asyncConnectUDPEgressResolvedChannel)
}

func TestAsyncConnect_prepareConnectRequest_steadyEncode_ZeroAllocs(t *testing.T) {
	// Steady-state connect encode after warm-up; AllocsPerRun panics under t.Parallel.
	_, ac := setupAsyncConnectIPCPrepareConnect(t)

	require.NoError(t, ac.PrepareConnectRequestForTest()) // warm connectWireBuffer growth

	testutil.AssertZeroAllocs(t, "AsyncConnect_prepareConnectRequest", 1000, func() {
		if err := ac.PrepareConnectRequestForTest(); err != nil {
			panic(err)
		}
	})
}

func TestAsyncConnect_ownedConnectFailsWithoutDriver(t *testing.T) {
	t.Parallel()

	clusterCtx := ClusterContext{
		IngressChannel: asyncConnectIPCChannel,
		EgressChannel:  asyncConnectIPCChannel,
		AeronDir:       t.TempDir(),
		AeronContext: aeron.Context{
			// Match cncPollInterval; enough headroom under -race.
			DriverTimeout: 50 * time.Millisecond,
		},
	}
	_, err := NewAsyncConnect(t.Context(), clusterCtx)
	require.Error(t, err)
	require.ErrorContains(t, err, "aeron:")
}

func TestAsyncConnect_closeReleasesPartialResources(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	var firstOffer int64
	ac.SetEgressHooksForTest(
		t,
		func(
			c *aeron.Client,
			registrationID int64,
		) (*aeron.Subscription, bool, error) {
			if firstOffer == 0 {
				firstOffer = registrationID
				return nil, false, nil
			}
			return c.PollAddSubscription(registrationID)
		},
		func() bool { return true },
	)

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
	t.Parallel()

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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "")
	t.Cleanup(func() { _ = ac.Close() })

	const heartbeatNs = int64(250_000_000)
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})
	require.NoError(t, err)
	require.Equal(t, 2*time.Duration(heartbeatNs), result.Context.NewLeaderTimeout)
}

func TestAsyncConnect_awaitPublicationConnected_firstConnectedMember(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
				writeOKSessionEventForConnect(t, termBuffer, correlation, 0, []byte("multi-member-ok"))
			},
		)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(0), result.LeaderMemberID)
}

func TestAsyncConnect_successTransfersOwnershipWithoutClose(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	var ac *AsyncConnect
	_, err := ac.Poll()
	require.EqualError(t, err, "cluster client: nil async connect")

	harness, ac := setupAsyncConnect(t, "")
	require.NoError(t, ac.Close())
	_, err = ac.Poll()
	require.EqualError(t, err, "cluster client: async connect closed")

	ac, err = NewAsyncConnect(t.Context(), baseAsyncConnectContext(harness.Client, ""))
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
	t.Parallel()

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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})
	require.Equal(t, ConnectStatePollResponse, ac.State())
	correlation := ac.CorrelationIDForTest()
	writeSessionEventRedirectForConnect(
		t,
		egressTermBufferForAsyncConnect(t, ac),
		correlation,
		"1=host-c:20112",
	)
	_, err := ac.Poll()
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
	t.Parallel()

	cases := []struct {
		name       string
		code       clusterwire.EventCode
		detail     []byte
		wantPrefix string
	}{
		{
			name:       "authentication_rejected",
			code:       clusterwire.EventCode_AUTHENTICATION_REJECTED,
			detail:     []byte("bad credentials"),
			wantPrefix: "cluster client: authentication rejected",
		},
		{
			name:       "error",
			code:       clusterwire.EventCode_ERROR,
			detail:     []byte("connect failed"),
			wantPrefix: "cluster client: connect error",
		},
		{
			name:       "unknown_code",
			code:       clusterwire.EventCode(99),
			detail:     []byte("unknown"),
			wantPrefix: "cluster client: unexpected session event code 99",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness, ac := setupAsyncConnect(t, "")
			t.Cleanup(func() { _ = ac.Close() })

			_, pollErr := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
				if ac.State() != ConnectStatePollResponse {
					return
				}
				withAsyncConnectEgressTerm(
					t,
					ac,
					func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
								clusterwire.SessionEventSbeSchemaVersion,
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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	redirected := false
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

	const explicit = 9 * time.Second
	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.NewLeaderTimeout = explicit
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })

	const heartbeatNs = int64(250_000_000)
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
				writeSessionEventWithHeartbeatForConnect(t, termBuffer, correlation, heartbeatNs)
			},
		)
	})
	require.NoError(t, err)
	require.Equal(t, explicit, result.Context.NewLeaderTimeout)
}

func TestAsyncConnect_ownedClientClosedOnMemberMapError(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	aeronDir := t.TempDir()
	SetConnectConstructionHooksForTest(
		t,
		aeronDir,
		func(ClusterContext) (*aeron.Client, error) {
			return harness.Client, nil
		},
	)

	clusterCtx := ClusterContext{
		IngressChannel:   "aeron:udp?ttl=16",
		EgressChannel:    asyncConnectIPCChannel,
		IngressEndpoints: "not-valid-endpoints",
		AeronDir:         aeronDir,
		MessageTimeout:   time.Second,
	}
	_, err := NewAsyncConnect(t.Context(), clusterCtx)
	require.Error(t, err)
	require.ErrorContains(t, err, "cluster client:")
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
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

func TestAsyncConnect_prepareConnectRequest_credentialsSizing(t *testing.T) {
	t.Parallel()

	require.Equal(t, connectRequestIPCChannelLen, sessionConnectResponseChannelMaxLen)

	t.Run("encoded_credentials_large_encode_success", func(t *testing.T) {
		harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
			clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
				encodedLen: connectEncodedCredentialsPastLayoutMax,
			}
			return clusterCtx
		})
		t.Cleanup(func() { _ = ac.Close() })
		setupAsyncConnectEgressReady(t, harness, ac)

		require.NoError(t, ac.PrepareConnectRequestForTest())
		requireConnectRequestWireLength(
			t,
			ac,
			connectRequestIPCChannelLen,
			connectEncodedCredentialsPastLayoutMax,
			connectRequestClientInfoLen,
		)
	})
}

func TestAsyncConnect_prepareConnectRequest_largeCredentials(t *testing.T) {
	t.Parallel()

	const encodedCredentialsLen = 1500

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: encodedCredentialsLen,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	requireConnectRequestWireLength(
		t,
		ac,
		sessionConnectResponseChannelMaxLen,
		encodedCredentialsLen,
		connectRequestClientInfoLen,
	)
}

func TestAsyncConnect_prepareConnectRequest_credentialsNearPublicationMax(t *testing.T) {
	t.Parallel()

	const targetWireLen = 7000
	// Fixed overhead for default clientInfo + IPC channel.
	const encodedCredentialsLen = targetWireLen - 70

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: encodedCredentialsLen,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())

	wantWireLen, ok := codecs.SessionConnectRequestWireLen(
		clusterwire.SessionConnectRequestSbeSchemaVersion,
		sessionConnectResponseChannelMaxLen,
		encodedCredentialsLen,
		connectRequestClientInfoLen,
	)
	require.True(t, ok)
	require.InDelta(t, targetWireLen, wantWireLen, 32)
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())
}

func TestAsyncConnect_prepareChallengeResponse_credentialsSizing(t *testing.T) {
	t.Parallel()

	t.Run("encoded_credentials_large_encode_success", func(t *testing.T) {
		_, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
			clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
				challengeLen: challengeResponseCredentialsAtLayoutMax,
			}
			return clusterCtx
		})
		t.Cleanup(func() { _ = ac.Close() })
		ac.encodedChallenge = []byte("challenge")
		ac.clusterSessionID = egressTestClusterSessionID

		require.NoError(t, ac.PrepareChallengeResponseForTest())
		requireChallengeResponseWireLength(t, ac, challengeResponseCredentialsAtLayoutMax)
	})
}

func TestAsyncConnect_pollSendMessage_oversizeWireFailsFast(t *testing.T) {
	t.Parallel()

	pub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)
	maxMessageLength := pub.MaxMessageLengthForTest()
	require.Equal(t, 8192, maxMessageLength)

	wantWireLen, ok := codecs.SessionConnectRequestWireLen(
		clusterwire.SessionConnectRequestSbeSchemaVersion,
		connectRequestIPCChannelLen,
		pollSendMessageOversizeWireCredentialsLen,
		connectRequestClientInfoLen,
	)
	require.True(t, ok)
	require.Greater(t, wantWireLen, maxMessageLength)
	require.Less(t, wantWireLen, logbuffer.MaxMessageLength)

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: pollSendMessageOversizeWireCredentialsLen,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())

	ac.ingressPublication = pub
	ac.state = ConnectStateSendMessage

	start := time.Now()
	_, err := ac.PollSendMessageForTest()
	require.Error(t, err)
	require.ErrorContains(t, err, "max message length")
	require.Less(
		t,
		time.Since(start),
		time.Second,
		"oversize wire should fail fast, not retry until timeout",
	)
}

func TestAsyncConnect_prepareConnectRequest_reachesPollResponse(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: connectEncodedCredentialsPastLayoutMax,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)

	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})
	requireConnectRequestWireLength(
		t,
		ac,
		connectRequestIPCChannelLen,
		connectEncodedCredentialsPastLayoutMax,
		connectRequestClientInfoLen,
	)
}

func TestAsyncConnect_resizeConnectWireBuffer_clearsAbandonedBackingOnGrow(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })

	setupAsyncConnectEgressReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	buf := ac.ConnectWireBufferForTest()
	require.NotNil(t, buf)
	oldBacking := buf[:cap(buf)]
	smallWireLen := ac.ConnectWireLengthForTest()
	require.Equal(t, len(oldBacking), smallWireLen, "small connect should not over-allocate")

	const sentinel = 0xCC
	for i := range oldBacking {
		oldBacking[i] = sentinel
	}

	ac.clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
		challengeLen: connectWireBufferCloseHighWaterCredentialsLen,
	}
	ac.encodedChallenge = []byte("challenge")
	ac.clusterSessionID = egressTestClusterSessionID
	require.NoError(t, ac.PrepareChallengeResponseForTest())

	newBuf := ac.ConnectWireBufferForTest()
	require.NotNil(t, newBuf)
	require.Greater(t, cap(newBuf), cap(oldBacking))

	for i := range oldBacking {
		require.Zero(t, oldBacking[i], "abandoned backing byte %d not cleared on grow", i)
	}
}

func TestAsyncConnect_prepareChallengeResponse_clearsConnectWireBufferSlack(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: connectWireBufferCloseHighWaterCredentialsLen,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })

	backing, smallWireLen := establishConnectWireBufferHighWater(t, harness, ac)
	requireConnectWireSlackCleared(
		t,
		backing,
		smallWireLen,
		"slack byte %d retained prior connect credentials",
	)
}

func establishConnectWireBufferHighWater(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
) (backing []byte, activeWireLen int) {
	t.Helper()

	setupAsyncConnectEgressReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	buf := ac.ConnectWireBufferForTest()
	require.NotNil(t, buf)
	backing = buf[:cap(buf)]
	largeWireLen := ac.ConnectWireLengthForTest()

	ac.clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
		challengeLen: connectWireBufferSlackChallengeLen,
	}
	ac.encodedChallenge = []byte("challenge")
	ac.clusterSessionID = egressTestClusterSessionID
	require.NoError(t, ac.PrepareChallengeResponseForTest())

	activeWireLen = ac.ConnectWireLengthForTest()
	require.Greater(t, len(backing), activeWireLen)
	require.Less(t, activeWireLen, largeWireLen)

	return backing, activeWireLen
}

func plantConnectWireBufferSlackSentinels(backing []byte, wireLen int, sentinel byte) {
	for i := wireLen; i < len(backing); i++ {
		backing[i] = sentinel
	}
}

func captureConnectWireBufferSlackOnce(
	planted *bool,
	ac *AsyncConnect,
	challengeWritten bool,
	sentinel byte,
) (backing []byte, slackWireLen int) {
	if *planted || !challengeWritten {
		return nil, 0
	}
	buf := ac.ConnectWireBufferForTest()
	if buf == nil {
		return nil, 0
	}
	capLen := cap(buf)
	wireLen := ac.ConnectWireLengthForTest()
	if capLen <= wireLen || wireLen <= 0 {
		return nil, 0
	}
	backing = buf[:capLen]
	plantConnectWireBufferSlackSentinels(backing, wireLen, sentinel)
	*planted = true
	return backing, wireLen
}

func TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnClose(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen: connectWireBufferCloseHighWaterCredentialsLen,
		}
		return clusterCtx
	})

	backing, wireLen := establishConnectWireBufferHighWater(t, harness, ac)
	plantConnectWireBufferSlackSentinels(backing, wireLen, 0xCC)

	require.NoError(t, ac.Close())
	requireConnectWireSlackCleared(t, backing, wireLen, "slack byte %d not cleared on Close")
	require.Nil(t, ac.ConnectWireBufferForTest())
}

func TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnSuccess(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.CredentialsSupplier = oversizedCredentialsSupplier{
			encodedLen:   connectWireBufferPollHighWaterCredentialsLen,
			challengeLen: connectWireBufferSlackChallengeLen,
		}
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)

	var backing []byte
	var slackWireLen int
	sentinelsPlanted := false
	var roundTrip asyncConnectChallengeRoundTripState

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		if capturedBacking, capturedWireLen := captureConnectWireBufferSlackOnce(
			&sentinelsPlanted,
			ac,
			roundTrip.challengeWritten,
			0xDD,
		); capturedBacking != nil {
			backing = capturedBacking
			slackWireLen = capturedWireLen
		}
		if roundTrip.sessionEventWritten {
			return
		}
		roundTrip.onStep(t, ac)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, roundTrip.challengeWritten)
	require.True(t, roundTrip.challengeResponseOffered)
	require.True(t, roundTrip.sessionEventWritten)
	require.True(t, sentinelsPlanted)
	require.NotNil(t, backing)
	requireConnectWireSlackCleared(t, backing, slackWireLen, "slack byte %d not cleared on success")
	require.Nil(t, ac.ConnectWireBufferForTest())
}

func TestAsyncConnect_concludeConnect_leaderMissingFromIngressMap(t *testing.T) {
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })

	const missingLeaderID int32 = 5
	_, pollErr := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectEgressTerm(
			t,
			ac,
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

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
			func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

	harness, ac := setupAsyncConnect(t, "0=host-a:20110,1=host-b:20111")
	t.Cleanup(func() { _ = ac.Close() })
	setupAsyncConnectEgressReady(t, harness, ac)
	require.Equal(t, ConnectStateCreateIngressPublications, ac.State())

	member0, ok := ac.memberIngressForTest(0)
	require.True(t, ok)
	member0.PublicationErr = errors.New("member 0 ingress rejected")
	member0.RegistrationID = conductor.NullCorrelationID

	member1, ok := ac.memberIngressForTest(1)
	require.True(t, ok)
	member1Pub := aeron.NewTestExclusivePublicationForHooks(t)
	member1.SetAsyncPollAddHookForTest(
		t,
		func(
			ingress *MemberIngress,
			clusterCtx ClusterContext,
			client *aeron.Client,
		) (bool, error) {
			require.Equal(t, int32(1), ingress.MemberID)
			ingress.RegistrationID = conductor.NullCorrelationID
			ingress.Publication = member1Pub
			return true, nil
		},
	)

	_, err := ac.pollCreateIngressPublications()
	require.NoError(t, err)
	require.Equal(t, ConnectStateAwaitPublicationConnected, ac.State())
	require.Same(t, member1Pub, ac.MemberIngressPublicationForTest(1))
}

func TestAsyncConnect_concludeConnect_normalizesPreV13Heartbeat(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	var now int64 = 1_000_000_000
	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.AeronContext.NanoClock = func() int64 { return now }
		clusterCtx.MessageTimeout = time.Second
		return clusterCtx
	})
	t.Cleanup(func() { _ = ac.Close() })

	deadline := time.Now().Add(asyncConnectPollTimeout)
	var result *ConnectResult
	var pollErr error
	sessionEventWritten := false
	for time.Now().Before(deadline) {
		asyncConnectHarnessStep(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
			h.RespondPendingAdds()
		})
		if !sessionEventWritten && ac.State() == ConnectStatePollResponse {
			withAsyncConnectEgressTerm(
				t,
				ac,
				func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
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
	t.Parallel()

	exclusive := false
	harness, ac := setupAsyncConnectWithContext(t, func(clusterCtx ClusterContext) ClusterContext {
		clusterCtx.IngressExclusive = &exclusive
		return clusterCtx
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
