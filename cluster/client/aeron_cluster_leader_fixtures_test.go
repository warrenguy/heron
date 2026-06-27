//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

var ingressPlanErrorCases = []struct {
	name      string
	endpoints []byte
	wantErr   string
}{
	{
		name:      "parse_error",
		endpoints: []byte("x=host:1"),
		wantErr:   "invalid ingress member id",
	},
	{
		name:      "leader_missing_in_new_map",
		endpoints: []byte("0=host-a:20110"),
		wantErr:   "member id 1 not in ingress map",
	},
}

var multiMemberEndpointsAC = []byte("0=host-a:20110,1=host-c:20113")

// fixedOfferResultIngressPublication wraps a real publication but returns a fixed
// result on offer paths so trackIngressPublicationResult can be exercised.
type fixedOfferResultIngressPublication struct {
	IngressPublication
	offerResult int64
}

func (pub fixedOfferResultIngressPublication) Offer(buffer []byte) int64 {
	return pub.offerResult
}

func (pub fixedOfferResultIngressPublication) OfferWithOffset(
	buffer []byte,
	offset, length int,
) int64 {
	return pub.Offer(buffer)
}

func (pub fixedOfferResultIngressPublication) TryClaim(
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	return 0, 0, pub.offerResult, nil
}

// fixedMaxPayloadIngressPublication overrides payload limits for scratch resize tests.
type fixedMaxPayloadIngressPublication struct {
	IngressPublication
	maxPayload int
}

func (pub fixedMaxPayloadIngressPublication) MaxPayloadLength() int {
	return pub.maxPayload
}

func (pub fixedMaxPayloadIngressPublication) MaxMessageLength() int {
	return SessionHeaderLength() + pub.maxPayload
}

// connectedButClosedIngressPublication reports connected for recovery polling but
// closed for offerSessionKeepAlive's early-return guard.
type connectedButClosedIngressPublication struct {
	IngressPublication
}

func (pub connectedButClosedIngressPublication) IsConnected() bool {
	return true
}

func (pub connectedButClosedIngressPublication) IsClosed() bool {
	return true
}

func enterChannelOnlyAwaitNewLeaderConnection(t *testing.T, ac *AeronCluster) *MemberIngress {
	t.Helper()

	err := ac.handleNewLeader(
		ac.ClusterSessionID(),
		connectFixtureLeadershipTermID+1,
		connectFixtureLeaderMemberID,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Nil(t, ac.IngressPublication())

	leader, ok := ac.MemberIngressMap().Lookup(channelOnlyMemberID)
	require.True(t, ok)
	return leader
}

func channelOnlyAwaitNewLeaderConnectionFixture(
	t *testing.T,
	mutate func(*ConnectResult),
) (*AeronCluster, *MemberIngress) {
	t.Helper()

	ac := newAeronClusterFromConnectFixture(t, mutate)
	leader := enterChannelOnlyAwaitNewLeaderConnection(t, ac)
	return ac, leader
}

// sequentialOfferResultIngressPublication returns scripted offer results for retry tests.
//
// Offer-only scripting; TryClaim delegates to the embedded IngressPublication.
//
// After the scripted slice is consumed, the final entry repeats (clamp semantics).
type sequentialOfferResultIngressPublication struct {
	IngressPublication
	results []int64
	calls   int
}

func (p *sequentialOfferResultIngressPublication) Offer([]byte) int64 {
	r := p.results[min(p.calls, len(p.results)-1)]
	p.calls++
	return r
}

// OfferWithOffset delegates to Offer so offset-based offers share the call counter.
func (p *sequentialOfferResultIngressPublication) OfferWithOffset(
	buffer []byte,
	offset, length int,
) int64 {
	return p.Offer(buffer)
}

// newFixedOfferResultIngressPublication builds a stateless fixture (value receiver).
func newFixedOfferResultIngressPublication(
	t *testing.T,
	offerResult int64,
) fixedOfferResultIngressPublication {
	t.Helper()

	return fixedOfferResultIngressPublication{
		IngressPublication: aeron.NewTestExclusivePublicationForHooks(t),
		offerResult:        offerResult,
	}
}

// newSequentialOfferResultIngressPublication builds a stateful fixture (pointer receiver).
func newSequentialOfferResultIngressPublication(
	t *testing.T,
	results ...int64,
) *sequentialOfferResultIngressPublication {
	t.Helper()

	require.NotEmpty(
		t,
		results,
		"sequentialOfferResultIngressPublication requires at least one result",
	)

	return &sequentialOfferResultIngressPublication{
		IngressPublication: aeron.NewTestExclusivePublicationForHooks(t),
		results:            results,
	}
}

func setLeaderFixedOfferResult(t *testing.T, leader *MemberIngress, offerResult int64) {
	t.Helper()

	leader.Publication = newFixedOfferResultIngressPublication(t, offerResult)
}

func channelOnlyAwaitWithSequentialOffers(
	t *testing.T,
	results ...int64,
) (*AeronCluster, *sequentialOfferResultIngressPublication) {
	t.Helper()

	ac, leader := channelOnlyAwaitNewLeaderConnectionFixture(t, nil)
	seqPub := newSequentialOfferResultIngressPublication(t, results...)
	leader.Publication = seqPub
	return ac, seqPub
}

// installConnectResultIngress configures channel-only ConnectResult fixtures.
//
// MemberIngressMap aliases pub so IngressPublication and member-0 share one publication.
func installConnectResultIngress(result *ConnectResult, pub IngressPublication) {
	result.IngressPublication = pub
	result.MemberIngressMap = channelOnlyIngressMap(pub)
}

func withSequentialOfferResultIngress(
	t *testing.T,
	result *ConnectResult,
	results ...int64,
) *sequentialOfferResultIngressPublication {
	t.Helper()

	seqPub := newSequentialOfferResultIngressPublication(t, results...)
	installConnectResultIngress(result, seqPub)
	return seqPub
}

func requirePollLeaderRecoveryZeroWork(t *testing.T, ac *AeronCluster) {
	t.Helper()

	require.Equal(t, 0, ac.PollEgress())
	require.Equal(t, SessionStateAwaitNewLeaderConnection, ac.SessionState())
	require.Nil(t, ac.IngressPublication())
}

func withFixedOfferResultIngress(t *testing.T, result *ConnectResult, offerResult int64) {
	t.Helper()

	installConnectResultIngress(result, newFixedOfferResultIngressPublication(t, offerResult))
}

func newMultiMemberAeronClusterInvalidIngress(
	t *testing.T,
) (*AeronCluster, *recordingEgressHandlers) {
	t.Helper()

	leaderPub := aeron.NewTestExclusivePublicationForHooks(t)
	result := newMultiMemberConnectResult(t, connectFixtureLeaderMemberID, leaderPub)
	installConnectFixtureEgressHandler(result, func(r *ConnectResult) {
		mutateConnectContext(r, func(ctx *ClusterContext) {
			ctx.IngressChannel = invalidIngressClusterContext().IngressChannel
		})
	})
	rec, ok := result.Context.EgressHandler.(*recordingEgressHandlers)
	require.True(t, ok)
	return newAeronClusterFromConnectResult(t, result), rec
}

type sessionRecoverySnapshot struct {
	state           SessionState
	stateDeadlineNs int64
	leaderID        int32
	termID          int64
	ingress         IngressPublication
	memberMap       MemberIngressMap
	egressImage     *aeron.Image
}

func cloneMemberIngressMapForSnapshot(m MemberIngressMap) MemberIngressMap {
	if m == nil {
		return nil
	}
	out := make(MemberIngressMap, len(m))
	for id, ingress := range m {
		if ingress == nil {
			out[id] = nil
			continue
		}
		copied := *ingress
		out[id] = &copied
	}
	return out
}

func snapshotSessionRecovery(ac *AeronCluster) sessionRecoverySnapshot {
	return sessionRecoverySnapshot{
		state:           ac.SessionState(),
		stateDeadlineNs: ac.stateDeadlineNs,
		leaderID:        ac.LeaderMemberID(),
		termID:          ac.LeadershipTermID(),
		ingress:         ac.IngressPublication(),
		memberMap:       cloneMemberIngressMapForSnapshot(ac.MemberIngressMap()),
		egressImage:     ac.egressImage,
	}
}

func requireMemberIngressRecoveryUnchanged(
	t testing.TB,
	before, after *MemberIngress,
) {
	t.Helper()

	if before == nil {
		require.Nil(t, after)
		return
	}
	require.NotNil(t, after)
	require.Equal(t, before.MemberID, after.MemberID)
	require.Equal(t, before.Endpoint, after.Endpoint)
	require.Equal(t, before.RegistrationID, after.RegistrationID)
	require.Same(t, before.Publication, after.Publication)
	require.Equal(t, before.PublicationErr, after.PublicationErr)
}

func requireMemberIngressMapRecoveryUnchanged(
	t testing.TB,
	before, after MemberIngressMap,
) {
	t.Helper()

	require.Len(t, after, len(before))
	for id, beforeIngress := range before {
		afterIngress, ok := after.Lookup(id)
		require.True(t, ok, "member %d missing from ingress map", id)
		requireMemberIngressRecoveryUnchanged(t, beforeIngress, afterIngress)
	}
}

func requireSessionRecoveryUnchanged(
	t testing.TB,
	before sessionRecoverySnapshot,
	ac *AeronCluster,
) {
	t.Helper()

	require.Equal(t, before.state, ac.SessionState())
	require.Equal(t, before.stateDeadlineNs, ac.stateDeadlineNs)
	require.Equal(t, before.leaderID, ac.LeaderMemberID())
	require.Equal(t, before.termID, ac.LeadershipTermID())
	require.Same(t, before.ingress, ac.IngressPublication())
	requireMemberIngressMapRecoveryUnchanged(t, before.memberMap, ac.MemberIngressMap())
	require.Same(t, before.egressImage, ac.egressImage)
	if before.ingress != nil {
		require.False(t, before.ingress.IsClosed())
	}
}

func newMultiMemberConnectResult(
	t *testing.T,
	leaderMemberID int32,
	leaderPub IngressPublication,
) *ConnectResult {
	t.Helper()

	result := newConnectResultWithEgressSubscription(t)
	result.LeaderMemberID = leaderMemberID
	result.IngressPublication = leaderPub
	result.MemberIngressMap = MemberIngressMap{
		channelOnlyMemberID: &MemberIngress{
			MemberID:    channelOnlyMemberID,
			Endpoint:    "host-a:20110",
			Publication: aeron.NewTestExclusivePublicationForHooks(t),
		},
		leaderMemberID: &MemberIngress{
			MemberID:    leaderMemberID,
			Endpoint:    "host-b:20111",
			Publication: leaderPub,
		},
	}
	return result
}

func testIngressPlanErrorNoMutation(
	t *testing.T,
	invoke func(ac *AeronCluster, endpoints []byte) error,
) {
	t.Helper()

	for _, tc := range ingressPlanErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			ac := newAeronClusterFromConnectFixture(t, nil)
			before := snapshotSessionRecovery(ac)

			err := invoke(ac, tc.endpoints)
			require.ErrorContains(t, err, tc.wantErr)
			requireSessionRecoveryUnchanged(t, before, ac)
		})
	}
}
