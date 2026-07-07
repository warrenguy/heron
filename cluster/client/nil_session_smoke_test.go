package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNilSession_smoke(t *testing.T) {
	var ac *AeronCluster
	require.True(t, ac.IsClosed())
	require.Zero(t, ac.ClusterSessionID())
	require.Zero(t, ac.LeadershipTermID())
	require.Zero(t, ac.LeaderMemberID())
	require.Nil(t, ac.MemberIngressMap())
	require.Nil(t, ac.IngressPublication())
	require.Nil(t, ac.EgressSubscription())
	require.Equal(t, ClusterContext{}, ac.Context())
	require.Nil(t, ac.Aeron())
	require.Equal(t, SessionStateConnected, ac.SessionState())

	require.Zero(t, ac.SpinOnce())

	var offerCalls int
	pos, err := ac.IngressOfferUntilPositive(t.Context(), IngressOfferConfig{}, func() int64 {
		offerCalls++
		return 1
	})
	require.ErrorIs(t, err, errNilIngressSession)
	require.Zero(t, pos)
	require.Zero(t, offerCalls)

	pos, err = ac.SendKeepAliveUntilPositive(t.Context(), IngressOfferConfig{})
	require.ErrorIs(t, err, errNilIngressSession)
	require.Zero(t, pos)
}
