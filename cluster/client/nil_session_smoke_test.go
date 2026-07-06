package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
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
	pos := ac.IngressOfferUntilPositive(IngressOfferConfig{}, func() int64 {
		offerCalls++
		return 1
	})
	require.Equal(t, aeron.NotConnected, pos)
	require.Zero(t, offerCalls)

	require.Equal(t, aeron.NotConnected, ac.SendKeepAliveUntilPositive(IngressOfferConfig{}))
}
