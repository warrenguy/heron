//go:build example

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
)

func TestClusterContext_connectReadyContract(t *testing.T) {
	config := Config{
		AeronDir:         "/tmp/aeron-auction",
		IngressChannel:   "aeron:udp",
		EgressChannel:    "aeron:udp?endpoint=localhost:0",
		IngressEndpoints: "0=localhost:9002",
	}
	transport := &aeron.Client{}
	handlers := &client.EgressHandlers{}

	clusterCtx := config.clusterContext(transport, handlers)

	require.Equal(t, config.AeronDir, clusterCtx.AeronDir)
	require.Equal(t, transport, clusterCtx.Aeron)
	require.Equal(t, config.IngressChannel, clusterCtx.IngressChannel)
	require.Equal(t, config.EgressChannel, clusterCtx.EgressChannel)
	require.Equal(t, config.IngressEndpoints, clusterCtx.IngressEndpoints)
	require.Equal(t, handlers, clusterCtx.EgressHandler)
	require.Equal(t, "cluster-auction-client", clusterCtx.ClientName)
	require.Zero(t, clusterCtx.NewLeaderTimeout, "must not pre-normalize before Connect")
	require.Zero(t, clusterCtx.MessageTimeout)
	require.Zero(t, clusterCtx.IngressStreamID)
	require.Nil(t, clusterCtx.IdleStrategy)
}
