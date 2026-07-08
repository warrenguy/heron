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

	ctx := config.clusterContext(transport, handlers)

	require.Equal(t, config.AeronDir, ctx.AeronDir)
	require.Equal(t, transport, ctx.Aeron)
	require.Equal(t, config.IngressChannel, ctx.IngressChannel)
	require.Equal(t, config.EgressChannel, ctx.EgressChannel)
	require.Equal(t, config.IngressEndpoints, ctx.IngressEndpoints)
	require.Equal(t, handlers, ctx.EgressHandler)
	require.Equal(t, "cluster-auction-client", ctx.ClientName)
	require.Zero(t, ctx.NewLeaderTimeout, "must not pre-normalize before Connect")
	require.Zero(t, ctx.MessageTimeout)
	require.Zero(t, ctx.IngressStreamID)
	require.Nil(t, ctx.IdleStrategy)
}
