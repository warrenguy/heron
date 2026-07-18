//go:build example

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
)

func TestClusterConfig_connectReadyContract(t *testing.T) {
	config := Config{
		AeronDir:         "/tmp/aeron-auction",
		IngressChannel:   "aeron:udp",
		EgressChannel:    "aeron:udp?endpoint=localhost:0",
		IngressEndpoints: "0=localhost:9002",
	}
	transport := &aeron.Client{}
	handlers := &client.EgressHandlers{}

	cfg := config.clusterConfig(transport, handlers)

	require.Equal(t, config.AeronDir, cfg.AeronDir)
	require.Equal(t, transport, cfg.Aeron)
	require.Equal(t, config.IngressChannel, cfg.IngressChannel)
	require.Equal(t, config.EgressChannel, cfg.EgressChannel)
	require.Equal(t, config.IngressEndpoints, cfg.IngressEndpoints)
	require.Equal(t, handlers, cfg.EgressHandler)
	require.Equal(t, "cluster-auction-client", cfg.ClientName)
	require.Zero(t, cfg.NewLeaderTimeout, "must not pre-normalize before Connect")
	require.Zero(t, cfg.MessageTimeout)
	require.Zero(t, cfg.IngressStreamID)
	require.Nil(t, cfg.IdleStrategy)
}
