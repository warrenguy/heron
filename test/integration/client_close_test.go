//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestClientCloseLifecycle(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.NoError(t, client.Close())
}
