//go:build integration && cluster_java

package cluster_java_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

func TestClusterComposeDirOverride(t *testing.T) {
	t.Setenv(clusterjava.ComposeDirEnvKey, "/custom/compose")
	require.Equal(t, "/custom/compose", clusterComposeDir())
}
