//go:build integration && cluster_sbe

package clustersbe

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestOracleVarFieldLens_matchesClusterVarFieldCount(t *testing.T) {
	for _, fixture := range Fixtures() {
		t.Run(fixture.Name, func(t *testing.T) {
			varLens, ok := oracleVarFieldLens(fixture)
			require.True(t, ok)
			want, ok := codecs.ClusterVarFieldCount(fixture.TemplateID, fixture.ActingVersion)
			require.True(t, ok)
			require.Len(t, varLens, want)
		})
	}
}
