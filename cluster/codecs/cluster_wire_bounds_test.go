package codecs

import (
	"testing"

	"github.com/stretchr/testify/require"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"
)

func TestClusterWireBodyScratchCap_bounds(t *testing.T) {
	wireTemplates := []struct {
		templateID uint16
		version    uint16
	}{
		{gensbe.SessionMessageHeaderSbeTemplateID, gensbe.SessionMessageHeaderSbeSchemaVersion},
		{gensbe.SessionEventSbeTemplateID, gensbe.SessionEventSbeSchemaVersion},
		{gensbe.SessionConnectRequestSbeTemplateID, gensbe.SessionConnectRequestSbeSchemaVersion},
		{gensbe.SessionCloseRequestSbeTemplateID, gensbe.SessionCloseRequestSbeSchemaVersion},
		{gensbe.SessionKeepAliveSbeTemplateID, gensbe.SessionKeepAliveSbeSchemaVersion},
		{gensbe.NewLeaderEventSbeTemplateID, gensbe.NewLeaderEventSbeSchemaVersion},
		{gensbe.ChallengeSbeTemplateID, gensbe.ChallengeSbeSchemaVersion},
		{gensbe.ChallengeResponseSbeTemplateID, gensbe.ChallengeResponseSbeSchemaVersion},
		{gensbe.AdminRequestSbeTemplateID, gensbe.AdminRequestSbeSchemaVersion},
		{gensbe.AdminResponseSbeTemplateID, gensbe.AdminResponseSbeSchemaVersion},
	}

	var maxRoot uint16
	var maxVarCount, maxRequired int
	for _, tmpl := range wireTemplates {
		_, maxBlock, ok := ClusterRootBlockLengthBounds(tmpl.templateID, tmpl.version)
		require.True(t, ok, "templateID=%d", tmpl.templateID)
		maxRoot = max(maxRoot, maxBlock)

		varCount, ok := ClusterVarFieldCount(tmpl.templateID, tmpl.version)
		require.True(t, ok, "templateID=%d", tmpl.templateID)
		maxVarCount = max(maxVarCount, varCount)

		required := int(maxBlock) + varCount*(VarLengthPrefixSize+ClusterMaxVarLen)
		maxRequired = max(maxRequired, required)
	}

	require.Equal(t, clusterWireMaxRootBlockLength, maxRoot)
	require.Equal(t, clusterWireMaxVarFieldCount, maxVarCount)
	require.LessOrEqual(t, maxRequired, int(clusterWireBodyScratchCap))
}
