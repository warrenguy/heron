//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

type memberToggles struct {
	control cluster.Toggle
	node    cluster.Toggle
}

func findMemberToggles(t *testing.T, sc serviceCluster) []memberToggles {
	t.Helper()

	modules := sc.set.Modules()
	toggles := make([]memberToggles, len(modules))
	for i, m := range modules {
		reader, err := m.Client().CountersReader()
		require.NoError(t, err)
		control, ok := cluster.FindControlToggle(reader, 0)
		require.True(t, ok, "member %d control toggle:\n%s", i, sc.dump())
		node, ok := cluster.FindNodeControlToggle(reader, 0)
		require.True(t, ok, "member %d node toggle:\n%s", i, sc.dump())
		requireToggleKeyAndLabel(t, reader, control, "Cluster control toggle - clusterId=0")
		requireToggleKeyAndLabel(t, reader, node, "Node control toggle - clusterId=0")
		toggles[i] = memberToggles{control: control, node: node}
	}
	return toggles
}

func requireToggleKeyAndLabel(
	t *testing.T,
	reader *cnc.CountersReader,
	toggle cluster.Toggle,
	wantLabel string,
) {
	t.Helper()

	key, ok := reader.CounterKey(toggle.CounterID())
	require.True(t, ok)
	require.Equal(t, [cnc.CounterMetadataMaxKeyLength]byte{}, key)
	label, ok := reader.CounterLabel(toggle.CounterID())
	require.True(t, ok)
	require.Equal(t, wantLabel, label)
}

// TestClusterControl_actsOnTogglesThroughTheLog drives the toggles on three live members.
func TestClusterControl_actsOnTogglesThroughTheLog(t *testing.T) {
	t.Parallel()

	sc := startServiceCluster(
		t,
		harness.ClusterControlPortBase,
		harness.ClusterControlLogEndpoint,
		amendServiceMember,
	)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	requireServicesJoined(t, sc)

	// Phase: the leader's control toggle reads NEUTRAL. Follower control
	// toggles read INACTIVE. Node toggles read NEUTRAL.
	toggles := findMemberToggles(t, sc)
	neutral := int64(consensus.ControlToggleNeutral)
	inactive := int64(consensus.ControlToggleInactive)
	nodeNeutral := int64(consensus.NodeControlToggleNeutral)
	leaderIndex := int(leader.ThisMemberID())
	requireServiceAwaitTick(
		t, sc,
		"toggles after election",
		durableWorkBound,
		func() bool {
			for i, toggle := range toggles {
				wantControl := inactive
				if i == leaderIndex {
					wantControl = neutral
				}
				if toggle.control.Load() != wantControl || toggle.node.Load() != nodeNeutral {
					return false
				}
			}
			return true
		},
	)

	// Phase: SNAPSHOT on the leader's toggle makes every member take a
	// snapshot at one log position. The toggle then returns to NEUTRAL.
	leaderToggle := toggles[leaderIndex].control
	require.True(t, consensus.ControlToggleSnapshot.Toggle(leaderToggle), sc.dump())
	requireServiceAwaitTick(
		t, sc,
		"snapshot toggle on every member",
		durableWorkBound,
		func() bool {
			requireNoServiceFailures(t, sc, " during snapshot")
			result, ok := leader.Agent().LastSnapshotTake()
			if !ok {
				return false
			}
			return snapshotTakenOnEveryMember(t, set, result.LogPosition, sc.dump) &&
				leaderToggle.Load() == neutral
		},
	)
	result, _ := leader.Agent().LastSnapshotTake()
	require.Positive(t, result.LogPosition, sc.dump())
	require.Equal(t, consensus.ModuleStateActive, leader.ModuleState(), sc.dump())
}
