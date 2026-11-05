//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"
	"time"

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

	transport, session := connectServiceSession(t, sc, leader)

	// Phase: SUSPEND on the leader's toggle moves every member to SUSPENDED.
	// The leader holds a client message. The toggle stays SUSPEND.
	require.True(t, consensus.ControlToggleSuspend.Toggle(leaderToggle), sc.dump())
	requireServiceAwaitTick(
		t, sc,
		"suspend on every member",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return everyMemberInState(set, consensus.ModuleStateSuspended)
		}),
	)
	payload := []byte(serviceMessagePayload)
	offerServiceMessage(t, sc, session, payload)
	heldPosition := leader.LogPublisher().Position()
	pump := sessionPump(transport, session, func() bool { return false })
	holdEnd := time.Now().Add(suspendedHoldFloor)
	for cycles := 0; cycles < suspendedDutyCycles || time.Now().Before(holdEnd); cycles++ {
		set.Tick()
		pump()
	}
	require.Equal(t, heldPosition, leader.LogPublisher().Position(), sc.dump())
	for i, mem := range sc.members {
		require.Empty(t, mem.rec.messages, "member %d:\n%s", i, sc.dump())
	}
	require.True(t, everyMemberInState(set, consensus.ModuleStateSuspended), sc.dump())
	require.Equal(t, int64(consensus.ControlToggleSuspend), leaderToggle.Load(), sc.dump())

	// Phase: RESUME on the leader's toggle moves every member to ACTIVE.
	// Every service then reads the held message. The toggle reads NEUTRAL.
	require.True(t, consensus.ControlToggleResume.Toggle(leaderToggle), sc.dump())
	requireServiceAwaitTick(
		t, sc,
		"resume on every member",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return everyMemberInState(set, consensus.ModuleStateActive) &&
				leaderToggle.Load() == neutral
		}),
	)
	requireMessagesAfterOpen(
		t, sc, transport, session,
		"held message after resume",
		session.ClusterSessionID(),
		payload,
	)
	requireNoServiceFailures(t, sc, " after resume")
}

const (
	suspendedDutyCycles = 500
	// suspendedHoldFloor spans several slow ticks. It gives the client message
	// time to reach the leader's ingress.
	suspendedHoldFloor = 100 * time.Millisecond
)

// everyMemberInState reports whether every member's ModuleState and module
// state counter equal state.
func everyMemberInState(set *harness.ClusterMemberSet, state consensus.ModuleState) bool {
	for _, m := range set.Modules() {
		if m.ModuleState() != state || !readModuleStateCounter(m).hasState(state) {
			return false
		}
	}
	return true
}
