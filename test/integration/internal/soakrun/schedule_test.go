package soakrun

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A draw-order change breaks replay of a stored seed, so goldenSteps is exact.
const goldenSeed = 1

func TestSchedule_fixedSeed(t *testing.T) {
	t.Parallel()

	got := takeSteps(goldenSeed, len(goldenSteps))
	require.Equal(t, goldenSteps, got)
}

func takeSteps(seed uint64, count int) []Step {
	schedule := NewSchedule(seed)
	steps := make([]Step, count)

	for i := range steps {
		steps[i] = schedule.Next()
	}
	return steps
}

func sec(n int) time.Duration {
	return time.Duration(n) * time.Second
}

var goldenSteps = []Step{
	{Index: 0, Kind: ArchiveKill, VictimRole: Follower, Settle: sec(9)},
	{Index: 1, Kind: DriverKill, VictimRole: Follower, Settle: sec(8)},
	{
		Index: 2, Kind: Partition, VictimRole: Leader,
		Hold: sec(5), Settle: sec(14),
	},
	{Index: 3, Kind: ArchiveKill, VictimRole: Leader, Settle: sec(6)},
	{
		Index: 4, Kind: PacketLoss, VictimRole: Follower,
		Hold: sec(19), LossPercent: 1, LossScope: OneMember, Settle: sec(11),
	},
	{
		Index: 5, Kind: MemberKill, VictimRole: Follower,
		FollowerPick: 1, Settle: sec(6),
	},
	{
		Index: 6, Kind: PacketLoss, VictimRole: Leader,
		Hold: sec(44), LossPercent: 20, LossScope: OneMember, Settle: sec(6),
	},
	{
		Index: 7, Kind: DriverKill, VictimRole: Leader,
		FollowerPick: 1, Settle: sec(5),
	},
	{Index: 8, Kind: MemberKill, VictimRole: Leader, Settle: sec(5)},
	{
		Index: 9, Kind: Partition, VictimRole: Follower,
		Hold: sec(8), Settle: sec(11),
	},
	{
		Index: 10, Kind: Partition, VictimRole: Leader,
		FollowerPick: 1, Hold: sec(30), Settle: sec(9),
	},
	{
		Index: 11, Kind: PacketLoss, VictimRole: Leader,
		Hold: sec(52), LossPercent: 5, LossScope: OneMember, Settle: sec(14),
	},
	{Index: 12, Kind: MemberKill, VictimRole: Leader, Settle: sec(6)},
	{
		Index: 13, Kind: ArchiveKill, VictimRole: Leader,
		FollowerPick: 1, Settle: sec(12),
	},
	{
		Index: 14, Kind: DriverKill, VictimRole: Follower,
		FollowerPick: 1, Settle: sec(10),
	},
	{
		Index: 15, Kind: Partition, VictimRole: Follower,
		Hold: sec(10), Settle: sec(5),
	},
	{Index: 16, Kind: DriverKill, VictimRole: Leader, Settle: sec(8)},
	{Index: 17, Kind: MemberKill, VictimRole: Follower, Settle: sec(9)},
	{
		Index: 18, Kind: ArchiveKill, VictimRole: Follower,
		FollowerPick: 1, Settle: sec(11),
	},
	{
		Index: 19, Kind: PacketLoss, VictimRole: Follower,
		Hold: sec(25), LossPercent: 5, LossScope: AllMembers, Settle: sec(11),
	},
}
