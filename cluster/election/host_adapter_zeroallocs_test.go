//go:build testhooks

package election_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/aeron/logbuffer"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
	"luas.fast/cluster/consensus"
	"luas.fast/cluster/election"
	"luas.fast/internal/testutil"
)

const (
	hostDutyCycleWarmupCycles = 10
	hostDutyCycleRuns         = 250

	// hostDutyCyclePair puts one slow tick in each run. AllocsPerRun floors its
	// average, so a single cycle per run reads a slow tick allocation as 0.
	hostDutyCyclePair = 2

	// hostDutyCycleCount counts every cycle, including the pair from the
	// AllocsPerRun warm-up call.
	hostDutyCycleCount int64 = hostDutyCycleWarmupCycles + (1+hostDutyCycleRuns)*hostDutyCyclePair

	// hostDutyCycleClockStepNs is one timer wheel tick. A slow tick runs on every
	// second cycle at this step.
	hostDutyCycleClockStepNs int64 = 1 << 23

	hostDutyCyclePositionStep int64 = 64

	hostDutyCycleTermID int64 = 3
)

// measureHostDutyCycle requires zero allocations and a true result from every
// measured cycle. A compare does not allocate, so a cycle can check its outcome
// inside the measurement.
func measureHostDutyCycle(t *testing.T, cycle func() bool) {
	t.Helper()
	for range hostDutyCycleWarmupCycles {
		cycle()
	}
	misses := 0
	testutil.AssertZeroAllocs(t, "duty cycle", hostDutyCycleRuns, func() {
		for range hostDutyCyclePair {
			if !cycle() {
				misses++
			}
		}
	})
	require.Zero(t, misses)
}

func publicationAdvanced(publication *aeron.ExclusivePublication, last *int64) bool {
	position := publication.Position()
	advanced := position > *last
	*last = position
	return advanced
}

// rewoundConsensusFrame is one committed ingress frame. A cycle encodes the
// payload in place, then rewinds the subscriber position, so the next poll
// delivers the frame again.
type rewoundConsensusFrame struct {
	image   *aeron.Image
	payload []byte
}

func bindRewoundConsensusFrame(
	t *testing.T,
	agent *consensus.Agent,
	blockLength uint16,
) *rewoundConsensusFrame {
	t.Helper()

	// hookTermID matches the hook image partition-0 initial term id.
	const hookTermID int32 = 7
	payloadLength, ok := codecs.ClusterWireMessageLen(blockLength)
	require.True(t, ok)
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	term := aeron.ImageTermBufferForHooks(image)
	aeron.WriteCommittedDataFrameForHooks(
		term,
		0,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		0,
		logbuffer.Unfragmented,
		make([]byte, payloadLength),
	)
	agent.SetIngress(consensus.NewSubscriptionConsensusIngress(sub))
	payloadEnd := logbuffer.OffsetFramePayload + payloadLength
	return &rewoundConsensusFrame{
		image:   image,
		payload: term.Bytes()[logbuffer.OffsetFramePayload:payloadEnd],
	}
}

func (f *rewoundConsensusFrame) rewind() {
	aeron.ImageSetSubscriberPositionForHooks(f.image, 0)
}

func newHostDutyCycleAgent(
	t *testing.T,
	thisMember int,
	leaderMemberID int32,
	publisher *election.AeronPublisher,
) (
	*consensus.Agent,
	[]*election.Member,
	*FakeClock,
) {
	t.Helper()
	members := hostAdapterMembers(3)
	_, agent, _, clock := newTestHostAdapter(
		t,
		members,
		thisMember,
		func(config *election.HostAdapterConfig) {
			config.Publisher = publisher
			config.LeaderMemberID = leaderMemberID
		},
	)
	agent.SetLeadershipTermID(hostDutyCycleTermID)

	// Without a factory, a cycle that leaves steady-state still reads a nil
	// election.
	agent.SetElectionFactory(func(consensus.ElectionSeed) consensus.ElectionRunner {
		require.Fail(t, "election entered")
		return nil
	})
	return agent, members, clock
}

func requireHostDutyCycleSettled(t *testing.T, agent *consensus.Agent) {
	t.Helper()
	require.Nil(t, agent.Election())
	require.NoError(t, agent.TerminationError())
	require.NoError(t, agent.AppendRankFailure())
	require.NoError(t, agent.ServiceAckFailure())
	require.NoError(t, agent.TimerFailure())
}

func TestHostAdapter_DoWork_ZeroAllocs(t *testing.T) {
	t.Run("leader", testHostAdapterLeaderZeroAllocs)
	t.Run("follower", testHostAdapterFollowerZeroAllocs)
}

func testHostAdapterLeaderZeroAllocs(t *testing.T) {
	appendPosition := new(atomic.Int64)
	logPublisher := consensus.NewLogPublisher()
	logPublisher.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
	publisher, publications := boundAeronPublisher(t, 1, 2)
	agent, members, clock := newHostDutyCycleAgent(t, 0, election.NullMemberID, publisher)
	agent.RoleSet(consensus.RoleLeader)
	agent.SetAppendSource(&consensus.AtomicAppendPosition{V: appendPosition})
	agent.SetLogPublisher(logPublisher)
	firstFollower, secondFollower := publications[1], publications[2]
	frame := bindRewoundConsensusFrame(t, agent, clusterwire.AppendPositionSbeBlockLength)

	var firstPosition, secondPosition int64
	measureHostDutyCycle(t, func() bool {
		clock.nowNs += hostDutyCycleClockStepNs
		position := appendPosition.Add(hostDutyCyclePositionStep)
		_, encoded := codecs.EncodeAppendPosition(
			frame.payload,
			0,
			clusterwire.AppendPositionSbeSchemaVersion,
			codecs.AppendPositionFields{
				LeadershipTermID: hostDutyCycleTermID,
				LogPosition:      position,
				FollowerMemberID: 1,
			},
		)
		frame.rewind()

		agent.DoWork(clock.nowNs)

		firstAdvanced := publicationAdvanced(firstFollower, &firstPosition)
		secondAdvanced := publicationAdvanced(secondFollower, &secondPosition)
		return encoded && agent.CommitPosition() == position && firstAdvanced && secondAdvanced
	})

	position := appendPosition.Load()
	require.Equal(t, hostDutyCycleCount*hostDutyCyclePositionStep, position)
	require.Equal(t, position, members[1].LogPosition)
	requireHostDutyCycleSettled(t, agent)
}

func testHostAdapterFollowerZeroAllocs(t *testing.T) {
	const leaderMemberID int32 = 0
	appendPosition := new(atomic.Int64)
	publisher, publications := boundAeronPublisher(t, leaderMemberID)
	agent, _, clock := newHostDutyCycleAgent(t, 1, leaderMemberID, publisher)
	agent.SetAppendSource(&consensus.AtomicAppendPosition{V: appendPosition})
	leader := publications[leaderMemberID]
	frame := bindRewoundConsensusFrame(t, agent, clusterwire.CommitPositionSbeBlockLength)

	var leaderPosition int64
	measureHostDutyCycle(t, func() bool {
		clock.nowNs += hostDutyCycleClockStepNs
		position := appendPosition.Add(hostDutyCyclePositionStep)
		_, encoded := codecs.EncodeCommitPosition(
			frame.payload,
			0,
			clusterwire.CommitPositionSbeSchemaVersion,
			codecs.CommitPositionFields{
				LeadershipTermID: hostDutyCycleTermID,
				LogPosition:      position,
				LeaderMemberID:   leaderMemberID,
			},
		)
		frame.rewind()

		agent.DoWork(clock.nowNs)

		advanced := publicationAdvanced(leader, &leaderPosition)
		return encoded && agent.NotifiedCommitPosition() == position && advanced
	})

	require.Equal(t, hostDutyCycleCount*hostDutyCyclePositionStep, appendPosition.Load())
	requireHostDutyCycleSettled(t, agent)
}
