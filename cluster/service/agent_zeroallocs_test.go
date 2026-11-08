//go:build testhooks

package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

const (
	serviceDutyWarmupCycles = 10
	serviceDutyRuns         = 250

	// serviceDutyPair is the cycles per run. AllocsPerRun floors its average,
	// so one cycle per run reads an allocation on every second cycle as 0.
	serviceDutyPair = 2

	dutySessionID int64 = 7
)

// dutyService counts callbacks in int fields. Those counters do not allocate.
type dutyService struct {
	cluster   Cluster
	reply     []byte
	messages  int
	timers    int
	lastOffer int64
}

func (s *dutyService) Start(cluster Cluster) error {
	s.cluster = cluster
	return nil
}

func (s *dutyService) SessionOpen(ClientSession, int64) {}

func (s *dutyService) SessionClose(ClientSession, int64, clusterwire.CloseReason) {}

func (s *dutyService) SessionMessage(session ClientSession, timestamp int64, _ []byte) {
	s.messages++
	s.lastOffer = session.Offer(s.reply)
	s.cluster.ScheduleTimer(int64(s.messages), timestamp)
}

func (s *dutyService) TimerEvent(int64, int64) {
	s.timers++
}

func (s *dutyService) DoBackgroundWork(int64) int {
	return 0
}

// serviceDutyCycleCount returns the cycles a measurement runs. The 1 counts
// the call AllocsPerRun makes before it measures.
func serviceDutyCycleCount() int {
	return serviceDutyWarmupCycles + (1+serviceDutyRuns)*serviceDutyPair
}

// measureServiceDuty requires zero allocations and a true result from every
// measured cycle. A compare does not allocate, so a cycle can check its outcome
// inside the measurement.
func measureServiceDuty(t *testing.T, cycle func() bool) {
	t.Helper()

	for range serviceDutyWarmupCycles {
		cycle()
	}

	misses := 0
	testutil.AssertZeroAllocs(t, "duty cycle", serviceDutyRuns, func() {
		for range serviceDutyPair {
			if !cycle() {
				misses++
			}
		}
	})
	require.Zero(t, misses)
}

func TestAgent_DoWork_ZeroAllocs(t *testing.T) {
	t.Run("leader", func(t *testing.T) {
		testServiceLogZeroAllocs(t, codecs.RoleLeader)
	})
	t.Run("follower", func(t *testing.T) {
		testServiceLogZeroAllocs(t, codecs.RoleFollower)
	})
	t.Run("request ack", testRequestAckZeroAllocs)
}

func testServiceLogZeroAllocs(t *testing.T, role codecs.Role) {
	svc := &dutyService{reply: []byte{1}}
	adder := newFakeResponseAdder()
	proxy, pub := boundModuleProxy(t)
	controlSub, _ := controlIngressFixtures(t)
	log := bindSessionLog(t, AgentConfig{
		Service:              svc,
		ModuleProxy:          proxy,
		Control:              NewSubscriptionControlIngress(controlSub),
		ResponsePublications: adder,
	}, role)
	agent := log.agent
	committed := log.committed

	log.open(dutySessionID, 1)
	openEnd := *committed
	require.Equal(t, openEnd, agent.LogPosition())

	if role == codecs.RoleLeader {
		response := aeron.NewTestPublicationForHooksWithConnected(t, true)
		adder.next(1, responsePoll{publication: response, done: true})
		agent.DoWork(0)
		require.Zero(t, agent.PendingResponsePublicationAdds())
		require.Equal(t, 1, agent.ResponsePublicationCount())
	}

	// A log image never rewinds, so every cycle must fit one term.
	cycleCount := serviceDutyCycleCount()
	pairSpan := logbuffer.AlignFragmentChunkLength(codecs.SessionMessageHeaderEncodedLen()) +
		logbuffer.AlignFragmentChunkLength(codecs.TimerEventEncodedLen())
	logEnd := openEnd + int64(cycleCount/2*pairSpan)
	require.LessOrEqual(t, logEnd, int64(logbuffer.TermMinLength))

	frameEnds := make([]int64, cycleCount)

	for i := range frameEnds {
		var payload []byte

		if i%2 == 0 {
			payload = encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: dutySessionID,
				Timestamp:        int64(i + 1),
			}, nil)
		} else {
			payload = encodeTimerEvent(t, codecs.TimerEventFields{
				LeadershipTermID: 1,
				CorrelationID:    int64(i/2 + 1),
				Timestamp:        int64(i + 1),
			})
		}

		frameEnds[i] = log.write(t, payload)
	}

	require.Equal(t, logEnd, frameEnds[cycleCount-1])

	// write commits each frame. Each cycle commits only its own frame.
	*committed = openEnd

	scheduleSpan := int64(logbuffer.AlignFragmentChunkLength(codecs.ScheduleTimerEncodedLen()))
	baseline := pub.Position()
	cursor := 0
	cycle := func() bool {
		end := frameEnds[cursor]
		message := cursor%2 == 0
		messagesBefore := svc.messages
		timersBefore := svc.timers
		pubBefore := pub.Position()
		*committed = end
		agent.DoWork(0)
		cursor++

		if agent.LogPosition() != end {
			return false
		}

		if message {
			return svc.messages == messagesBefore+1 &&
				svc.timers == timersBefore &&
				pub.Position()-pubBefore == scheduleSpan &&
				dutyOfferOK(role, svc.lastOffer)
		}

		return svc.timers == timersBefore+1 &&
			svc.messages == messagesBefore &&
			pub.Position() == pubBefore
	}

	measureServiceDuty(t, cycle)

	// Frames alternate message and timer, and cycleCount is even.
	frames := cycleCount / 2
	require.Equal(t, frames, svc.messages)
	require.Equal(t, frames, svc.timers)
	require.Equal(t, baseline+int64(frames)*scheduleSpan, pub.Position())
	require.NoError(t, agent.ServiceFailure())
}

func dutyOfferOK(role codecs.Role, result int64) bool {
	if role == codecs.RoleLeader {
		return result > MockedOffer
	}
	return result == MockedOffer
}

// Each cycle rewinds the control image and polls the same RequestServiceAck.
// The log position stays 0, so the new entry is ready before the drain.
func testRequestAckZeroAllocs(t *testing.T) {
	controlSub, controlImage := controlIngressFixtures(t)
	proxy, _ := boundModuleProxy(t)
	agent, _, _ := bindImage(t, AgentConfig{
		Service:     &dutyService{},
		ModuleProxy: proxy,
		Control:     NewSubscriptionControlIngress(controlSub),
	}, 0)

	// Start runs before the frame exists. Measured cycles do not run Start again.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, int64(0), agent.NextAckID())

	writeUnfragmentedFrame(t, controlImage, 0, encodeRequestServiceAckControl(t, 0))

	var ackID int64
	cycle := func() bool {
		aeron.ImageSetSubscriberPositionForHooks(controlImage, 0)
		agent.DoWork(0)
		ackID++
		return agent.NextAckID() == ackID &&
			agent.PendingCoordinationAcks() == 0 &&
			agent.LogPosition() == 0
	}

	measureServiceDuty(t, cycle)

	require.Equal(t, int64(serviceDutyCycleCount()), agent.NextAckID())
	require.NoError(t, agent.ServiceFailure())
}
