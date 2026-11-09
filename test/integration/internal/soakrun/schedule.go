package soakrun

import (
	"fmt"
	"math/rand/v2"
	"time"
)

type StepKind uint8

const (
	MemberKill StepKind = iota
	ArchiveKill
	DriverKill
	PacketLoss

	// Partition cuts one member off the bridge.
	Partition
)

var stepKindNames = [...]string{
	MemberKill:  "member-kill",
	ArchiveKill: "archive-kill",
	DriverKill:  "driver-kill",
	PacketLoss:  "packet-loss",
	Partition:   "partition",
}

func (k StepKind) String() string {
	if int(k) >= len(stepKindNames) {
		return fmt.Sprintf("StepKind(%d)", uint8(k))
	}
	return stepKindNames[k]
}

// VictimRole is a target that the caller resolves when the step runs.
type VictimRole uint8

const (
	Leader VictimRole = iota
	Follower
)

var victimRoleNames = [...]string{
	Leader:   "leader",
	Follower: "follower",
}

func (r VictimRole) String() string {
	if int(r) >= len(victimRoleNames) {
		return fmt.Sprintf("VictimRole(%d)", uint8(r))
	}
	return victimRoleNames[r]
}

type LossScope uint8

const (
	OneMember LossScope = iota + 1
	AllMembers
)

var lossScopeNames = [...]string{
	OneMember:  "one-member",
	AllMembers: "all-members",
}

func (s LossScope) String() string {
	if int(s) >= len(lossScopeNames) || lossScopeNames[s] == "" {
		return fmt.Sprintf("LossScope(%d)", uint8(s))
	}
	return lossScopeNames[s]
}

// Step is one drawn fault.
//
// Hold applies to PacketLoss and Partition. LossPercent and LossScope apply to
// PacketLoss. FollowerPick selects among the followers known at run time.
type Step struct {
	Index        int
	Kind         StepKind
	VictimRole   VictimRole
	FollowerPick int
	Hold         time.Duration
	LossPercent  int
	LossScope    LossScope
	Settle       time.Duration
}

// Schedule draws steps from one PCG seed.
//
// Schedules with the same seed share a step prefix.
type Schedule struct {
	rng     *rand.Rand
	pending []Step
	index   int
}

func NewSchedule(seed uint64) *Schedule {
	return &Schedule{rng: rand.New(rand.NewPCG(seed, seed))}
}

// Next deals each round's ten pairs of kind and role in shuffled order. The
// first round forces OneMember on both loss steps.
func (s *Schedule) Next() Step {
	if len(s.pending) == 0 {
		s.pending = s.dealRound()
	}

	step := s.pending[0]
	s.pending = s.pending[1:]
	step.Index = s.index
	s.index++
	return step
}

type kindRole struct {
	kind StepKind
	role VictimRole
}

func (s *Schedule) dealRound() []Step {
	pairs := []kindRole{
		{MemberKill, Leader},
		{MemberKill, Follower},
		{ArchiveKill, Leader},
		{ArchiveKill, Follower},
		{DriverKill, Leader},
		{DriverKill, Follower},
		{PacketLoss, Leader},
		{PacketLoss, Follower},
		{Partition, Leader},
		{Partition, Follower},
	}
	s.rng.Shuffle(len(pairs), func(i, j int) {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	})

	firstRound := s.index == 0
	steps := make([]Step, len(pairs))

	for i, pair := range pairs {
		// Draw order is part of the seed: FollowerPick, Hold, loss fields, Settle.
		step := Step{
			Kind:         pair.kind,
			VictimRole:   pair.role,
			FollowerPick: s.rng.IntN(2),
		}

		switch pair.kind {
		case PacketLoss:
			step.Hold = drawSeconds(s.rng, 5, 60)
			step.LossPercent = lossPercents[s.rng.IntN(len(lossPercents))]
			step.LossScope = OneMember
			if !firstRound && s.rng.IntN(2) == 1 {
				step.LossScope = AllMembers
			}

		case Partition:
			step.Hold = drawSeconds(s.rng, 5, 30)

		case MemberKill, ArchiveKill, DriverKill:
			// A kill step does not draw a hold.
		}

		step.Settle = drawSeconds(s.rng, 5, 15)
		steps[i] = step
	}
	return steps
}

var lossPercents = [...]int{1, 5, 20}

func drawSeconds(rng *rand.Rand, minSeconds, maxSeconds int) time.Duration {
	span := maxSeconds - minSeconds + 1
	return time.Duration(minSeconds+rng.IntN(span)) * time.Second
}
