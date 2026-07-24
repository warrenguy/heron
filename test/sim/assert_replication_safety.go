//go:build testhooks

package sim

import (
	"fmt"
	"maps"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

// advanceWireScan folds CommitPosition rows appended to publisher.Sent since
// the last call into n's per-term cursor, then returns it. Continuous safety
// reads this every step; the cursor keeps that O(new rows) instead of a full
// Sent rescan. Nil when n has no publisher.
func advanceWireScan(n *Node) *wireCommitScan {
	if n == nil || n.publisher == nil {
		return nil
	}
	scan := n.wireScan
	if scan == nil || scan.gen != n.publishGen {
		scan = &wireCommitScan{
			gen:            n.publishGen,
			highByTerm:     make(map[int64]int64),
			leaderByTerm:   make(map[int64]int32),
			conflictByTerm: make(map[int64][2]int32),
		}
		n.wireScan = scan
	}
	sent := n.publisher.Sent
	for _, msg := range sent[scan.nextIndex:] {
		if msg.TemplateID != clusterwire.CommitPositionSbeTemplateID {
			continue
		}
		fields, ok := msg.Fields.(codecs.CommitPositionFields)
		if !ok {
			continue
		}
		term := fields.LeadershipTermID
		if fields.LogPosition > scan.highByTerm[term] {
			scan.highByTerm[term] = fields.LogPosition
		}
		first, seen := scan.leaderByTerm[term]
		switch {
		case !seen:
			scan.leaderByTerm[term] = fields.LeaderMemberID
		case first != fields.LeaderMemberID:
			if _, dual := scan.conflictByTerm[term]; !dual {
				scan.conflictByTerm[term] = [2]int32{first, fields.LeaderMemberID}
			}
		}
	}
	scan.nextIndex = len(sent)
	return scan
}

// WireCommitHighWater returns the max CommitPosition LogPosition published by n
// for leadership term. Zero when none match.
//
// N=1 HostAdapter skips thisMember, so Sent often stays empty.
func WireCommitHighWater(n *Node, term int64) int64 {
	scan := advanceWireScan(n)
	if scan == nil {
		return 0
	}
	return scan.highByTerm[term]
}

// ModelQuorumFloor is the commit floor oracle on the leader node: active-member
// QuorumPosition bounded by LeaderAppend, via the replication HostAdapter.
//
// MUST use this node's member pointers (not peer copies). Zero when the node
// has no live election or no replication host.
func ModelQuorumFloor(leaderNode *Node, nowNs int64) int64 {
	if leaderNode == nil || leaderNode.election == nil {
		return 0
	}
	h := leaderNode.replicationHost()
	if h == nil {
		return 0
	}
	return h.QuorumPositionBoundedByLeaderLog(leaderNode.election.AppendPosition(), nowNs)
}

// commitSignal selects the continuous commit observation for a live node.
//
// N=1: Agent under LEADER_READY (wire Sent empty expected).
// N>=2: max(wire high-water, Agent under LEADER_READY).
func commitSignal(n *Node, clusterSize int) (term, signal int64, ok bool) {
	if n == nil || n.election == nil {
		return 0, 0, false
	}
	term = n.election.LeadershipTermID()

	var agent int64
	if n.election.State() == election.ElectionStateLeaderReady {
		agent = AgentCommitPosition(n)
	}
	if clusterSize == 1 {
		return term, agent, true
	}
	return term, max(WireCommitHighWater(n, term), agent), true
}

// CommitSignalHighWater returns the continuous commit signal for n under s's
// cluster size (scenario-side view of the same observation continuous safety
// checks). Zero when n has no live election.
func CommitSignalHighWater(s *ElectionSim, n *Node) int64 {
	if s == nil {
		return 0
	}
	_, signal, _ := commitSignal(n, len(s.nodes))
	return signal
}

// CheckCommitMonotonic fails when any live node's commit signal regressed for
// its (memberID, term) vs the last recorded high-water.
//
// Does not update lastCommitSignals; continuous path records after monotonic
// and floor checks both pass.
func CheckCommitMonotonic(s *ElectionSim) error {
	if s == nil {
		return nil
	}
	clusterSize := len(s.nodes)
	for _, n := range s.nodes {
		term, signal, ok := commitSignal(n, clusterSize)
		if !ok {
			continue
		}
		key := commitSignalKey{memberID: n.id, term: term}
		last := s.lastCommitSignals[key]
		if signal < last {
			return fmt.Errorf(
				"sim: commit regression member=%d term=%d last=%d now=%d",
				n.id, term, last, signal,
			)
		}
	}
	return nil
}

// CheckCommitBoundedByModelQuorum fails when a live self-leader advances its
// commit signal above ModelQuorumFloor. Unchanged signals skip the floor
// re-check (on-advance only).
//
// Does not update lastCommitSignals; continuous path records after monotonic
// and floor checks both pass.
func CheckCommitBoundedByModelQuorum(s *ElectionSim) error {
	if s == nil {
		return nil
	}
	clusterSize := len(s.nodes)
	nowNs := s.Now()
	for _, n := range s.nodes {
		if _, ok := liveSelfLeaderTerm(n); !ok {
			continue
		}
		term, signal, ok := commitSignal(n, clusterSize)
		if !ok {
			continue
		}
		key := commitSignalKey{memberID: n.id, term: term}
		last := s.lastCommitSignals[key]
		if signal <= last {
			continue
		}
		floor := ModelQuorumFloor(n, nowNs)
		if signal > floor {
			return fmt.Errorf(
				"sim: commit advance above floor member=%d term=%d signal=%d floor=%d",
				n.id, term, signal, floor,
			)
		}
	}
	return nil
}

// CheckSingleCommitAuthority fails when more than one live self-leader shares a
// term, or when live self-leaders publish CommitPosition under more than one
// distinct LeaderMemberID for a term.
//
// Dual self-leader detection here is independent of SkipSplitBrainCheck: that
// flag only skips the election after-step scan. Under UseReplicationHost with
// continuous commit safety on, dual self-leaders still fail via this check.
// Set SkipReplicationSafety (or both flags) to force dual leaders without
// stopping the step loop.
//
// N=1: no Sent publishers; dual-self-leader branch covers multi-leader only.
func CheckSingleCommitAuthority(s *ElectionSim) error {
	if s == nil {
		return nil
	}

	// Dual live self-leaders same term (also election live split-brain).
	if err := CheckNoSplitBrainLive(s); err != nil {
		return err
	}
	return checkDualCommitAuthority(s)
}

// checkDualCommitAuthority fails when live self-leaders publish CommitPosition
// under more than one distinct LeaderMemberID for a term. Reads the per-node
// wire cursors; the cross-node merge allocates only when a second live
// self-leader has published.
func checkDualCommitAuthority(s *ElectionSim) error {
	var (
		firstScan *wireCommitScan
		merged    map[int64]int32
	)
	for _, n := range s.nodes {
		if _, ok := liveSelfLeaderTerm(n); !ok {
			continue
		}
		scan := advanceWireScan(n)
		if scan == nil {
			continue
		}
		if err := dualAuthorityErr(scan); err != nil {
			return err
		}
		if len(scan.leaderByTerm) == 0 {
			continue
		}
		if firstScan == nil {
			firstScan = scan
			continue
		}
		if merged == nil {
			merged = make(map[int64]int32, len(firstScan.leaderByTerm))
			maps.Copy(merged, firstScan.leaderByTerm)
		}
		for term, leaderID := range scan.leaderByTerm {
			if prev, seen := merged[term]; seen && prev != leaderID {
				return fmt.Errorf(
					"sim: dual commit authority term=%d leaders=%d and %d",
					term, prev, leaderID,
				)
			}
			merged[term] = leaderID
		}
	}
	return nil
}

// dualAuthorityErr reports the lowest conflicted term on one node's cursor.
func dualAuthorityErr(scan *wireCommitScan) error {
	if len(scan.conflictByTerm) == 0 {
		return nil
	}
	lowest, first := int64(0), true
	for term := range scan.conflictByTerm {
		if first || term < lowest {
			lowest, first = term, false
		}
	}
	pair := scan.conflictByTerm[lowest]
	return fmt.Errorf(
		"sim: dual commit authority term=%d leaders=%d and %d",
		lowest, pair[0], pair[1],
	)
}

// checkReplicationCommitSafety runs commit monotonic then floor-on-advance,
// records high-water, then CheckSingleCommitAuthority.
//
// CheckSingleCommitAuthority rechecks live split-brain even when
// checkContinuousSafety already ran it this step; the repeat is cheap and
// keeps dual self-leaders failing with SkipSplitBrainCheck set.
func checkReplicationCommitSafety(s *ElectionSim) error {
	if err := CheckCommitMonotonic(s); err != nil {
		return err
	}
	if err := CheckCommitBoundedByModelQuorum(s); err != nil {
		return err
	}
	recordCommitSignals(s)
	return CheckSingleCommitAuthority(s)
}

// recordCommitSignals advances lastCommitSignals to max(last, signal) per live node.
func recordCommitSignals(s *ElectionSim) {
	if s.lastCommitSignals == nil {
		s.lastCommitSignals = make(map[commitSignalKey]int64)
	}
	clusterSize := len(s.nodes)
	for _, n := range s.nodes {
		term, signal, ok := commitSignal(n, clusterSize)
		if !ok {
			continue
		}
		key := commitSignalKey{memberID: n.id, term: term}
		if signal > s.lastCommitSignals[key] {
			s.lastCommitSignals[key] = signal
		}
	}
}

// clearCommitSignalsForMember drops continuous commit high-water for memberID
// across all terms. CrashNode calls this so restart does not compare a fresh
// empty wire/Agent against a pre-crash watermark.
func (s *ElectionSim) clearCommitSignalsForMember(memberID int32) {
	if s == nil || s.lastCommitSignals == nil {
		return
	}
	for k := range s.lastCommitSignals {
		if k.memberID == memberID {
			delete(s.lastCommitSignals, k)
		}
	}
}
