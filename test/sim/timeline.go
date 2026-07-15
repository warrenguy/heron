package sim

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/warrenguy/heron/cluster/election"
)

// TimelineEntry records one simulation event for replay and failure dumps.
// Kind selects which event-specific fields are meaningful; detail strings are
// formatted only when a timeline is dumped or canonicalized.
type TimelineEntry struct {
	AtNs   int64
	Seq    uint64
	Kind   string
	NodeID int32

	From       int32
	To         int32
	TemplateID uint16
	State      election.ElectionState
	Work       int
	HandedOff  bool
}

func (e TimelineEntry) detail() string {
	switch e.Kind {
	case "tick":
		return fmt.Sprintf("state=%s work=%d", e.State, e.Work)
	case "deliver":
		return fmt.Sprintf("from=%d to=%d template=%d handed_off=%v",
			e.From, e.To, e.TemplateID, e.HandedOff)
	case "publish", "drop":
		return fmt.Sprintf("to=%d template=%d", e.To, e.TemplateID)
	case "state":
		return fmt.Sprintf("election_state=%s", e.State)
	default:
		return ""
	}
}

// NodeSnapshot captures per-node observable state.
type NodeSnapshot struct {
	NodeID             int32
	ElectionState      election.ElectionState
	LeadershipTermID   int64
	LogPosition        int64
	CandidateTermID    int64
	PublishedCount     int
	InboundCount       int
	PrepareCalls       int
	PrepareLogPosition int64
	PrepareNowNs       int64
}

// TimelineCanonicalBytes returns stable newline-delimited timeline bytes for replay tests.
func TimelineCanonicalBytes(entries []TimelineEntry) []byte {
	var b bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&b, "%d,%d,%s,%d,%s\n", e.AtNs, e.Seq, e.Kind, e.NodeID, e.detail())
	}
	return b.Bytes()
}

func formatTimelineDump(entries []TimelineEntry) string {
	if len(entries) == 0 {
		return "(no timeline entries)"
	}
	var b strings.Builder
	for _, e := range entries {
		fmt.Fprintf(
			&b,
			"[seq=%d at=%d] %s node=%d %s\n",
			e.Seq, e.AtNs, e.Kind, e.NodeID, e.detail(),
		)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatNodeDump(node *Node) string {
	snap := node.Snapshot()
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"state: %s leadership_term=%d log_pos=%d candidate_term=%d\npublished: %d inbound: %d\n",
		snap.ElectionState,
		snap.LeadershipTermID,
		snap.LogPosition,
		snap.CandidateTermID,
		snap.PublishedCount,
		snap.InboundCount,
	)
	if pub := node.Publisher(); pub != nil {
		for i, msg := range pub.Sent {
			fmt.Fprintf(&b, "  %d: template=%d dst=%d\n", i, msg.TemplateID, msg.Dst.MemberID)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func dumpElectionSim(tb testing.TB, s *ElectionSim) {
	tb.Helper()
	tb.Log("=== ElectionSim failure dump ===")
	tb.Logf("seed: %d", s.cfg.Seed)
	tb.Logf("steps: %d", s.steps)
	tb.Logf("master_now_ns: %d", s.scheduler.Now())
	tb.Log("-- timeline --")
	tb.Log(formatTimelineDump(s.timeline))
	for _, node := range s.nodes {
		tb.Logf("-- node %d --", node.id)
		tb.Log(formatNodeDump(node))
	}
}
