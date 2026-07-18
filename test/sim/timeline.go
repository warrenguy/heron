package sim

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/warrenguy/heron/cluster/election"
)

// TimelineEntry records one simulation event for replay and failure dumps.
//
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
	Skipped    bool
	Action     string
	Detail     string
}

func (e TimelineEntry) detail() string {
	switch e.Kind {
	case "tick":
		return fmt.Sprintf("state=%s work=%d", e.State, e.Work)
	case "tick_skip":
		return "skipped=true"
	case "deliver", "deliver_skip":
		return fmt.Sprintf("from=%d to=%d template=%d handed_off=%v skipped=%v",
			e.From, e.To, e.TemplateID, e.HandedOff, e.Skipped)
	case "publish", "drop":
		return fmt.Sprintf("to=%d template=%d", e.To, e.TemplateID)
	case "state":
		return fmt.Sprintf("election_state=%s", e.State)
	case "nemesis":
		if e.Action != "" {
			return fmt.Sprintf("action=%s", e.Action)
		}
		return e.Detail
	default:
		return e.Detail
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

func formatDumpHeader(s *ElectionSim) string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== ElectionSim failure dump ===\n")
	fmt.Fprintf(&b, "derived_seed: %d\n", s.cfg.Seed)
	if attr := s.cfg.SeedAttribution; attr.Scenario != "" {
		fmt.Fprintf(&b, "base_seed: %d\n", attr.BaseSeed)
		fmt.Fprintf(&b, "scenario: %s\n", attr.Scenario)
		fmt.Fprintf(&b, "run_index: %d\n", attr.RunIndex)
	}
	fmt.Fprintf(&b, "steps: %d\n", s.steps)
	fmt.Fprintf(&b, "master_now_ns: %d\n", s.scheduler.Now())
	if s.safetyErr != nil {
		fmt.Fprintf(&b, "safety_err: %v\n", s.safetyErr)
	} else {
		fmt.Fprintf(&b, "safety_err: (none)\n")
	}
	for _, node := range s.nodes {
		fmt.Fprintf(&b, "live node=%d %v\n", node.id, s.IsLive(node.id))
	}
	return b.String()
}

func formatNodesDump(s *ElectionSim) string {
	var b strings.Builder
	for _, node := range s.nodes {
		fmt.Fprintf(&b, "-- node %d --\n", node.id)
		fmt.Fprintf(&b, "%s\n", formatNodeDump(node))
	}
	return strings.TrimRight(b.String(), "\n")
}

// dumpElectionSim logs state and, when HERON_SIM_SAVE=1, writes under
// FailureDumpsDir. Returns the dump directory path on successful save; "" when
// save is off or write fails (write errors are logged only).
func dumpElectionSim(tb testing.TB, s *ElectionSim) string {
	tb.Helper()
	header := formatDumpHeader(s)
	timeline := formatTimelineDump(s.timeline)
	nodes := formatNodesDump(s)

	tb.Log(strings.TrimRight(header, "\n"))
	tb.Log("-- timeline --")
	tb.Log(timeline)
	for line := range strings.SplitSeq(nodes, "\n") {
		tb.Log(line)
	}

	if os.Getenv("HERON_SIM_SAVE") != "1" {
		return ""
	}
	dir, err := saveDumpToDisk(tb, s, header, timeline, nodes)
	if err != nil {
		tb.Logf("sim: HERON_SIM_SAVE write failed: %v", err)
		return ""
	}
	return dir
}

// failureDumpRel is the dump root relative to the module root (go.mod).
const failureDumpRel = "test/sim/testdata/sim/failures"

// FailureDumpsDir returns the absolute path of the failure dump directory
// under the module root (walk cwd parents for go.mod).
//
// Path is always module-root/test/sim/testdata/sim/failures regardless of the
// package directory go test uses as cwd.
func FailureDumpsDir() (string, error) {
	root, err := findModuleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, filepath.FromSlash(failureDumpRel)), nil
}

func findModuleRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("sim: go.mod not found from working directory")
		}
		dir = parent
	}
}

func saveDumpToDisk(
	tb testing.TB,
	s *ElectionSim,
	header, timeline, nodes string,
) (string, error) {
	tb.Helper()
	root, err := FailureDumpsDir()
	if err != nil {
		return "", err
	}
	name := sanitizeDumpName(tb.Name())
	dir := filepath.Join(
		root,
		fmt.Sprintf("%s-seed-%d-%d", name, s.cfg.Seed, time.Now().Unix()),
	)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	writes := []struct {
		name string
		body string
	}{
		{"dump.txt", header},
		{"timeline.txt", timeline + "\n"},
		{"nodes.txt", nodes + "\n"},
	}
	for _, w := range writes {
		if err := os.WriteFile(filepath.Join(dir, w.name), []byte(w.body), 0o644); err != nil {
			return "", err
		}
	}
	tb.Logf("sim: saved failure dump to %s", dir)
	return dir, nil
}

func sanitizeDumpName(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_", "\\", "_")
	return r.Replace(name)
}
