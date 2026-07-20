package consensus

// ElectionRunner is the agent-to-election boundary.
// Concrete type: *election.Election (outside this package).
//
// Method semantics:
//   - DoWork: one election duty-cycle tick
//   - HandleConsensusMessage: decode consensus wire and dispatch On* handlers.
//     Returns true when decode succeeds. ElectionStateInit gates handler bodies
//     only (return may still be true when handlers are skipped).
//
// Held by [Agent]; never the reverse import.
type ElectionRunner interface {
	DoWork(nowNs int64) int
	HandleConsensusMessage(buf []byte, offset int) bool
}
