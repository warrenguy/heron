package consensus

// ElectionRunner is the agent-to-election boundary for driving the state machine.
// The consensus module agent holds a field of this type so it need not import
// cluster/election. Concrete implementations are provided outside this package
// (for example, by cluster/election).
//
// DoWork (doWork) is called each tick when the agent holds a non-nil
// ElectionRunner, mirroring ConsensusModuleAgent.
type ElectionRunner interface {
	DoWork(nowNs int64) int
}
