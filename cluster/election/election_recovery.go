package election

import "github.com/warrenguy/heron/cluster/recordinglog"

// ApplyRecoveryPlan seeds cfg term and positions from a recordinglog.RecoveryPlan.
//
// Caller still owns Open, PlanRecovery, Host.SetLogRecordingID, Close, and
// wiring deps. An empty plan seeds the cold-start values: a null leadership
// term and zero positions.
//
// LogPosition is the plan replay start, SeedLogPosition, on every plan.
func ApplyRecoveryPlan(cfg *ElectionConfig, plan recordinglog.RecoveryPlan) {
	cfg.LogPosition = plan.SeedLogPosition()
	if plan.IsEmpty() {
		cfg.LeadershipTermID = NullValue
		cfg.TermBaseLogPosition = 0
		cfg.AppendPosition = 0
		return
	}
	cfg.LeadershipTermID = plan.LastLeadershipTermID
	cfg.TermBaseLogPosition = plan.LastTermBaseLogPosition
	cfg.AppendPosition = plan.AppendedOrCommittedPosition()
}
