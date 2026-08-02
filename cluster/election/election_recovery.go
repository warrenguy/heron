package election

import "github.com/warrenguy/heron/cluster/recordinglog"

// ApplyRecoveryPlan seeds cfg term and positions from a recordinglog.RecoveryPlan.
//
// Caller still owns Open, PlanRecovery, Host.SetLogRecordingID, Close, and
// wiring deps. An empty plan seeds the cold-start values: a null leadership
// term and zero positions.
func ApplyRecoveryPlan(cfg *ElectionConfig, plan recordinglog.RecoveryPlan) {
	if plan.IsEmpty() {
		cfg.LeadershipTermID = NullValue
		cfg.TermBaseLogPosition = 0
		cfg.LogPosition = 0
		cfg.AppendPosition = 0
		return
	}
	cfg.LeadershipTermID = plan.LastLeadershipTermID
	cfg.TermBaseLogPosition = plan.LastTermBaseLogPosition
	cfg.LogPosition = plan.CommittedLogPosition
	cfg.AppendPosition = plan.AppendedOrCommittedPosition()
}
