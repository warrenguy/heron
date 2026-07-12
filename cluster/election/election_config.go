package election

type ElectionConfig struct {
	IsNodeStartup          bool
	GracefulClosedLeaderID int32
	LeadershipTermID       int64
	TermBaseLogPosition    int64
	LogPosition            int64
	AppendPosition         int64

	AppointedLeaderID int32
	MemberID          int32
	AppVersion        int32

	ElectionTimeoutNs         int64
	ElectionStatusIntervalNs  int64
	LeaderHeartbeatTimeoutNs  int64
	LeaderHeartbeatIntervalNs int64
	StartupCanvassTimeoutNs   int64
}
