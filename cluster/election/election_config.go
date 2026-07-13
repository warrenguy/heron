package election

import "errors"

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

// DefaultElectionConfig returns the consensus-module election defaults: the five
// timeout intervals plus the NULL_VALUE leader sentinels.
//
// AppointedLeaderID and GracefulClosedLeaderID default to NullValue so no real
// member id (0 included) is mistaken for an appointed or gracefully-closed leader.
func DefaultElectionConfig() ElectionConfig {
	return ElectionConfig{
		AppointedLeaderID:         int32(NullValue),
		GracefulClosedLeaderID:    int32(NullValue),
		ElectionTimeoutNs:         1_000_000_000,
		ElectionStatusIntervalNs:  100_000_000,
		StartupCanvassTimeoutNs:   60_000_000_000,
		LeaderHeartbeatTimeoutNs:  10_000_000_000,
		LeaderHeartbeatIntervalNs: 200_000_000,
	}
}

// ElectionTestConfig returns the same timeout defaults as DefaultElectionConfig.
// The Aeron ElectionTest fixture runs with unmodified ConsensusModule.Context
// defaults, so parity tests seed from these values and override positions per case.
func ElectionTestConfig() ElectionConfig {
	return DefaultElectionConfig()
}

// ValidateElectionConfig rejects non-positive interval and timeout values that
// would collapse publish cadence and election deadlines to fire immediately.
func ValidateElectionConfig(cfg ElectionConfig) error {
	if cfg.ElectionTimeoutNs <= 0 {
		return errors.New("election: ElectionTimeoutNs must be positive")
	}
	if cfg.ElectionStatusIntervalNs <= 0 {
		return errors.New("election: ElectionStatusIntervalNs must be positive")
	}
	if cfg.LeaderHeartbeatTimeoutNs <= 0 {
		return errors.New("election: LeaderHeartbeatTimeoutNs must be positive")
	}
	if cfg.StartupCanvassTimeoutNs <= 0 {
		return errors.New("election: StartupCanvassTimeoutNs must be positive")
	}
	if cfg.LeaderHeartbeatIntervalNs <= 0 {
		return errors.New("election: LeaderHeartbeatIntervalNs must be positive")
	}
	return nil
}
