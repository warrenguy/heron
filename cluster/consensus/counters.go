package consensus

const (
	// ClusterConsensusModuleStateCounterTypeID is the aeron counter type ID
	// CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID.
	ClusterConsensusModuleStateCounterTypeID int32 = 200

	// ClusterNodeRoleCounterTypeID is the aeron counter type ID CLUSTER_NODE_ROLE_TYPE_ID.
	// Counter value is [Role.Code].
	ClusterNodeRoleCounterTypeID int32 = 201

	// ClusterCommitPositionCounterTypeID is the aeron counter type ID CLUSTER_COMMIT_POSITION_TYPE_ID.
	ClusterCommitPositionCounterTypeID int32 = 203

	// ClusterSnapshotCounterTypeID is the aeron counter type ID CLUSTER_SNAPSHOT_COUNTER_TYPE_ID.
	ClusterSnapshotCounterTypeID int32 = 205

	// ClusterElectionStateCounterTypeID is the aeron counter type ID CLUSTER_ELECTION_STATE_TYPE_ID.
	// Counter value is the aeron cluster ElectionState code (0-17).
	ClusterElectionStateCounterTypeID int32 = 207

	// ClusterLeadershipTermIDCounterTypeID is the aeron counter type ID
	// CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID.
	ClusterLeadershipTermIDCounterTypeID int32 = 239
)
