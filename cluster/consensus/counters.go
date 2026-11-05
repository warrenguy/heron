package consensus

const (
	// ClusterConsensusModuleStateCounterTypeID is the aeron counter type ID
	// CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID.
	ClusterConsensusModuleStateCounterTypeID int32 = 200

	// ClusterNodeRoleCounterTypeID is the aeron counter type ID CLUSTER_NODE_ROLE_TYPE_ID.
	// Counter value is [Role.Code].
	ClusterNodeRoleCounterTypeID int32 = 201

	// ClusterControlToggleCounterTypeID is the aeron counter type ID
	// CLUSTER_CONTROL_TOGGLE_TYPE_ID. Counter value is [ControlToggleState.Code].
	ClusterControlToggleCounterTypeID int32 = 202

	// ClusterCommitPositionCounterTypeID is the aeron counter type ID CLUSTER_COMMIT_POSITION_TYPE_ID.
	ClusterCommitPositionCounterTypeID int32 = 203

	// ClusterSnapshotCounterTypeID is the aeron counter type ID CLUSTER_SNAPSHOT_COUNTER_TYPE_ID.
	ClusterSnapshotCounterTypeID int32 = 205

	// ClusterElectionStateCounterTypeID is the aeron counter type ID CLUSTER_ELECTION_STATE_TYPE_ID.
	// Counter value is the aeron cluster ElectionState code (0-17).
	ClusterElectionStateCounterTypeID int32 = 207

	// ClusterConsensusModuleErrorCountCounterTypeID is the aeron counter type ID
	// CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID.
	ClusterConsensusModuleErrorCountCounterTypeID int32 = 212

	// NodeControlToggleCounterTypeID is the aeron counter type ID
	// NODE_CONTROL_TOGGLE_TYPE_ID. Counter value is
	// [NodeControlToggleState.Code].
	NodeControlToggleCounterTypeID int32 = 233

	// ClusterLeadershipTermIDCounterTypeID is the aeron counter type ID
	// CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID.
	ClusterLeadershipTermIDCounterTypeID int32 = 239
)
