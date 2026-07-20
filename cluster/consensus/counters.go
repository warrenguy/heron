package consensus

const (
	// ClusterNodeRoleCounterTypeID is the aeron counter type ID CLUSTER_NODE_ROLE_TYPE_ID.
	// Counter value is [Role.Code].
	ClusterNodeRoleCounterTypeID int32 = 201

	// ClusterCommitPositionCounterTypeID is the aeron counter type ID CLUSTER_COMMIT_POSITION_TYPE_ID.
	ClusterCommitPositionCounterTypeID int32 = 203

	// ClusterElectionStateCounterTypeID is the aeron counter type ID CLUSTER_ELECTION_STATE_TYPE_ID.
	// Counter value is the aeron cluster ElectionState code (0-17).
	ClusterElectionStateCounterTypeID int32 = 207
)
