package consensus

import "github.com/warrenguy/heron/cluster/codecs"

// Role is the cluster node role written to CLUSTER_NODE_ROLE_TYPE_ID (201).
//
// Alias of [codecs.Role]. The codecs package owns the role codes because the
// JoinLog body carries the same field. Use [codecs.RoleFromCode] to decode a
// counter value.
type Role = codecs.Role

const (
	RoleFollower  = codecs.RoleFollower
	RoleCandidate = codecs.RoleCandidate
	RoleLeader    = codecs.RoleLeader
)
