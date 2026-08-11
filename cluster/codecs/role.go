package codecs

import "fmt"

// Role is the cluster node role. Codes match upstream Cluster.Role.
//
// Behaviour: the role reaches Go code on two surfaces. Counter type
// CLUSTER_NODE_ROLE_TYPE_ID (201) carries the role as a counter value. The
// JoinLog body carries the role as a fixed block field.
type Role int32

const (
	RoleFollower  Role = 0
	RoleCandidate Role = 1
	RoleLeader    Role = 2
)

var roleNames = [...]string{
	"FOLLOWER",
	"CANDIDATE",
	"LEADER",
}

func (r Role) Code() int32 { return int32(r) }

func (r Role) String() string {
	if r < 0 || int(r) >= len(roleNames) {
		return fmt.Sprintf("Role(%d)", int32(r))
	}
	return roleNames[r]
}

// RoleFromCode decodes a counter value. Valid codes are 0 through 2.
//
// Returns an error when code is outside that range. The returned Role is
// unspecified when err != nil.
func RoleFromCode(code int64) (Role, error) {
	if code < 0 || code >= int64(len(roleNames)) {
		return 0, fmt.Errorf("codecs: invalid role counter code: %d", code)
	}
	return Role(code), nil
}
