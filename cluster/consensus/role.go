package consensus

import "fmt"

// Role is the cluster node role written to CLUSTER_NODE_ROLE_TYPE_ID (201).
// Mirrors aeron cluster Role.
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

// RoleFromCode decodes a counter value. Valid codes are 0-2 only.
// Values outside 0-2 return an error.
//
// The returned Role is unspecified when err != nil.
func RoleFromCode(code int64) (Role, error) {
	if code < 0 || code >= int64(len(roleNames)) {
		return 0, fmt.Errorf("consensus: invalid role counter code: %d", code)
	}
	return Role(code), nil
}
