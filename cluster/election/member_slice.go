package election

// MembersPtrs returns pointers aliasing elements of members.
// Mutating via pointer updates the backing slice.
// After ParseMembers, pass MembersPtrs(members) to quorum helpers.
//
// Valid only until members is reallocated.
func MembersPtrs(members []Member) []*Member {
	ptrs := make([]*Member, len(members))
	for i := range members {
		ptrs[i] = &members[i]
	}
	return ptrs
}
