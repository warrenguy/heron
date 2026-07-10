package election

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidMemberSpec is returned when a member segment has fewer than six or
	// more than eight comma-separated fields. The error is wrapped with the segment
	// and full input value.
	ErrInvalidMemberSpec = errors.New("election: invalid member specification")

	// ErrInvalidMemberID is returned when the member id field is not a base-10
	// integer in int32 range.
	ErrInvalidMemberID = errors.New("election: invalid cluster member id")
)

// ParseMembers parses Java ClusterMember.parse format:
//
//	member-id,ingress,consensus,log,catchup,archive|next-member|...
//
// Optional 7th archive-response and 8th egress-response fields per member.
// Empty input returns nil, nil (Java EMPTY_MEMBERS).
// Pipe-only input (e.g. "|", "|||") returns a non-nil zero-length slice.
func ParseMembers(value string) ([]Member, error) {
	if value == "" {
		return nil, nil
	}

	segments := splitTrimTrailingEmpty(value, "|")
	members := make([]Member, len(segments))

	for i, segment := range segments {
		member, err := parseMemberSegment(segment, value)
		if err != nil {
			return nil, err
		}
		members[i] = member
	}

	return members, nil
}

func splitTrimTrailingEmpty(s, sep string) []string {
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func parseMemberSegment(segment, value string) (Member, error) {
	fields := splitTrimTrailingEmpty(segment, ",")
	if len(fields) < 6 || len(fields) > 8 {
		return Member{}, fmt.Errorf("%w: %q in %q", ErrInvalidMemberSpec, segment, value)
	}

	id64, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return Member{}, ErrInvalidMemberID
	}

	var archiveResponseEndpoint string
	if len(fields) > 6 {
		archiveResponseEndpoint = fields[6]
	}

	var egressResponseEndpoint string
	if len(fields) > 7 {
		egressResponseEndpoint = fields[7]
	}

	return Member{
		ID:                      int32(id64),
		IngressEndpoint:         fields[1],
		ConsensusEndpoint:       fields[2],
		LogEndpoint:             fields[3],
		CatchupEndpoint:         fields[4],
		ArchiveEndpoint:         fields[5],
		ArchiveResponseEndpoint: archiveResponseEndpoint,
		EgressResponseEndpoint:  egressResponseEndpoint,
		Endpoints:               strings.Join(fields[1:], ","),
	}, nil
}
