//go:build rapid

package election_test

import (
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestParseMembers_validSpecs_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		memberCount := rapid.IntRange(1, 5).Draw(rt, "memberCount")
		segments := make([]string, memberCount)
		segmentFields := make([][]string, memberCount)

		for i := range segments {
			fieldCount := rapid.IntRange(6, 8).Draw(rt, "fieldCount")
			fields := make([]string, fieldCount)
			fields[0] = strconv.Itoa(rapid.IntRange(0, 100).Draw(rt, "id"))
			for j := 1; j < fieldCount; j++ {
				if j >= 6 && j < fieldCount-1 && rapid.Bool().Draw(rt, "emptyOptional") {
					fields[j] = ""
					continue
				}
				fields[j] = rapid.StringMatching(`[a-z][a-z0-9]{0,7}`).Draw(rt, "endpoint")
			}
			fields = stripTrailingEmptyFields(fields)
			require.GreaterOrEqual(rt, len(fields), 6)
			segmentFields[i] = fields
			segments[i] = strings.Join(fields, ",")
		}

		value := strings.Join(segments, "|")
		members, err := election.ParseMembers(value)
		require.NoError(rt, err)
		require.Len(rt, members, memberCount)

		ptrs := election.MembersPtrs(members)
		require.Len(rt, ptrs, memberCount)
		for i := range members {
			require.Same(rt, &members[i], ptrs[i])
			require.Equal(rt, expectedEndpoints(segmentFields[i]), members[i].Endpoints)
		}
	})
}

func stripTrailingEmptyFields(fields []string) []string {
	for len(fields) > 0 && fields[len(fields)-1] == "" {
		fields = fields[:len(fields)-1]
	}
	return fields
}

func expectedEndpoints(fields []string) string {
	endpoints := strings.Join(fields[1:6], ",")
	if len(fields) > 6 {
		endpoints += "," + fields[6]
	}
	if len(fields) > 7 {
		endpoints += "," + fields[7]
	}
	return endpoints
}
