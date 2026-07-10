package election_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

const testClusterMembersOracle = "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005," +
	"localhost:9001|"

const testFailoverClusterMembers = "0,cluster-0:9002,cluster-0:9003,cluster-0:9004," +
	"cluster-0:9005,cluster-0:9001|1,cluster-1:9102,cluster-1:9103,cluster-1:9104," +
	"cluster-1:9105,cluster-1:9101|2,cluster-2:9202,cluster-2:9203,cluster-2:9204," +
	"cluster-2:9205,cluster-2:9201|"

func TestParseMembers_oracle(t *testing.T) {
	t.Parallel()

	t.Run("single member", func(t *testing.T) {
		t.Parallel()

		members, err := election.ParseMembers(testClusterMembersOracle)
		require.NoError(t, err)
		require.Len(t, members, 1)

		member := members[0]
		require.Equal(t, int32(0), member.ID)
		require.Equal(t, "localhost:9002", member.IngressEndpoint)
		require.Equal(t, "localhost:9003", member.ConsensusEndpoint)
		require.Equal(t, "localhost:9004", member.LogEndpoint)
		require.Equal(t, "localhost:9005", member.CatchupEndpoint)
		require.Equal(t, "localhost:9001", member.ArchiveEndpoint)
		require.Empty(t, member.ArchiveResponseEndpoint)
		require.Empty(t, member.EgressResponseEndpoint)
		require.Equal(
			t,
			"localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001",
			member.Endpoints,
		)
	})

	t.Run("trailing pipe discards empty segment", func(t *testing.T) {
		t.Parallel()

		members, err := election.ParseMembers(testClusterMembersOracle)
		require.NoError(t, err)
		require.Len(t, members, 1)
	})

	t.Run("multi member trailing pipes", func(t *testing.T) {
		t.Parallel()

		value := "0,ingress,consensus,log,catchup,archive|" +
			"1,ingress,consensus,log,catchup,archive|||"
		members, err := election.ParseMembers(value)
		require.NoError(t, err)
		require.Len(t, members, 2)
		require.Equal(t, int32(0), members[0].ID)
		require.Equal(t, int32(1), members[1].ID)
	})
}

func TestParseMembers_failoverThreeMembers(t *testing.T) {
	t.Parallel()

	members, err := election.ParseMembers(testFailoverClusterMembers)
	require.NoError(t, err)
	require.Len(t, members, 3)

	require.Equal(t, int32(0), members[0].ID)
	require.Equal(t, "cluster-0:9002", members[0].IngressEndpoint)
	require.Equal(t, "cluster-0:9001", members[0].ArchiveEndpoint)

	require.Equal(t, int32(1), members[1].ID)
	require.Equal(t, "cluster-1:9102", members[1].IngressEndpoint)
	require.Equal(t, "cluster-1:9101", members[1].ArchiveEndpoint)

	require.Equal(t, int32(2), members[2].ID)
	require.Equal(t, "cluster-2:9202", members[2].IngressEndpoint)
	require.Equal(t, "cluster-2:9201", members[2].ArchiveEndpoint)
}

func TestParseMembers_archiveResponseEndpoint(t *testing.T) {
	t.Parallel()

	value := "0,ingress,consensus,log,catchup,archive,archiveResponse|"
	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 1)

	member := members[0]
	require.Equal(t, "archiveResponse", member.ArchiveResponseEndpoint)
	require.Empty(t, member.EgressResponseEndpoint)
	require.Equal(
		t,
		"ingress,consensus,log,catchup,archive,archiveResponse",
		member.Endpoints,
	)
}

func TestParseMembers_emptyArchiveResponseEndpoint(t *testing.T) {
	t.Parallel()

	value := "0,i,c,l,u,a,,|"
	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 1)

	member := members[0]
	require.Empty(t, member.ArchiveResponseEndpoint)
	require.Empty(t, member.EgressResponseEndpoint)
	require.Equal(t, "i,c,l,u,a", member.Endpoints)
}

func TestParseMembers_egressResponseEndpoint(t *testing.T) {
	t.Parallel()

	value := "0,ingress,consensus,log,catchup,archive,archiveResponse,egressResponse|"
	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 1)

	member := members[0]
	require.Equal(t, "archiveResponse", member.ArchiveResponseEndpoint)
	require.Equal(t, "egressResponse", member.EgressResponseEndpoint)
	require.Equal(
		t,
		"ingress,consensus,log,catchup,archive,archiveResponse,egressResponse",
		member.Endpoints,
	)
}

func TestParseMembers_emptyArchiveNonEmptyEgressResponse(t *testing.T) {
	t.Parallel()

	value := "0,i,c,l,u,a,,egress|"
	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 1)

	member := members[0]
	require.Empty(t, member.ArchiveResponseEndpoint)
	require.Equal(t, "egress", member.EgressResponseEndpoint)
	require.Equal(t, "i,c,l,u,a,,egress", member.Endpoints)
}

func TestParseMembers_emptyEndpointFieldsAllowed(t *testing.T) {
	t.Parallel()

	value := "0,,consensus,log,catchup,archive"
	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 1)

	member := members[0]
	require.Empty(t, member.IngressEndpoint)
	require.Equal(t, "consensus", member.ConsensusEndpoint)
	require.Equal(t, ",consensus,log,catchup,archive", member.Endpoints)
}

func TestParseMembers_int32Boundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "max int32", id: "2147483647"},
		{name: "min int32", id: "-2147483648"},
		{name: "negative id", id: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			value := tt.id + ",ingress,consensus,log,catchup,archive"
			members, err := election.ParseMembers(value)
			require.NoError(t, err)
			require.Len(t, members, 1)

			want, err := strconv.ParseInt(tt.id, 10, 32)
			require.NoError(t, err)
			require.Equal(t, int32(want), members[0].ID)
		})
	}
}

func TestParseMembers_emptyInput(t *testing.T) {
	t.Parallel()

	members, err := election.ParseMembers("")
	require.NoError(t, err)
	require.Nil(t, members)
}

func TestParseMembers_pipeOnlyInput(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"|", "|||"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			members, err := election.ParseMembers(value)
			require.NoError(t, err)
			require.NotNil(t, members)
			require.Empty(t, members)
		})
	}
}

func TestParseMembers_internalEmptySegment(t *testing.T) {
	t.Parallel()

	_, err := election.ParseMembers(
		"0,ingress,consensus,log,catchup,archive||1,ingress,consensus,log,catchup,archive",
	)
	require.Error(t, err)
	require.ErrorIs(t, err, election.ErrInvalidMemberSpec)
}

func TestMembersPtrs_aliasesSliceElements(t *testing.T) {
	t.Parallel()

	members, err := election.ParseMembers(testClusterMembersOracle)
	require.NoError(t, err)

	ptrs := election.MembersPtrs(members)
	require.Len(t, ptrs, 1)
	require.Same(t, &members[0], ptrs[0])

	ptrs[0].ID = 99
	require.Equal(t, int32(99), members[0].ID)
}

func TestMembersPtrs_emptyInput(t *testing.T) {
	t.Parallel()

	t.Run("nil slice", func(t *testing.T) {
		t.Parallel()

		ptrs := election.MembersPtrs(nil)
		require.Empty(t, ptrs)
		require.NotNil(t, ptrs)
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		ptrs := election.MembersPtrs([]election.Member{})
		require.Empty(t, ptrs)
		require.NotNil(t, ptrs)
	})
}

func TestParseMembers_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		value            string
		wantErr          error
		wantErrContains  []string
		wantBareSentinel bool
	}{
		{
			name:            "five fields",
			value:           "0,ingress,consensus,log,catchup",
			wantErr:         election.ErrInvalidMemberSpec,
			wantErrContains: []string{"0,ingress,consensus,log,catchup"},
		},
		{
			name:    "whitespace only spaces",
			value:   "   ",
			wantErr: election.ErrInvalidMemberSpec,
		},
		{
			name:    "whitespace only tabs newlines",
			value:   "\t\n",
			wantErr: election.ErrInvalidMemberSpec,
		},
		{
			name:    "nine fields",
			value:   "0,a,b,c,d,e,f,g,h",
			wantErr: election.ErrInvalidMemberSpec,
		},
		{
			name:    "leading empty segment",
			value:   "|0,ingress,consensus,log,catchup,archive",
			wantErr: election.ErrInvalidMemberSpec,
		},
		{
			name:             "non numeric id",
			value:            "abc,ingress,consensus,log,catchup,archive",
			wantErr:          election.ErrInvalidMemberID,
			wantBareSentinel: true,
		},
		{
			name:             "empty id field",
			value:            ",ingress,consensus,log,catchup,archive",
			wantErr:          election.ErrInvalidMemberID,
			wantBareSentinel: true,
		},
		{
			name:             "id above int32 range",
			value:            "2147483648,ingress,consensus,log,catchup,archive",
			wantErr:          election.ErrInvalidMemberID,
			wantBareSentinel: true,
		},
		{
			name:             "id below int32 range",
			value:            "-2147483649,ingress,consensus,log,catchup,archive",
			wantErr:          election.ErrInvalidMemberID,
			wantBareSentinel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := election.ParseMembers(tt.value)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			for _, fragment := range tt.wantErrContains {
				require.ErrorContains(t, err, fragment)
			}
			if tt.wantBareSentinel {
				require.Equal(t, tt.wantErr, err)
			}
		})
	}
}

func TestParseMembers_validSpecBuilder(t *testing.T) {
	t.Parallel()

	value := buildMemberSpec(t, []memberSpecFields{
		{id: 0, endpoints: []string{"i0", "c0", "l0", "u0", "a0"}},
		{id: 7, endpoints: []string{"i7", "c7", "l7", "u7", "a7", "ar7", "er7"}},
	})

	members, err := election.ParseMembers(value)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.Equal(t, int32(7), members[1].ID)
	require.Equal(t, "er7", members[1].EgressResponseEndpoint)
}

type memberSpecFields struct {
	id        int
	endpoints []string
}

func buildMemberSpec(t *testing.T, specs []memberSpecFields) string {
	t.Helper()

	var segments []string
	for _, spec := range specs {
		require.GreaterOrEqual(t, len(spec.endpoints), 5)
		require.LessOrEqual(t, len(spec.endpoints), 7)
		fields := append([]string{strconv.Itoa(spec.id)}, spec.endpoints...)
		segments = append(segments, strings.Join(fields, ","))
	}
	return strings.Join(segments, "|")
}
