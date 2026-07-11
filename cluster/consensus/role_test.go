package consensus_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
)

func TestRoleFromCode(t *testing.T) {
	tests := []struct {
		name    string
		code    int64
		want    consensus.Role
		wantErr bool
	}{
		{name: "follower", code: 0, want: consensus.RoleFollower},
		{name: "candidate", code: 1, want: consensus.RoleCandidate},
		{name: "leader", code: 2, want: consensus.RoleLeader},
		{name: "above range", code: 3, wantErr: true},
		{name: "negative", code: -1, wantErr: true},
		{name: "far above range", code: 99, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := consensus.RoleFromCode(tt.code)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRoleCode(t *testing.T) {
	tests := []struct {
		role consensus.Role
		want int32
	}{
		{role: consensus.RoleFollower, want: 0},
		{role: consensus.RoleCandidate, want: 1},
		{role: consensus.RoleLeader, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.role.String(), func(t *testing.T) {
			require.Equal(t, tt.want, tt.role.Code())
		})
	}
}

func TestRoleString(t *testing.T) {
	tests := []struct {
		role consensus.Role
		want string
	}{
		{role: consensus.RoleFollower, want: "FOLLOWER"},
		{role: consensus.RoleCandidate, want: "CANDIDATE"},
		{role: consensus.RoleLeader, want: "LEADER"},
		{role: consensus.Role(99), want: "Role(99)"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.role.String())
		})
	}
}
