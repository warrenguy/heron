package codecs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestRoleFromCode(t *testing.T) {
	tests := []struct {
		name    string
		code    int64
		want    codecs.Role
		wantErr bool
	}{
		{name: "follower", code: 0, want: codecs.RoleFollower},
		{name: "candidate", code: 1, want: codecs.RoleCandidate},
		{name: "leader", code: 2, want: codecs.RoleLeader},
		{name: "above range", code: 3, wantErr: true},
		{name: "negative", code: -1, wantErr: true},
		{name: "far above range", code: 99, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := codecs.RoleFromCode(tt.code)
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
		role codecs.Role
		want int32
	}{
		{role: codecs.RoleFollower, want: 0},
		{role: codecs.RoleCandidate, want: 1},
		{role: codecs.RoleLeader, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.role.String(), func(t *testing.T) {
			require.Equal(t, tt.want, tt.role.Code())
		})
	}
}

func TestRoleString(t *testing.T) {
	tests := []struct {
		role codecs.Role
		want string
	}{
		{role: codecs.RoleFollower, want: "FOLLOWER"},
		{role: codecs.RoleCandidate, want: "CANDIDATE"},
		{role: codecs.RoleLeader, want: "LEADER"},
		{role: codecs.Role(99), want: "Role(99)"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.role.String())
		})
	}
}
