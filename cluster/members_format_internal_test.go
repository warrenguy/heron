package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestBuildIngressEndpointsString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		members []*election.Member
		want    string
	}{
		{
			name:    "nil slice",
			members: nil,
			want:    "",
		},
		{
			name:    "empty slice",
			members: []*election.Member{},
			want:    "",
		},
		{
			name:    "all nil entries",
			members: []*election.Member{nil, nil},
			want:    "",
		},
		{
			name: "mix nil and members",
			members: []*election.Member{
				nil,
				{ID: 1, IngressEndpoint: "b:9102"},
				nil,
				{ID: 0, IngressEndpoint: "a:9002"},
			},
			want: "0=a:9002,1=b:9102",
		},
		{
			name: "reverse id order sorts ascending",
			members: []*election.Member{
				{ID: 2, IngressEndpoint: "c:9202"},
				{ID: 0, IngressEndpoint: "a:9002"},
				{ID: 1, IngressEndpoint: "b:9102"},
			},
			want: "0=a:9002,1=b:9102,2=c:9202",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, buildIngressEndpointsString(tc.members))
		})
	}
}
