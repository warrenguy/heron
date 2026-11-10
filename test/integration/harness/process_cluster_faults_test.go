//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReturnerHasImages(t *testing.T) {
	t.Parallel()

	unbound := &MemberStatus{LogSessionID: 0, LogImageSessions: []int32{0, 4}}

	tests := []struct {
		name  string
		child *MemberStatus
		peers []imagePeer
		want  bool
	}{
		{
			name:  "session zero counts",
			child: boundStatus(0, 0, 4),
			peers: []imagePeer{{status: boundStatus(4, 0, 4), live: true}},
			want:  true,
		},
		{
			name:  "peer session zero counts",
			child: boundStatus(5, 5, 0),
			peers: []imagePeer{{status: boundStatus(0, 5, 0), live: true}},
			want:  true,
		},
		{
			name:  "both sessions zero",
			child: boundStatus(0, 0),
			peers: []imagePeer{{status: boundStatus(0, 0), live: true}},
			want:  true,
		},
		{
			name:  "unbound returner",
			child: unbound,
			peers: []imagePeer{{status: boundStatus(4, 0, 4), live: true}},
			want:  false,
		},
		{
			name:  "unbound peer",
			child: boundStatus(5, 5, 0),
			peers: []imagePeer{{status: unbound, live: true}},
			want:  false,
		},
		{
			name:  "missing peer image",
			child: boundStatus(5, 5),
			peers: []imagePeer{{status: boundStatus(4, 5, 4), live: true}},
			want:  false,
		},
		{
			name:  "missing status",
			child: nil,
			peers: []imagePeer{{status: boundStatus(4, 4), live: true}},
			want:  false,
		},
		{
			name:  "dead unbound peer ignored",
			child: boundStatus(3, 3),
			peers: []imagePeer{{status: unbound, live: false}},
			want:  true,
		},
		{
			name:  "live peer without status",
			child: boundStatus(3, 3, 4),
			peers: []imagePeer{{status: nil, live: true}},
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pc := clusterWith(tc.child)

			for _, peer := range tc.peers {
				pc.slots = append(pc.slots, processSlot{
					incarnations: []*ProcessMember{newStatusMember(peer.status, peer.live)},
				})
			}

			require.Equal(t, tc.want, pc.returnerHasImages(pc.slots[0].current()))
		})
	}
}

func TestHasLogImages_sessionZero(t *testing.T) {
	t.Parallel()

	require.True(t, clusterWith(boundStatus(0, 0)).hasLogImages())

	unbound := &MemberStatus{LogSessionID: 0, LogImageSessions: []int32{0}}
	require.False(t, clusterWith(unbound).hasLogImages())

	zero := boundStatus(0, 0, 7)
	seven := boundStatus(7, 0, 7)
	require.True(t, clusterWith(zero, seven).hasLogImages())
	require.False(t, clusterWith(zero, unbound).hasLogImages())
}

type imagePeer struct {
	status *MemberStatus
	live   bool
}

func boundStatus(id int32, images ...int32) *MemberStatus {
	return &MemberStatus{
		LogPublicationBound: true,
		LogSessionID:        id,
		LogImageSessions:    images,
	}
}

func newStatusMember(status *MemberStatus, live bool) *ProcessMember {
	member := &ProcessMember{status: status}

	if live {
		return member
	}

	done := make(chan struct{})
	close(done)
	member.doneCh = done

	return member
}

func clusterWith(statuses ...*MemberStatus) *ProcessCluster {
	slots := make([]processSlot, len(statuses))

	for i, status := range statuses {
		slots[i].incarnations = []*ProcessMember{newStatusMember(status, true)}
	}

	return &ProcessCluster{slots: slots}
}
