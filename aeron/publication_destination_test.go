package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const sendDestinationTestDestinationID int64 = 9002

type sendDestinationResource interface {
	Close() error
}

type sendDestinationOp struct {
	name       string
	newTarget  func(*testing.T) sendDestinationResource
	call       func(sendDestinationResource, string) error
	hasChannel bool
	closedErr  string
}

func newPublicationForDestinationTest(t *testing.T) sendDestinationResource {
	t.Helper()
	return aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 0)
}

func newExclusivePublicationForDestinationTest(t *testing.T) sendDestinationResource {
	t.Helper()
	return aeron.NewTestExclusivePublication(t, aeron.DefaultTestExclusivePublicationLogConfig(), 0)
}

var sendDestinationOps = []sendDestinationOp{
	{
		name:      "Publication.AddDestination",
		newTarget: newPublicationForDestinationTest,
		call: func(resource sendDestinationResource, channel string) error {
			return resource.(*aeron.Publication).AddDestination(channel)
		},
		hasChannel: true,
		closedErr:  "aeron: publication is closed",
	},
	{
		name:      "Publication.RemoveDestination",
		newTarget: newPublicationForDestinationTest,
		call: func(resource sendDestinationResource, channel string) error {
			return resource.(*aeron.Publication).RemoveDestination(channel)
		},
		hasChannel: true,
		closedErr:  "aeron: publication is closed",
	},
	{
		name:      "Publication.RemoveDestinationByID",
		newTarget: newPublicationForDestinationTest,
		call: func(resource sendDestinationResource, _ string) error {
			return resource.(*aeron.Publication).RemoveDestinationByID(
				sendDestinationTestDestinationID,
			)
		},
		closedErr: "aeron: publication is closed",
	},
	{
		name:      "ExclusivePublication.AddDestination",
		newTarget: newExclusivePublicationForDestinationTest,
		call: func(resource sendDestinationResource, channel string) error {
			return resource.(*aeron.ExclusivePublication).AddDestination(channel)
		},
		hasChannel: true,
		closedErr:  "aeron: exclusive publication is closed",
	},
	{
		name:      "ExclusivePublication.RemoveDestination",
		newTarget: newExclusivePublicationForDestinationTest,
		call: func(resource sendDestinationResource, channel string) error {
			return resource.(*aeron.ExclusivePublication).RemoveDestination(channel)
		},
		hasChannel: true,
		closedErr:  "aeron: exclusive publication is closed",
	},
	{
		name:      "ExclusivePublication.RemoveDestinationByID",
		newTarget: newExclusivePublicationForDestinationTest,
		call: func(resource sendDestinationResource, _ string) error {
			return resource.(*aeron.ExclusivePublication).RemoveDestinationByID(
				sendDestinationTestDestinationID,
			)
		},
		closedErr: "aeron: exclusive publication is closed",
	},
}

func TestPublication_sendDestination_preconditions(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*testing.T, sendDestinationResource)
		require func(*testing.T, sendDestinationOp, error)
	}{
		{
			name: "closed",
			setup: func(t *testing.T, resource sendDestinationResource) {
				require.NoError(t, resource.Close())
			},
			require: func(t *testing.T, op sendDestinationOp, err error) {
				require.EqualError(t, err, op.closedErr)
			},
		},
		{
			name:  "unbound",
			setup: func(*testing.T, sendDestinationResource) {},
			require: func(t *testing.T, _ sendDestinationOp, err error) {
				require.ErrorIs(t, err, aeron.ErrClientNotBound)
			},
		},
		{
			name: "client closed",
			setup: func(_ *testing.T, resource sendDestinationResource) {
				client := aeron.NewClientForTest(nil, func() int64 { return 0 })
				client.PublishClosedForTest()
				aeron.BindClientForTest(resource, client)
			},
			require: func(t *testing.T, _ sendDestinationOp, err error) {
				require.ErrorIs(t, err, aeron.ErrClientClosed)
			},
		},
		{
			name:  "client not ready",
			setup: bindNotReadyClientForDestinationTest,
			require: func(t *testing.T, _ sendDestinationOp, err error) {
				require.ErrorIs(t, err, aeron.ErrClientNotReady)
			},
		},
	}
	for _, op := range sendDestinationOps {
		t.Run(op.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					resource := op.newTarget(t)
					test.setup(t, resource)

					err := op.call(resource, destinationTestChannel)
					test.require(t, op, err)
				})
			}
		})
	}
}

func TestPublication_sendDestination_nilReceiver(t *testing.T) {
	var pub *aeron.Publication
	require.EqualError(t, pub.AddDestination(destinationTestChannel), "aeron: publication is closed")

	var ep *aeron.ExclusivePublication
	require.EqualError(
		t,
		ep.RemoveDestinationByID(sendDestinationTestDestinationID),
		"aeron: exclusive publication is closed",
	)
}

func TestPublication_sendDestination_validateChannel(t *testing.T) {
	// The bound client is not ready. A channel error shows that channel
	// validation runs before the ready check.
	cases := []struct {
		name    string
		channel string
		wantErr string
	}{
		{"empty", "", "aeron: channel is required"},
		{"non-ASCII high byte", "aeron:\x80", "aeron: channel must be ASCII"},
		{"non-ASCII utf8", "\u00fc", "aeron: channel must be ASCII"},
	}
	for _, op := range sendDestinationOps {
		if !op.hasChannel {
			continue
		}
		t.Run(op.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					resource := op.newTarget(t)
					bindNotReadyClientForDestinationTest(t, resource)

					err := op.call(resource, tc.channel)
					require.EqualError(t, err, tc.wantErr)
				})
			}
		})
	}
}

func bindNotReadyClientForDestinationTest(_ *testing.T, resource sendDestinationResource) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })
	aeron.BindClientForTest(resource, client)
}
