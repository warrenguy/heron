//go:build testhooks

package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs/gensbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
)

func withConnectContext(mutate func(*client.ClusterContext)) func(*client.ConnectResult) {
	return func(result *client.ConnectResult) {
		ctx := result.Context
		mutate(&ctx)
		result.Context = ctx
	}
}

func requireDecodedTemplate[T any](
	t *testing.T,
	wire []byte,
	wantTemplateID uint16,
) T {
	t.Helper()

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, wantTemplateID, decoded.TemplateID)

	fields, ok := decoded.Fields.(T)
	require.True(t, ok)
	return fields
}

func requireDecodedSessionMessageHeader(
	t *testing.T,
	wire []byte,
	wantClusterSessionID int64,
	wantLeadershipTermID int64,
	wantTimestamp int64,
) int {
	t.Helper()

	fields := requireDecodedTemplate[codecs.SessionMessageHeaderFields](
		t,
		wire,
		gensbe.SessionMessageHeaderSbeTemplateID,
	)
	require.Equal(t, wantClusterSessionID, fields.ClusterSessionID)
	require.Equal(t, wantLeadershipTermID, fields.LeadershipTermID)
	require.Equal(t, wantTimestamp, fields.Timestamp)
	return client.SessionHeaderLength()
}

func TestNewAeronCluster_rejectsNilResult(t *testing.T) {
	_, err := client.NewAeronCluster(nil)
	require.EqualError(t, err, "cluster client: nil connect result")
}

func TestNewAeronCluster_rejectsInvalidConnectResult(t *testing.T) {
	leaderEndpoint := "localhost:20110"

	tests := []struct {
		name    string
		mutate  func(*client.ConnectResult)
		wantErr string
	}{
		{
			name: "nil_ingress",
			mutate: func(result *client.ConnectResult) {
				result.IngressPublication = nil
			},
			wantErr: "cluster client: ingress publication missing",
		},
		{
			name: "closed_ingress",
			mutate: func(result *client.ConnectResult) {
				pub := result.IngressPublication.(*aeron.ExclusivePublication)
				require.NoError(t, pub.Close())
			},
			wantErr: "cluster client: ingress publication is closed",
		},
		{
			name: "closed_egress_subscription",
			mutate: func(result *client.ConnectResult) {
				require.NoError(t, result.EgressSubscription.Close())
			},
			wantErr: "cluster client: egress subscription is closed",
		},
		{
			name: "nil_egress_subscription",
			mutate: func(result *client.ConnectResult) {
				result.EgressSubscription = nil
			},
			wantErr: "cluster client: egress subscription missing",
		},
		{
			name:    "nil_egress_handler",
			mutate:  withConnectContext(withNilEgressHandler),
			wantErr: errEgressHandlerRequired,
		},
		{
			name:    "typed_nil_egress_handler",
			mutate:  withConnectContext(withTypedNilEgressHandlers),
			wantErr: errEgressHandlerRequired,
		},
		{
			name: "nil_aeron",
			mutate: func(result *client.ConnectResult) {
				result.Aeron = nil
			},
			wantErr: "cluster client: aeron client missing",
		},
		{
			name: "unknown_leader_member",
			mutate: func(result *client.ConnectResult) {
				result.LeaderMemberID = testLeaderMemberID
				result.MemberIngressMap = client.MemberIngressMap{
					99: &client.MemberIngress{
						MemberID:    99,
						Endpoint:    leaderEndpoint,
						Publication: result.IngressPublication,
					},
				}
			},
			wantErr: "cluster client: member id 1 not in ingress map",
		},
		{
			name: "missing_leader_ingress",
			mutate: func(result *client.ConnectResult) {
				result.LeaderMemberID = testLeaderMemberID
				result.MemberIngressMap = client.MemberIngressMap{
					testLeaderMemberID: &client.MemberIngress{
						MemberID: testLeaderMemberID,
						Endpoint: leaderEndpoint,
					},
				}
			},
			wantErr: "cluster client: leader ingress publication missing",
		},
		{
			name: "leader_ingress_mismatch",
			mutate: func(result *client.ConnectResult) {
				otherPub := aeron.NewTestExclusivePublicationForHooks(t)
				result.LeaderMemberID = testLeaderMemberID
				result.MemberIngressMap = client.MemberIngressMap{
					testLeaderMemberID: &client.MemberIngress{
						MemberID:    testLeaderMemberID,
						Endpoint:    leaderEndpoint,
						Publication: otherPub,
					},
				}
			},
			wantErr: "cluster client: leader ingress publication mismatch",
		},
		{
			name: "channel_only_ingress_mismatch",
			mutate: func(result *client.ConnectResult) {
				otherPub := aeron.NewTestExclusivePublicationForHooks(t)
				result.MemberIngressMap = client.MemberIngressMap{
					channelOnlyMemberID: &client.MemberIngress{
						MemberID:    channelOnlyMemberID,
						Publication: otherPub,
					},
				}
			},
			wantErr: "cluster client: leader ingress publication mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := newTestConnectResult(t, tc.mutate)
			_, err := client.NewAeronCluster(result)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewAeronCluster_ingressScratchSizedFromPublication(t *testing.T) {
	// ingressScratch cap is unexported; behavioral check mirrors header+maxPayload.
	result := newTestConnectResult(t, nil)
	pub := result.IngressPublication.(*aeron.ExclusivePublication)
	maxPayload := pub.MaxPayloadLength()
	payload := make([]byte, maxPayload-client.SessionHeaderLength())

	ac, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	require.Positive(t, ac.Offer(payload))
	oversize := make([]byte, maxPayload-client.SessionHeaderLength()+1)
	require.Equal(t, aeron.UnsupportedMessageLength, ac.Offer(oversize))
}

func TestNewAeronCluster_transfersOwnership(t *testing.T) {
	result := newTestConnectResult(t, nil)
	ingress := result.IngressPublication
	egressSub := result.EgressSubscription

	ac, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	require.Nil(t, result.IngressPublication)
	require.Nil(t, result.EgressSubscription)
	require.Nil(t, result.EgressImage)
	require.Same(t, ingress, ac.IngressPublication())
	require.Same(t, egressSub, ac.EgressSubscription())
}

func TestAeronCluster_Offer_prependsSessionHeader(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	payload := []byte("offer-app-payload")
	position := ac.Offer(payload)
	require.Positive(t, position)

	pub := ac.IngressPublication().(*aeron.ExclusivePublication)
	frame := exclusiveIngressTermBytes(t, pub, 0, 0)
	wire := frame[logbuffer.OffsetFramePayload:]

	headerEnd := requireDecodedSessionMessageHeader(
		t,
		wire,
		testClusterSessionID,
		testLeadershipTermID,
		testIngressTimestampNs,
	)
	require.Equal(t, payload, wire[headerEnd:])
}

func TestAeronCluster_Offer_returnsNotConnectedWhenClosed(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, ac *client.AeronCluster)
		wantCode int64
	}{
		{
			name: "cluster_closed",
			setup: func(t *testing.T, ac *client.AeronCluster) {
				require.NoError(t, ac.Close())
			},
			wantCode: aeron.Closed,
		},
		{
			name: "ingress_closed",
			setup: func(t *testing.T, ac *client.AeronCluster) {
				pub := ac.IngressPublication().(*aeron.ExclusivePublication)
				require.NoError(t, pub.Close())
			},
			wantCode: aeron.NotConnected,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ac := newTestAeronCluster(t, nil)
			tc.setup(t, ac)
			require.Equal(t, tc.wantCode, ac.Offer([]byte("payload")))
		})
	}
}

func TestAeronCluster_Offer_payloadLengthBoundaries(t *testing.T) {
	t.Run("exclusive_unsupported_message_length", func(t *testing.T) {
		ac := newTestAeronCluster(t, nil)
		pub := ac.IngressPublication().(*aeron.ExclusivePublication)
		maxPayload := pub.MaxPayloadLength()
		oversizePayload := make([]byte, maxPayload-client.SessionHeaderLength()+1)

		got := ac.Offer(oversizePayload)
		require.Equal(t, aeron.UnsupportedMessageLength, got)
	})

	t.Run("concurrent_fragmented_offer", func(t *testing.T) {
		result := newTestConnectResult(t, func(result *client.ConnectResult) {
			pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
			result.IngressPublication = pub
			result.MemberIngressMap = client.MemberIngressMap{
				testLeaderMemberID: &client.MemberIngress{
					MemberID:    testLeaderMemberID,
					Publication: pub,
				},
			}
		})
		ac, err := client.NewAeronCluster(result)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ac.Close() })

		pub := ac.IngressPublication().(*aeron.Publication)
		maxPayload := pub.MaxPayloadLength()
		fragmentedPayload := make([]byte, maxPayload-client.SessionHeaderLength()+1)
		require.Positive(t, ac.Offer(fragmentedPayload))
	})

	t.Run("concurrent_offer_large_payload", func(t *testing.T) {
		result := newTestConnectResult(t, func(result *client.ConnectResult) {
			pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
			result.IngressPublication = pub
			result.MemberIngressMap = client.MemberIngressMap{
				testLeaderMemberID: &client.MemberIngress{
					MemberID:    testLeaderMemberID,
					Publication: pub,
				},
			}
		})
		ac, err := client.NewAeronCluster(result)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ac.Close() })

		pub := ac.IngressPublication().(*aeron.Publication)
		maxPayload := pub.MaxPayloadLength()
		largePayload := make([]byte, maxPayload+1)
		require.Positive(t, ac.Offer(largePayload))
	})

	t.Run("concurrent_offer_exceeds_max_message_length", func(t *testing.T) {
		result := newTestConnectResult(t, func(result *client.ConnectResult) {
			pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
			result.IngressPublication = pub
			result.MemberIngressMap = client.MemberIngressMap{
				testLeaderMemberID: &client.MemberIngress{
					MemberID:    testLeaderMemberID,
					Publication: pub,
				},
			}
		})
		ac, err := client.NewAeronCluster(result)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ac.Close() })

		pub := ac.IngressPublication().(*aeron.Publication)
		maxMessageLength := pub.MaxMessageLength()
		oversizePayload := make([]byte, maxMessageLength-client.SessionHeaderLength()+1)
		require.Equal(t, aeron.MaxPositionExceeded, ac.Offer(oversizePayload))
		require.Equal(t, client.SessionStateConnected, ac.SessionState())
	})
}

func TestAeronCluster_TryClaimCommit_roundtrip(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	payload := []byte("try-claim-payload")
	claim, position, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)
	require.Equal(t, len(payload), claim.PayloadLength)

	pub := ac.IngressPublication().(*aeron.ExclusivePublication)
	termBuf := pub.TermBuffer(claim.TermIndex)
	headerStart := claim.FrameOffset + logbuffer.OffsetFramePayload
	headerWire := termBuf.Bytes()[headerStart : headerStart+client.SessionHeaderLength()]
	requireDecodedSessionMessageHeader(
		t,
		headerWire,
		testClusterSessionID,
		testLeadershipTermID,
		testIngressTimestampNs,
	)

	committedPosition, err := ac.CommitClaim(claim, payload)
	require.NoError(t, err)
	require.Equal(t, position, committedPosition)
	require.Equal(t, client.SessionStateConnected, ac.SessionState())
	payloadOffset := headerStart + client.SessionHeaderLength()
	require.Equal(t, payload, termBuf.Bytes()[payloadOffset:payloadOffset+len(payload)])

	claim2, position2, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position2)
	_, err = ac.CommitClaim(claim2, payload)
	require.NoError(t, err)
}

func TestAeronCluster_TryClaim_invalidLengthReturnsError(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	pub := ac.IngressPublication().(*aeron.ExclusivePublication)
	maxPayload := pub.MaxPayloadLength()

	tests := []struct {
		name       string
		payloadLen int
		wantErr    string
		exactErr   bool
	}{
		{
			name:       "negative_length",
			payloadLen: -1,
			wantErr:    "cluster client: negative payload length -1",
			exactErr:   true,
		},
		{
			name:       "oversize_length",
			payloadLen: maxPayload - client.SessionHeaderLength() + 1,
			wantErr:    "exceeds maxPayloadLength=",
			exactErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claim, position, err := ac.TryClaim(tc.payloadLen)
			if tc.exactErr {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
			require.Equal(t, int64(0), position)
			require.Zero(t, claim)
		})
	}
}

func TestAeronCluster_CommitClaim_wrongLengthAborts(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	claim, position, err := ac.TryClaim(8)
	require.NoError(t, err)
	require.Positive(t, position)

	_, err = ac.CommitClaim(claim, []byte("short"))
	require.EqualError(
		t,
		err,
		"cluster client: payload length 5 does not match claimed 8",
	)

	claim2, position2, err := ac.TryClaim(8)
	require.NoError(t, err)
	require.Positive(t, position2)
	require.Equal(t, 8, claim2.PayloadLength)
	_, err = ac.CommitClaim(claim2, make([]byte, 8))
	require.NoError(t, err)
}

func TestAeronCluster_SendKeepAlive_encodesAndOffers(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	position := ac.SendKeepAlive()
	require.Positive(t, position)

	pub := ac.IngressPublication().(*aeron.ExclusivePublication)
	wire := exclusiveIngressTermBytes(t, pub, 0, 0)[logbuffer.OffsetFramePayload:]

	fields := requireDecodedTemplate[codecs.SessionKeepAliveFields](
		t,
		wire,
		gensbe.SessionKeepAliveSbeTemplateID,
	)
	require.Equal(t, testClusterSessionID, fields.ClusterSessionID)
	require.Equal(t, testLeadershipTermID, fields.LeadershipTermID)
}

func TestAeronCluster_Close_sendsSessionCloseBestEffort(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	pub := ac.IngressPublication().(*aeron.ExclusivePublication)

	require.NoError(t, ac.Close())

	wireAfterFirstClose := exclusiveIngressTermBytes(t, pub, 0, 0)
	payloadWire := wireAfterFirstClose[logbuffer.OffsetFramePayload:]
	require.NotEmpty(t, payloadWire)

	fields := requireDecodedTemplate[codecs.SessionCloseRequestFields](
		t,
		payloadWire,
		gensbe.SessionCloseRequestSbeTemplateID,
	)
	require.Equal(t, testClusterSessionID, fields.ClusterSessionID)
	require.Equal(t, testLeadershipTermID, fields.LeadershipTermID)

	// Close ignores backpressure; a second close must not offer again.
	require.NoError(t, ac.Close())
	wireAfterSecondClose := exclusiveIngressTermBytes(t, pub, 0, 0)
	require.Equal(t, wireAfterFirstClose, wireAfterSecondClose)
}

func TestAeronCluster_Close_idempotent(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	require.NoError(t, ac.Close())
	require.NoError(t, ac.Close())
	require.True(t, ac.IsClosed())
}

func TestAeronCluster_PollEgress_returnsZeroWhenClosed(t *testing.T) {
	ac := newTestAeronCluster(t, nil)

	require.NoError(t, ac.Close())
	require.Equal(t, 0, ac.PollEgress())
}

func TestAeronCluster_ingressMethods_returnClosedWhenClosed(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	payload := []byte("closed-path")

	claim, position, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)

	require.NoError(t, ac.Close())

	claim2, position2, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Equal(t, aeron.Closed, position2)
	require.Zero(t, claim2)

	committed, err := ac.CommitClaim(claim, payload)
	require.NoError(t, err)
	require.Equal(t, aeron.Closed, committed)

	require.NoError(t, ac.AbortClaim(claim2))

	require.Equal(t, aeron.Closed, ac.SendKeepAlive())
}

func TestAeronCluster_ingressMethods_returnNotConnectedWhenIngressClosed(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	payload := []byte("not-connected-path")
	pub := ac.IngressPublication().(*aeron.ExclusivePublication)

	claim, position, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)
	require.NoError(t, ac.AbortClaim(claim))

	require.NoError(t, pub.Close())

	claim2, position2, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Equal(t, aeron.NotConnected, position2)
	require.Zero(t, claim2)

	require.Equal(t, aeron.NotConnected, ac.Offer(payload))

	require.Equal(t, aeron.NotConnected, ac.SendKeepAlive())
}

func TestAeronCluster_AbortClaim_releasesClaimForReclaim(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	payload := []byte("abort-reclaim")

	claim, position, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)

	require.NoError(t, ac.AbortClaim(claim))

	claim2, position2, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position2)
	_, err = ac.CommitClaim(claim2, payload)
	require.NoError(t, err)
}

func TestAeronCluster_AbortClaim_noOpWhenClosed(t *testing.T) {
	ac := newTestAeronCluster(t, nil)
	payload := []byte("abort-closed")

	claim, position, err := ac.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)

	require.NoError(t, ac.Close())
	require.NoError(t, ac.AbortClaim(claim))
}
