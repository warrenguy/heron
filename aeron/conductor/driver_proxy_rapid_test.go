//go:build rapid

package conductor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	aerontestutil "github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/internal/testutil"
)

type driverProxyOp int

const (
	driverProxyOpClientKeepalive driverProxyOp = iota
	driverProxyOpAddPublication
	driverProxyOpAddExclusivePublication
	driverProxyOpAddSubscription
	driverProxyOpRemovePublication
	driverProxyOpRemoveSubscription
	driverProxyOpGetNextAvailableSessionID
)

type driverProxyExpectedRecord struct {
	msgTypeID int
	verify    func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int)
}

func drawDriverProxyOp(rt *rapid.T, name string) driverProxyOp {
	return driverProxyOp(
		rapid.IntRange(0, int(driverProxyOpGetNextAvailableSessionID)).Draw(rt, name),
	)
}

func verifyDriverProxyWantRecords(
	rt *rapid.T,
	ring *conductor.DriverProxy,
	want []driverProxyExpectedRecord,
) {
	rt.Helper()
	gotIndex := 0
	readCount := ring.Ring().Read(
		func(msgTypeID int, buffer atomic.AtomicBuffer, index, length int) {
			require.Less(rt, gotIndex, len(want), "unexpected committed record")
			expected := want[gotIndex]
			require.Equal(rt, expected.msgTypeID, msgTypeID)
			expected.verify(rt, buffer, index, length)
			gotIndex++
		},
	)
	require.Equal(rt, len(want), readCount)
}

func appendDriverProxyAddPublicationWant(
	want []driverProxyExpectedRecord,
	msgTypeID int,
	clientID int64,
	correlationID int64,
	streamID int32,
	channel string,
	bodyLength int,
) []driverProxyExpectedRecord {
	return append(want, driverProxyExpectedRecord{
		msgTypeID: msgTypeID,
		verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
			got, decodeErr := conductor.DecodePublicationMessage(buffer, index, length)
			require.NoError(t, decodeErr)
			require.Equal(t, conductor.PublicationMessage{
				ClientID:      clientID,
				CorrelationID: correlationID,
				StreamID:      streamID,
				Channel:       channel,
			}, got)
			require.Equal(t, bodyLength, length)
		},
	})
}

func drawDriverProxyEncodeFailureInput(
	rt *rapid.T,
	kind driverProxyEncodeFailureKind,
	clientID int64,
	step int,
) driverProxyEncodeFailureInput {
	input := driverProxyEncodeFailureInput{ClientID: clientID}
	switch kind {
	case driverProxyEncodeFailureCorrelated:
		input.CorrelationID = conductor.NullCorrelationID
	case driverProxyEncodeFailurePublication:
		input.Channel = testutil.DrawASCIIString(rt, fmt.Sprintf("failChannel%d", step), 64)
		input.CorrelationID = rapid.Int64().Draw(rt, fmt.Sprintf("failPubCorr%d", step))
		input.StreamID = rapid.Int32().Draw(rt, fmt.Sprintf("failPubStream%d", step))
	case driverProxyEncodeFailureSubscription:
		input.Channel = testutil.DrawASCIIString(rt, fmt.Sprintf("failSubChannel%d", step), 64)
		input.CorrelationID = rapid.Int64().Draw(rt, fmt.Sprintf("failSubCorr%d", step))
		input.StreamID = rapid.Int32().Draw(rt, fmt.Sprintf("failSubStream%d", step))
		input.RegistrationCorrelationID = conductor.NullRegistrationCorrelationID
	case driverProxyEncodeFailureRemovePublication:
		input.CorrelationID = rapid.Int64().Draw(rt, fmt.Sprintf("failRmPubCorr%d", step))
		input.RegistrationID = rapid.Int64().Draw(rt, fmt.Sprintf("failRmPubReg%d", step))
		input.Revoke = rapid.Bool().Draw(rt, fmt.Sprintf("failRmPubRevoke%d", step))
	case driverProxyEncodeFailureRemoveSubscription:
		input.CorrelationID = rapid.Int64().Draw(rt, fmt.Sprintf("failRmSubCorr%d", step))
		input.RegistrationID = rapid.Int64().Draw(rt, fmt.Sprintf("failRmSubReg%d", step))
	}
	return input
}

func runDriverProxyAddPublicationStep(
	rt *rapid.T,
	proxy *conductor.DriverProxy,
	want []driverProxyExpectedRecord,
	clientID int64,
	step int,
	namePrefix string,
	msgTypeID int,
	offer func(correlationID int64, streamID int32, channel string) error,
) ([]driverProxyExpectedRecord, bool) {
	correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("%sCorr%d", namePrefix, step))
	streamID := rapid.Int32().Draw(rt, fmt.Sprintf("%sStream%d", namePrefix, step))
	channel := testutil.DrawASCIIString(rt, fmt.Sprintf("%sChannel%d", namePrefix, step), 128)
	err := offer(correlationID, streamID, channel)
	if returnOnInsufficientCapacity(rt, proxy, want, err) {
		return want, true
	}
	bodyLength := conductor.PublicationMessageBodyLength(channel)
	return appendDriverProxyAddPublicationWant(
		want,
		msgTypeID,
		clientID,
		correlationID,
		streamID,
		channel,
		bodyLength,
	), false
}

func returnOnInsufficientCapacity(
	rt *rapid.T,
	ring *conductor.DriverProxy,
	want []driverProxyExpectedRecord,
	err error,
) bool {
	rt.Helper()
	if err == nil {
		return false
	}
	require.ErrorIs(rt, err, conductor.ErrRingInsufficientCapacity)
	verifyDriverProxyWantRecords(rt, ring, want)
	return true
}

func TestDriverProxy_offerReadRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const clientID int64 = 7
		ring, _ := newDriverProxyTestRing(t)
		proxy := conductor.NewDriverProxy(ring, clientID)

		stepCount := rapid.IntRange(1, 16).Draw(rt, "stepCount")
		var want []driverProxyExpectedRecord
		for i := range stepCount {
			op := drawDriverProxyOp(rt, fmt.Sprintf("op%d", i))
			switch op {
			case driverProxyOpClientKeepalive:
				err := proxy.ClientKeepalive()
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				want = append(want, driverProxyExpectedRecord{
					msgTypeID: conductor.ClientKeepalive,
					verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
						got, decodeErr := conductor.DecodeCorrelated(buffer, index)
						require.NoError(t, decodeErr)
						require.Equal(t, conductor.CorrelatedMessage{
							ClientID:      clientID,
							CorrelationID: conductor.NullCorrelationID,
						}, got)
						require.Equal(t, conductor.CorrelatedMessageLength, length)
					},
				})
			case driverProxyOpAddPublication, driverProxyOpAddExclusivePublication:
				namePrefix := "pub"
				msgTypeID := conductor.AddPublication
				offer := proxy.AddPublication
				if op == driverProxyOpAddExclusivePublication {
					namePrefix = "exPub"
					msgTypeID = conductor.AddExclusivePublication
					offer = proxy.AddExclusivePublication
				}
				var done bool
				want, done = runDriverProxyAddPublicationStep(
					rt,
					proxy,
					want,
					clientID,
					i,
					namePrefix,
					msgTypeID,
					offer,
				)
				if done {
					return
				}
			case driverProxyOpAddSubscription:
				correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("subCorr%d", i))
				streamID := int32(rapid.Int32().Draw(rt, fmt.Sprintf("subStream%d", i)))
				channel := testutil.DrawASCIIString(rt, fmt.Sprintf("subChannel%d", i), 128)
				err := proxy.AddSubscription(correlationID, streamID, channel)
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				bodyLength := conductor.SubscriptionMessageBodyLength(channel)
				want = append(want, driverProxyExpectedRecord{
					msgTypeID: conductor.AddSubscription,
					verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
						got, decodeErr := conductor.DecodeSubscriptionMessage(buffer, index, length)
						require.NoError(t, decodeErr)
						require.Equal(t, conductor.SubscriptionMessage{
							ClientID:                  clientID,
							CorrelationID:             correlationID,
							RegistrationCorrelationID: conductor.NullRegistrationCorrelationID,
							StreamID:                  streamID,
							Channel:                   channel,
						}, got)
						require.Equal(t, bodyLength, length)
					},
				})
			case driverProxyOpRemovePublication:
				correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("rmPubCorr%d", i))
				registrationID := rapid.Int64().Draw(rt, fmt.Sprintf("rmPubReg%d", i))
				revoke := rapid.Bool().Draw(rt, fmt.Sprintf("rmPubRevoke%d", i))
				err := proxy.RemovePublication(correlationID, registrationID, revoke)
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				want = append(want, driverProxyExpectedRecord{
					msgTypeID: conductor.RemovePublication,
					verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
						got, decodeErr := conductor.DecodeRemovePublicationMessage(
							buffer,
							index,
							conductor.RemovePublicationBodyLength,
						)
						require.NoError(t, decodeErr)
						require.Equal(t, conductor.RemovePublicationMessage{
							RemoveMessage: conductor.RemoveMessage{
								ClientID:       clientID,
								CorrelationID:  correlationID,
								RegistrationID: registrationID,
							},
							Revoke: revoke,
						}, got)
						require.Equal(t, conductor.RemovePublicationBodyLength, length)
					},
				})
			case driverProxyOpRemoveSubscription:
				correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("rmSubCorr%d", i))
				registrationID := rapid.Int64().Draw(rt, fmt.Sprintf("rmSubReg%d", i))
				err := proxy.RemoveSubscription(correlationID, registrationID)
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				want = append(want, driverProxyExpectedRecord{
					msgTypeID: conductor.RemoveSubscription,
					verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
						got, decodeErr := conductor.DecodeRemoveMessage(buffer, index, length)
						require.NoError(t, decodeErr)
						require.Equal(t, conductor.RemoveMessage{
							ClientID:       clientID,
							CorrelationID:  correlationID,
							RegistrationID: registrationID,
						}, got)
						require.Equal(t, conductor.RemoveMessageBodyLength, length)
					},
				})
			case driverProxyOpGetNextAvailableSessionID:
				correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("sessCorr%d", i))
				streamID := int32(rapid.Int32().Draw(rt, fmt.Sprintf("sessStream%d", i)))
				err := proxy.NextAvailableSessionID(correlationID, streamID)
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				want = append(want, driverProxyExpectedRecord{
					msgTypeID: conductor.GetNextAvailableSessionID,
					verify: func(t *rapid.T, buffer atomic.AtomicBuffer, index, length int) {
						got, decodeErr := conductor.DecodeCorrelated(buffer, index)
						require.NoError(t, decodeErr)
						require.Equal(t, conductor.CorrelatedMessage{
							ClientID:      clientID,
							CorrelationID: correlationID,
						}, got)
						require.Equal(t, streamID, buffer.GetInt32(index+conductor.CorrelatedMessageLength))
						require.Equal(t, conductor.GetNextAvailableSessionIDBodyLength, length)
					},
				})
			}
		}

		verifyDriverProxyWantRecords(rt, proxy, want)
	})
}

func TestDriverProxy_encodeFailureAborts_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const clientID int64 = 3
		ring, buffer := newDriverProxyTestRing(t)
		proxy := conductor.NewDriverProxy(ring, clientID)

		stepCount := rapid.IntRange(1, 12).Draw(rt, "stepCount")
		outOfBoundsOffset := buffer.Capacity()
		var want []driverProxyExpectedRecord
		for i := range stepCount {
			validOffer := rapid.Bool().Draw(rt, fmt.Sprintf("validOffer%d", i))
			if validOffer {
				correlationID := rapid.Int64().Draw(rt, fmt.Sprintf("corr%d", i))
				streamID := rapid.Int32().Draw(rt, fmt.Sprintf("stream%d", i))
				channel := testutil.DrawASCIIString(rt, fmt.Sprintf("channel%d", i), 64)
				err := proxy.AddPublication(correlationID, streamID, channel)
				if returnOnInsufficientCapacity(rt, proxy, want, err) {
					return
				}
				bodyLength := conductor.PublicationMessageBodyLength(channel)
				want = appendDriverProxyAddPublicationWant(
					want,
					conductor.AddPublication,
					clientID,
					correlationID,
					streamID,
					channel,
					bodyLength,
				)
				continue
			}

			failureKind := drawDriverProxyEncodeFailureKind(rt, fmt.Sprintf("failureKind%d", i))
			input := drawDriverProxyEncodeFailureInput(rt, failureKind, clientID, i)
			encodeCase := driverProxyEncodeFailureCaseForKind(failureKind, input)
			wantErrText := driverProxyOutOfBoundsMsg(
				encodeCase.OutOfBoundsKind,
				outOfBoundsOffset,
				outOfBoundsOffset,
			)

			committedBefore := aerontestutil.CommittedNonPaddingRecordCount(ring)
			err := proxy.OfferBodyForTest(
				encodeCase.MsgTypeID,
				encodeCase.BodyLength,
				false,
				func(encodedOffset int) error {
					return encodeCase.Encode(proxy.Ring().Buffer(), encodedOffset)
				},
			)
			require.ErrorIs(rt, err, encodeCase.WantErr)
			require.EqualError(rt, err, wantErrText)
			require.Equal(
				rt,
				committedBefore,
				aerontestutil.CommittedNonPaddingRecordCount(ring),
				"aborted claim should not commit",
			)
		}

		verifyDriverProxyWantRecords(rt, proxy, want)
	})
}
