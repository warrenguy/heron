package aeron

import (
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const publicationErrorFrameForeignRegistrationID int64 = 9009

func publicationErrorFrameTestEvent(registrationID int64) conductor.PublicationError {
	return conductor.PublicationError{
		RegistrationID:            registrationID,
		DestinationRegistrationID: 3003,
		SessionID:                 42,
		StreamID:                  clientAddStreamID,
		ReceiverID:                4004,
		GroupTag:                  5005,
		AddressType:               conductor.PublicationErrorAddressTypeIPv4,
		AddressPort:               40123,
		Address:                   [16]byte{192, 0, 2, 7},
		ErrorCode:                 11,
		ErrorMessage:              "image rejected",
	}
}

func TestPublicationErrorFrameFrom(t *testing.T) {
	t.Parallel()

	ipv6 := [16]byte{0x20, 0x01, 0x0d, 0xb8, 15: 0x01}
	ipv4Mapped := [16]byte{10: 0xff, 11: 0xff, 12: 198, 13: 51, 14: 100, 15: 9}

	testCases := []struct {
		name        string
		addressType int16
		address     [16]byte
		errorCode   int32
		wantSource  netip.AddrPort
	}{
		{
			name:        "ipv4",
			addressType: conductor.PublicationErrorAddressTypeIPv4,
			address:     [16]byte{192, 0, 2, 7, 15: 0xff},
			errorCode:   11,
			wantSource:  netip.MustParseAddrPort("192.0.2.7:40123"),
		},
		{
			name:        "ipv6",
			addressType: conductor.PublicationErrorAddressTypeIPv6,
			address:     ipv6,
			errorCode:   11,
			wantSource:  netip.MustParseAddrPort("[2001:db8::1]:40123"),
		},
		{
			name:        "ipv4MappedIPv6",
			addressType: conductor.PublicationErrorAddressTypeIPv6,
			address:     ipv4Mapped,
			errorCode:   11,
			wantSource:  netip.MustParseAddrPort("198.51.100.9:40123"),
		},
		{
			name:        "unknownAddressType",
			addressType: 0,
			address:     [16]byte{192, 0, 2, 7},
			errorCode:   11,
			wantSource:  netip.AddrPort{},
		},
		{
			name:        "rawErrorCode",
			addressType: conductor.PublicationErrorAddressTypeIPv4,
			address:     [16]byte{192, 0, 2, 7},
			errorCode:   99,
			wantSource:  netip.MustParseAddrPort("192.0.2.7:40123"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			event := publicationErrorFrameTestEvent(clientAddPublicationRegistrationID)
			event.AddressType = testCase.addressType
			event.Address = testCase.address
			event.ErrorCode = testCase.errorCode

			require.Equal(t, PublicationErrorFrame{
				RegistrationID:            clientAddPublicationRegistrationID,
				DestinationRegistrationID: 3003,
				SessionID:                 42,
				StreamID:                  clientAddStreamID,
				ReceiverID:                4004,
				GroupTag:                  5005,
				ErrorCode:                 testCase.errorCode,
				ErrorMessage:              "image rejected",
				SourceAddress:             testCase.wantSource,
			}, publicationErrorFrameFrom(event))
		})
	}
}

func TestClient_PublicationErrorFrameHandler_localFrame(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)
			var frames []PublicationErrorFrame
			cfg := Config{
				AeronDir: dir,
				PublicationErrorFrameHandler: func(frame PublicationErrorFrame) {
					frames = append(frames, frame)
				},
			}
			client, fixture := newClientAddFixtureWithConfig(t, cfg)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			correlationID, err := testCase.offerAdd(client)
			require.NoError(t, err)
			pub := pollAddUntilReady(
				t,
				client,
				correlationID,
				func(t *testing.T, deadline time.Time) {
					_, _ = testCase.pollRespond(t, fixture, logPath, deadline, nil)
				},
				testCase.pollAdd,
				testCase.pollTimeoutMsg,
			)
			registrationID, originalRegistrationID := addPublicationIDs(pub)
			require.Equal(t, clientAddPublicationRegistrationID, originalRegistrationID)
			require.NotEqual(t, originalRegistrationID, registrationID)

			frameRegistrationIDs := []int64{
				originalRegistrationID,
				registrationID,
				publicationErrorFrameForeignRegistrationID,
			}
			for _, frameRegistrationID := range frameRegistrationIDs {
				require.NoError(t, fixture.Transmit(
					conductor.OnPublicationError,
					conductor.PublicationErrorBytes(
						publicationErrorFrameTestEvent(frameRegistrationID),
					),
				))
			}
			// One DoWork tick receives at most one broadcast message.
			for range frameRegistrationIDs {
				require.Positive(t, client.DoWork())
			}

			require.Equal(t, []PublicationErrorFrame{{
				RegistrationID:            originalRegistrationID,
				DestinationRegistrationID: 3003,
				SessionID:                 42,
				StreamID:                  clientAddStreamID,
				ReceiverID:                4004,
				GroupTag:                  5005,
				ErrorCode:                 11,
				ErrorMessage:              "image rejected",
				SourceAddress:             netip.MustParseAddrPort("192.0.2.7:40123"),
			}}, frames)
			require.Equal(t, 1, client.PublicationCountForTest())
		})
	}
}

type closablePublication interface {
	Close() error
	IsClosed() bool
}

func TestClient_PublicationErrorFrameHandler_reentrantCalls(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dir, logPath := testCase.setupLog(t)

			var (
				client           *Client
				pendingID        int64
				registrationID   int64
				pub              closablePublication
				gotErrors        []error
				added            *Subscription
				addErr           error
				publicationFound bool
				lookupOK         bool
				closeErr         error
			)
			cfg := Config{
				AeronDir: dir,
				ErrorHandler: func(err error) {
					gotErrors = append(gotErrors, err)
				},
				PublicationErrorFrameHandler: func(PublicationErrorFrame) {
					client.CancelPending(pendingID)
					added, addErr = client.AddSubscription(clientAddStreamID, clientAddChannel)
					_, publicationFound = client.Publication(registrationID)
					looked, ok := testCase.lookup(client, registrationID)
					lookupOK = ok && looked == pub
					closeErr = pub.Close()
				},
			}
			client, fixture := newClientAddFixtureWithConfig(t, cfg)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			correlationID, err := testCase.offerAdd(client)
			require.NoError(t, err)
			addedPub := pollAddUntilReady(
				t,
				client,
				correlationID,
				func(t *testing.T, deadline time.Time) {
					_, _ = testCase.pollRespond(t, fixture, logPath, deadline, nil)
				},
				testCase.pollAdd,
				testCase.pollTimeoutMsg,
			)
			var ok bool
			pub, ok = addedPub.(closablePublication)
			require.True(t, ok)
			var originalRegistrationID int64
			registrationID, originalRegistrationID = addPublicationIDs(pub)

			pendingID, err = client.OfferAddSubscription(
				clientAddStreamID,
				clientAddChannel,
			)
			require.NoError(t, err)

			require.NoError(t, fixture.Transmit(
				conductor.OnPublicationError,
				conductor.PublicationErrorBytes(
					publicationErrorFrameTestEvent(originalRegistrationID),
				),
			))

			var workCount int
			testutil.RequireReturnsWithin(t, clientConcurrentResultTimeout, func() {
				workCount = client.DoWork()
			})

			require.Equal(t, 1, workCount)
			require.Nil(t, added)
			require.ErrorIs(t, addErr, ErrReentrantCall)
			require.Len(t, gotErrors, 1)
			require.ErrorIs(t, gotErrors[0], ErrReentrantCall)
			_, isPublication := pub.(*Publication)
			require.Equal(t, isPublication, publicationFound)
			require.True(t, lookupOK)
			require.NoError(t, closeErr)
			require.True(t, pub.IsClosed())
			_, pending := fixture.Conductor.PendingResult(pendingID)
			require.True(t, pending)
		})
	}
}
