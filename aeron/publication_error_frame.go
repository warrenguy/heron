package aeron

import (
	"net/netip"

	"github.com/warrenguy/heron/aeron/conductor"
)

// PublicationErrorFrame holds one publication error frame from the media
// driver. The driver sends the frame when a receiver rejects an image of a
// publication.
type PublicationErrorFrame struct {
	// RegistrationID equals OriginalRegistrationID of the matching publication.
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64

	// ErrorCode holds the raw wire value.
	ErrorCode int32

	// ErrorMessage comes from the remote receiver. Treat it as untrusted.
	ErrorMessage string

	// SourceAddress holds the address and port of the rejecting receiver. The
	// zero value means the address type is neither IPv4 nor IPv6.
	SourceAddress netip.AddrPort
}

// PublicationErrorFrameHandler receives publication error frames for
// publications on one Client.
//
// Behaviour: the Client calls the handler once per Publication or
// ExclusivePublication whose OriginalRegistrationID equals the frame
// RegistrationID. A frame that matches no publication causes no call.
//
// Concurrency: invoker-only. The handler runs on the goroutine that holds
// clientLock: Client.DoWork or the poll loop of a blocking Client call. The
// conductor holds no resource lock during the call.
//
// Client API restrictions match AvailableImageHandler. Handlers may call
// Publication.Close, ExclusivePublication.Close, and the Client.Publication,
// Client.ExclusivePublication, and Client.Subscription lookups. None of these
// takes clientLock.
//
// The Client counts the matching publications before the first call. A later
// call may arrive after its publication closed.
//
// Lifetime: the frame is a value. The handler may keep it after the call.
//
// Panic handling matches AvailableImageHandler. Delivery to the remaining
// matching publications continues after a recovered panic.
type PublicationErrorFrameHandler func(frame PublicationErrorFrame)

func publicationErrorFrameFrom(event conductor.PublicationError) PublicationErrorFrame {
	return PublicationErrorFrame{
		RegistrationID:            event.RegistrationID,
		DestinationRegistrationID: event.DestinationRegistrationID,
		SessionID:                 event.SessionID,
		StreamID:                  event.StreamID,
		ReceiverID:                event.ReceiverID,
		GroupTag:                  event.GroupTag,
		ErrorCode:                 event.ErrorCode,
		ErrorMessage:              event.ErrorMessage,
		SourceAddress:             publicationErrorSourceAddress(event),
	}
}

// publicationErrorSourceAddress returns the zero AddrPort for an unknown
// address type.
func publicationErrorSourceAddress(event conductor.PublicationError) netip.AddrPort {
	var address netip.Addr
	switch event.AddressType {
	case conductor.PublicationErrorAddressTypeIPv4:
		address = netip.AddrFrom4([4]byte(event.Address[:4]))
	case conductor.PublicationErrorAddressTypeIPv6:
		// Unmap matches Java InetAddress.getByAddress: an IPv4-mapped address
		// decodes as IPv4.
		address = netip.AddrFrom16(event.Address).Unmap()
	default:
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(address, event.AddressPort)
}
