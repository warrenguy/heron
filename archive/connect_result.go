package archive

import (
	"github.com/warrenguy/heron/aeron"
)

// ConnectResult holds archive control handles and session ids after a successful
// connect handshake. Poll transfers ownership of connect resources into the result;
// see AsyncConnect.Close for success vs failure cleanup.
//
// Proxy Try methods and Poller.Poll never call DoWork; the caller must drive
// Aeron.DoWork between post-connect poll and offer steps (or use
// [Client.ControlOfferUntilPositive] for offer retries).
type ConnectResult struct {
	ControlSessionID int64
	ArchiveID        int64
	ServerVersion    int32

	ControlPublication  ControlPublication
	ControlSubscription *aeron.Subscription

	Proxy  *Proxy
	Poller *ControlResponsePoller

	Aeron           *aeron.Client
	OwnsAeronClient bool
	Config          Config
}
