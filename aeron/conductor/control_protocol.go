package conductor

import "github.com/warrenguy/heron/aeron/cnc"

const (
	AddPublication            = 0x01
	RemovePublication         = 0x02
	AddExclusivePublication   = 0x03
	AddSubscription           = 0x04
	RemoveSubscription        = 0x05
	ClientKeepalive           = 0x06
	AddDestination            = 0x07
	RemoveDestination         = 0x08
	AddCounter                = 0x09
	RemoveCounter             = 0x0A
	ClientClose               = 0x0B
	AddReceiveDestination     = 0x0C
	RemoveReceiveDestination  = 0x0D
	TerminateDriver           = 0x0E
	AddStaticCounter          = 0x0F
	RejectImage               = 0x10
	RemoveDestinationByID     = 0x11
	GetNextAvailableSessionID = 0x12

	OnError                     = 0x0F01
	OnAvailableImage            = 0x0F02
	OnPublicationReady          = 0x0F03
	OnOperationSuccess          = 0x0F04
	OnUnavailableImage          = 0x0F05
	OnExclusivePublicationReady = 0x0F06
	OnSubscriptionReady         = 0x0F07
	OnCounterReady              = 0x0F08
	OnUnavailableCounter        = 0x0F09
	OnClientTimeout             = 0x0F0A
	OnStaticCounter             = 0x0F0B
	OnPublicationError          = 0x0F0C
	OnNextAvailableSessionID    = 0x0F0D

	SystemCounterControlProtocolVersion = cnc.SystemCounterControlProtocolVersionID

	ErrorCodeUnknownSubscription int32 = 2
	ErrorCodeUnknownPublication  int32 = 3
)

var ControlProtocolVersion = cnc.ComposeSemanticVersion(1, 0, 0)
