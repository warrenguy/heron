//go:build integration

package harness

const (
	PubSubRoundtripStreamID int32 = 1007

	PollUntilConnectedTimeoutStreamID      = 7990
	PollUntilImageCountZeroActiveStreamID  = 7995
	PollUntilImageCountOneWantTwoStreamID  = 7999
	PollUntilImageCountInvalidWantStreamID = 8000

	PollDeliveredTimeoutStreamID         = 7996
	PollDeliveredPartialDeliveryStreamID = 8001
	PollDeliveredInvalidWantStreamID     = 7997
	PollDeliveredLiveIPCStreamID         = 7998

	ConnectedWaitPublicationTimeoutStreamID  = 7991
	RequirePublicationLookupMissStreamID     = 7992
	WaitForIPCLinkConnectedStreamID          = 7993
	ConnectedWaitSubscriptionTimeoutStreamID = 7994
)
