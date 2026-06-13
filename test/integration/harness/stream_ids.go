//go:build integration

package harness

const (
	PubSubRoundtripStreamID           int32 = 1007
	PubSubTermRolloverStreamID        int32 = 1008
	PubSubCloseDrainStreamID          int32 = 1009
	MPSCMatrixPubSubStreamID          int32 = 1010
	MPSCMatrixSessionIDStreamID       int32 = 1011
	PubSubFragmentedRoundtripStreamID int32 = 1012

	PollUntilConnectedTimeoutStreamID      = 7990
	PollUntilImageCountZeroActiveStreamID  = 7995
	PollUntilImageCountOneWantTwoStreamID  = 7999
	PollUntilImageCountInvalidWantStreamID = 8000

	PollDeliveredTimeoutStreamID         = 7996
	PollDeliveredPartialDeliveryStreamID = 8001
	PollDeliveredInvalidWantStreamID     = 7997
	PollDeliveredLiveIPCStreamID         = 7998

	PollReassembledTimeoutStreamID         = 8002
	PollReassembledPartialDeliveryStreamID = 8005
	PollReassembledInvalidWantStreamID     = 8004
	PollReassembledLiveIPCStreamID         = 8003
	PollReassembledNoImageStreamID         = 8006

	ConnectedWaitPublicationTimeoutStreamID  = 7991
	RequirePublicationLookupMissStreamID     = 7992
	WaitForIPCLinkConnectedStreamID          = 7993
	ConnectedWaitSubscriptionTimeoutStreamID = 7994

	OfferWithRetryWitnessCleanStreamID         = 8010
	OfferWithRetryWitnessBackPressuredStreamID = 8011

	PollUntilNoActiveImagesTimeoutStreamID = 8012

	PollSplitClientIPCLinkTimeoutStreamID       = 8014
	PollUntilNoActiveImagesSplitTimeoutStreamID = 8015
)
