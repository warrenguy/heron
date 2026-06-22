//go:build integration

package harness

const (
	ChannelStatusCounterStreamID      int32 = 1001
	PubSubRoundtripStreamID           int32 = 1007
	PubSubTermRolloverStreamID        int32 = 1008
	PubSubCloseDrainStreamID          int32 = 1009
	MPSCMatrixPubSubStreamID          int32 = 1010
	MPSCMatrixSessionIDStreamID       int32 = 1011
	PubSubFragmentedRoundtripStreamID int32 = 1012
	PubSubTwoClientsRoundtripStreamID int32 = 1013
	PublicationLimitStreamID          int32 = 1014
	// Isolated from PublicationLimitStreamID for shared-driver cross-test safety.
	PublicationLimitAfterDrainStreamID              int32 = 1015
	ExclusivePublicationTwoClientsRoundtripStreamID int32 = 7206

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

	PollDeliveredSplitTimeoutStreamID         = 8016
	PollDeliveredSplitPartialDeliveryStreamID = 8017
	PollDeliveredSplitInvalidWantStreamID     = 8018
)
