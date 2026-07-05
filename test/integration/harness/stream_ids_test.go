//go:build integration

package harness

import "testing"

// sharedDriverStreamIDs lists every named stream ID constant in stream_ids.go consumed
// by parallel Shared(t) tests. Any stream ID registered in a parallel Shared(t) harness
// test must appear here. Do not use +N expressions in test registrations; add a named
// constant instead.
var sharedDriverStreamIDs = map[string]int32{
	"ChannelStatusCounterStreamID":                    ChannelStatusCounterStreamID,
	"PubSubRoundtripStreamID":                         PubSubRoundtripStreamID,
	"PubSubTermRolloverStreamID":                      PubSubTermRolloverStreamID,
	"PubSubCloseDrainStreamID":                        PubSubCloseDrainStreamID,
	"MPSCMatrixPubSubStreamID":                        MPSCMatrixPubSubStreamID,
	"MPSCMatrixSessionIDStreamID":                     MPSCMatrixSessionIDStreamID,
	"PubSubFragmentedRoundtripStreamID":               PubSubFragmentedRoundtripStreamID,
	"PubSubTwoClientsRoundtripStreamID":               PubSubTwoClientsRoundtripStreamID,
	"PublicationLimitStreamID":                        PublicationLimitStreamID,
	"PublicationLimitAfterDrainStreamID":              PublicationLimitAfterDrainStreamID,
	"ExclusivePublicationTwoClientsRoundtripStreamID": ExclusivePublicationTwoClientsRoundtripStreamID,
	"PollUntilConnectedTimeoutStreamID":               PollUntilConnectedTimeoutStreamID,
	"PollUntilImageCountZeroActiveStreamID":           PollUntilImageCountZeroActiveStreamID,
	"PollUntilImageCountOneWantTwoStreamID":           PollUntilImageCountOneWantTwoStreamID,
	"PollUntilImageCountInvalidWantStreamID":          PollUntilImageCountInvalidWantStreamID,
	"PollDeliveredTimeoutStreamID":                    PollDeliveredTimeoutStreamID,
	"PollDeliveredPartialDeliveryStreamID":            PollDeliveredPartialDeliveryStreamID,
	"PollDeliveredInvalidWantStreamID":                PollDeliveredInvalidWantStreamID,
	"PollDeliveredLiveIPCStreamID":                    PollDeliveredLiveIPCStreamID,
	"PollReassembledTimeoutStreamID":                  PollReassembledTimeoutStreamID,
	"PollReassembledPartialDeliveryStreamID":          PollReassembledPartialDeliveryStreamID,
	"PollReassembledInvalidWantStreamID":              PollReassembledInvalidWantStreamID,
	"PollReassembledLiveIPCStreamID":                  PollReassembledLiveIPCStreamID,
	"PollReassembledNoImageStreamID":                  PollReassembledNoImageStreamID,
	"ConnectedWaitPublicationTimeoutStreamID":         ConnectedWaitPublicationTimeoutStreamID,
	"RequirePublicationLookupMissStreamID":            RequirePublicationLookupMissStreamID,
	"WaitForIPCLinkConnectedStreamID":                 WaitForIPCLinkConnectedStreamID,
	"ConnectedWaitSubscriptionTimeoutStreamID":        ConnectedWaitSubscriptionTimeoutStreamID,
	"OfferWithRetryWitnessCleanStreamID":              OfferWithRetryWitnessCleanStreamID,
	"OfferWithRetryWitnessBackPressuredStreamID":      OfferWithRetryWitnessBackPressuredStreamID,
	"PollUntilNoActiveImagesTimeoutStreamID":          PollUntilNoActiveImagesTimeoutStreamID,
	"PollSplitClientIPCLinkTimeoutStreamID":           PollSplitClientIPCLinkTimeoutStreamID,
	"PollSplitClientIPCLinkSubStreamID":               PollSplitClientIPCLinkSubStreamID,
	"PollUntilNoActiveImagesSplitTimeoutStreamID":     PollUntilNoActiveImagesSplitTimeoutStreamID,
	"PollDeliveredSplitTimeoutStreamID":               PollDeliveredSplitTimeoutStreamID,
	"PollDeliveredSplitPartialDeliveryStreamID":       PollDeliveredSplitPartialDeliveryStreamID,
	"PollDeliveredSplitInvalidWantStreamID":           PollDeliveredSplitInvalidWantStreamID,
	"OfferWithRetryNotConnectedTimeoutStreamID":       OfferWithRetryNotConnectedTimeoutStreamID,
	"OfferWithRetryNotConnectedThenConnectStreamID":   OfferWithRetryNotConnectedThenConnectStreamID,
	"OfferWithRetryClosedPublicationStreamID":         OfferWithRetryClosedPublicationStreamID,
	"AssertSubscribeMetadataStreamID":                 AssertSubscribeMetadataStreamID,
	"AeronHelpersSmokeStreamID":                       AeronHelpersSmokeStreamID,
	"AeronHelpersFragmentedSmokeStreamID":             AeronHelpersFragmentedSmokeStreamID,
	"PollUntilConnectedInvalidTimeoutStreamID":        PollUntilConnectedInvalidTimeoutStreamID,
	"OfferWithRetryInvalidTimeoutStreamID":            OfferWithRetryInvalidTimeoutStreamID,
}

func TestSharedDriverStreamIDs_unique(t *testing.T) {
	t.Parallel()

	seen := make(map[int32]string, len(sharedDriverStreamIDs))
	for name, id := range sharedDriverStreamIDs {
		if prev, ok := seen[id]; ok {
			t.Fatalf("stream ID %d used by both %s and %s", id, prev, name)
		}
		seen[id] = name
	}
}
