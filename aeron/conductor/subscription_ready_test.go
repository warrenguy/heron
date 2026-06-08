package conductor

import (
	"testing"
)

func TestDecodeSubscriptionReady_shortBody(t *testing.T) {
	testDecodeEventShortBody(
		t,
		SubscriptionReadyBodyLength,
		[]shortBodyTestCase{
			{name: "fixedMinusOne", bodyLength: SubscriptionReadyBodyLength - 1},
			{name: "empty", bodyLength: 0},
		},
		DecodeSubscriptionReady,
	)
}
