//go:build !testhooks

package archive

import "github.com/warrenguy/luas/aeron"

// recordControlOfferRetryDoWork is a no-op in production builds; testhooks replaces it
// for DoWork counting in control_offer_retry_test.go.
func recordControlOfferRetryDoWork(*aeron.Client) {}
