//go:build !testhooks

package consensus

// noteDoWorkStep is a no-op in production builds; testhooks replaces it for
// DoWork step-order tests.
func noteDoWorkStep(string) {}
