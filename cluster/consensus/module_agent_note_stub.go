//go:build !testhooks

package consensus

// noteDoWorkStep is a no-op in production builds; testhooks replaces it for
// DoWork step-order tests.
func noteDoWorkStep(string) {}

// noteUnexpectedHigherTerm is a no-op in production builds; testhooks replaces
// it for higher-term ingress observation tests.
func noteUnexpectedHigherTerm(int64, int64) {}
