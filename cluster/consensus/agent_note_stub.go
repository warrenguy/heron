//go:build !testhooks

package consensus

// noteDoWorkStep is a no-op in production builds; testhooks replaces it for
// DoWork step-order tests.
func noteDoWorkStep(string) {}

// noteModuleStateChange does nothing in production builds. The testhooks
// build replaces the stub for module state transition tests.
func noteModuleStateChange(ModuleState, ModuleState, string) {}

// noteUnexpectedHigherTerm is a no-op in production builds; testhooks replaces
// it for higher-term ingress observation tests.
func noteUnexpectedHigherTerm(int64, int64) {}
