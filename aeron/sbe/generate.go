// Package sbe provides zero-alloc helpers for Aeron cluster SBE ingress framing.
// Flyweights are in generated/sbe.
//
// Regenerate flyweights with make sbe-generate (requires a Java runtime).
// The go:generate directive below invokes the same script but needs Java on PATH.
package sbe

//go:generate ../../scripts/generate-sbe.sh
