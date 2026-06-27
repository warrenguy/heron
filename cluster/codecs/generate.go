// Package codecs provides zero-alloc helpers for Aeron cluster SBE ingress framing.
// Flyweights are in gensbe.
//
// Regenerate flyweights with make sbe-generate (requires a Java runtime).
// The go:generate directive below invokes the same script but needs Java on PATH.
package codecs

//go:generate ../../scripts/generate-sbe.sh
