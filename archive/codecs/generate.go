// Package codecs provides zero-alloc helpers for Aeron Archive control SBE
// framing. Flyweights are in archivewire.
//
// Regenerate flyweights with make sbe-generate-archive (requires a Java
// runtime). The go:generate directive below invokes the same script but needs
// Java on PATH.
package codecs

//go:generate ../../scripts/generate-sbe.sh archive
