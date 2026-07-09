// Package election holds cluster election primitives. Schema 112 flyweights for
// node-state.dat live in nodewire.
//
// Regenerate with make sbe-generate-nodestate (requires a Java runtime).
// The go:generate directive below invokes the same script but needs Java on PATH.
package election

//go:generate ../../scripts/generate-sbe.sh nodestate
