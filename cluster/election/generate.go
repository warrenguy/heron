// Package election holds cluster election primitives.
// Node-state SBE flyweights are in nodewire.
//
// Regenerate flyweights with make sbe-generate-nodestate (requires a Java
// runtime). The go:generate directive below invokes the same script.
package election

//go:generate ../../scripts/generate-sbe.sh nodestate
