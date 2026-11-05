// Package election implements the cluster election state machine. The package
// also parses cluster members. The package reads and writes the node state
// file. Package nodewire holds the node-state SBE flyweights.
//
// Run make sbe-generate-nodestate to regenerate the flyweights. The script
// needs java on PATH. The go:generate directive in generate.go runs the same
// script.
package election
