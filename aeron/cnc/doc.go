// Package cnc reads the media driver CnC file (cnc.dat).
//
// WaitCncReady waits until the driver publishes a compatible CnC version.
// MapCncFile maps the file. WireRings builds the to-driver ring buffer and the
// to-clients broadcast receiver over the mapping. CountersReader reads the
// counters region.
package cnc
