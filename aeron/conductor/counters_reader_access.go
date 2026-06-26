package conductor

import "github.com/warrenguy/heron/aeron/cnc"

// CountersReader returns the driver counters view bound to this conductor.
//
// Valid only for the owning Aeron client lifetime; after close the view is
// invalid. The returned reader aliases CNC mmap buffers; reads must follow the
// single-thread Client.DoWork contract.
func (conductor *Conductor) CountersReader() *cnc.CountersReader {
	if conductor == nil {
		return nil
	}
	return conductor.countersReader
}
