// Package archive implements the Aeron Archive control client.
//
// Connect runs the blocking connect handshake and returns a Client.
// AsyncConnect runs the same handshake without blocking. Proxy encodes control
// requests. Client.Poll demuxes control responses, recording descriptors, and
// recording signals from one control-response subscription.
package archive
