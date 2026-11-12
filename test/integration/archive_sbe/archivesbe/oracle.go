//go:build integration || testhooks

package archivesbe

import (
	"slices"

	"github.com/warrenguy/luas/archive/codecs"
)

// WireFixture is one archive control message from the catalog.
type WireFixture struct {
	Name          string
	TemplateID    uint16
	ActingVersion uint16
	Wire          []byte
	Fields        any
	ClientSent    bool
}

// Fixtures returns a copy of the catalog rows.
//
// The rows share Wire and the byte slices in Fields with the catalog. Callers
// treat both as read-only.
func Fixtures() []WireFixture {
	return slices.Clone(wireFixtures)
}

// ReencodeWire encodes a client-sent row into a buffer the size of fixture.Wire.
//
// It returns false for a server-sent template or for a Fields value the encoder
// rejects.
func ReencodeWire(fixture WireFixture) ([]byte, bool) {
	tmpl, ok := templateByID(fixture.TemplateID)
	if !ok || tmpl.encode == nil || len(fixture.Wire) == 0 {
		return nil, false
	}

	buf := make([]byte, len(fixture.Wire))
	wireEnd, ok := tmpl.encode(buf, fixture)
	if !ok {
		return nil, false
	}

	return buf[:wireEnd], true
}

// ExpectedDecodeFields returns the fields a decode reads at
// fixture.ActingVersion.
//
// Replay, auth connect, and replicate rows read SBE null or nil for fields
// newer than that version. Other templates return fixture.Fields.
func ExpectedDecodeFields(fixture WireFixture) any {
	switch fields := fixture.Fields.(type) {
	case codecs.ReplayRequestFields:
		return codecs.ReplayRequestOracleFields(fields, fixture.ActingVersion)
	case codecs.AuthConnectRequestFields:
		return codecs.AuthConnectRequestOracleFields(fields, fixture.ActingVersion)
	case codecs.ReplicateRequest2Fields:
		return codecs.ReplicateRequest2OracleFields(fields, fixture.ActingVersion)
	default:
		return fixture.Fields
	}
}
