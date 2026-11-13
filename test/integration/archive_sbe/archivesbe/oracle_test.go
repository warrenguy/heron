//go:build integration || testhooks

package archivesbe

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
)

func TestWireFixtures_oracle(t *testing.T) {
	t.Parallel()

	for _, fixture := range Fixtures() {
		t.Run(fixture.Name, func(t *testing.T) {
			t.Parallel()

			_, templateID, schemaID, version, ok := codecs.DecodeMessageHeader(fixture.Wire, 0)
			require.True(t, ok)
			require.Equal(t, fixture.TemplateID, templateID)
			require.Equal(t, archivewire.MessageHeaderSbeSchemaID, schemaID)
			require.Equal(t, fixture.ActingVersion, version)

			wireEnd, ok := codecs.ArchiveMessageWireEnd(
				fixture.Wire,
				0,
				codecs.ArchiveDecodeMaxVarLen,
			)
			require.True(t, ok)
			require.Equal(t, len(fixture.Wire), wireEnd)

			decoded, ok := codecs.TryDecodeByTemplate(
				fixture.Wire,
				0,
				codecs.ArchiveDecodeMaxVarLen,
			)
			require.True(t, ok)
			require.Equal(t, fixture.TemplateID, decoded.TemplateID)
			require.Equal(t, ExpectedDecodeFields(fixture), decoded.Fields)

			if !fixture.ClientSent {
				return
			}

			wire, ok := ReencodeWire(fixture)
			require.True(t, ok)
			require.Equal(t, fixture.Wire, wire)
		})
	}
}

func TestReencodeWire_rejects(t *testing.T) {
	t.Parallel()

	fixtures := Fixtures()

	serverIndex := slices.IndexFunc(fixtures, func(fixture WireFixture) bool {
		return !fixture.ClientSent
	})
	require.NotEqual(t, -1, serverIndex)

	clientIndex := slices.IndexFunc(fixtures, func(fixture WireFixture) bool {
		return fixture.ClientSent
	})
	require.NotEqual(t, -1, clientIndex)

	_, ok := ReencodeWire(fixtures[serverIndex])
	require.False(t, ok)

	client := fixtures[clientIndex]
	client.Fields = codecs.ControlResponseFields{}
	_, ok = ReencodeWire(client)
	require.False(t, ok)
}
