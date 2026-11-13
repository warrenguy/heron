//go:build integration || testhooks

package archivesbe

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
)

//go:embed fixtures/catalog.json
var catalogJSON []byte

const schemaVersion = 1

var (
	wireFixtures []WireFixture
	catalogRows  []catalogRow
)

type catalogRow struct {
	Template      string
	ActingVersion uint16
	Fields        map[string]any
}

type catalogFile struct {
	SchemaVersion json.Number   `json:"schemaVersion"`
	Fixtures      []fixtureFile `json:"fixtures"`
}

type fixtureFile struct {
	Name          string         `json:"name"`
	Template      string         `json:"template"`
	ActingVersion json.Number    `json:"actingVersion"`
	Hex           string         `json:"hex"`
	Fields        map[string]any `json:"fields"`
}

func init() {
	loaded, rows, err := parseCatalog(catalogJSON)
	if err != nil {
		panic(err)
	}

	wireFixtures = loaded
	catalogRows = rows
}

func parseCatalog(data []byte) ([]WireFixture, []catalogRow, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var file catalogFile
	if err := decoder.Decode(&file); err != nil {
		return nil, nil, fmt.Errorf("malformed json: %w", err)
	}

	if err := checkSchemaVersion(file.SchemaVersion); err != nil {
		return nil, nil, err
	}

	if len(file.Fixtures) == 0 {
		return nil, nil, errors.New("fixtures must be non-empty")
	}

	fixtures := make([]WireFixture, 0, len(file.Fixtures))
	rows := make([]catalogRow, 0, len(file.Fixtures))
	seen := make(map[string]struct{}, len(file.Fixtures))

	for i, entry := range file.Fixtures {
		if entry.Name == "" {
			return nil, nil, fmt.Errorf("fixture %d: empty name", i)
		}

		if _, dup := seen[entry.Name]; dup {
			return nil, nil, fmt.Errorf("duplicate fixture name %q", entry.Name)
		}

		seen[entry.Name] = struct{}{}

		fixture, err := parseFixture(entry)
		if err != nil {
			return nil, nil, fmt.Errorf("fixture %q: %w", entry.Name, err)
		}

		fixtures = append(fixtures, fixture)
		rows = append(rows, catalogRow{
			Template:      entry.Template,
			ActingVersion: fixture.ActingVersion,
			Fields:        entry.Fields,
		})
	}

	return fixtures, rows, nil
}

func parseFixture(entry fixtureFile) (WireFixture, error) {
	tmpl, ok := templateByName(entry.Template)
	if !ok {
		return WireFixture{}, fmt.Errorf("unknown template %q", entry.Template)
	}

	acting, err := actingVersionFrom(entry.ActingVersion)
	if err != nil {
		return WireFixture{}, err
	}

	wire, err := decodeWireHex(entry.Hex)
	if err != nil {
		return WireFixture{}, err
	}

	projected, err := resolveFields(tmpl, acting, entry.Fields)
	if err != nil {
		return WireFixture{}, err
	}

	fixture := WireFixture{
		Name:          entry.Name,
		TemplateID:    tmpl.id,
		ActingVersion: acting,
		Wire:          wire,
		Fields:        projected,
		ClientSent:    tmpl.encode != nil,
	}

	return fixture, nil
}

func checkSchemaVersion(number json.Number) error {
	parsed, err := jsonInt64(number)
	if err != nil || parsed != schemaVersion {
		return errors.New("unsupported schemaVersion")
	}

	return nil
}

func actingVersionFrom(number json.Number) (uint16, error) {
	if number == "" {
		return 0, errors.New("missing actingVersion")
	}

	parsed, err := jsonInt64(number)
	if err != nil {
		return 0, err
	}

	maxVersion := int64(archivewire.MessageHeaderSbeSchemaVersion)
	if parsed < 0 || parsed > maxVersion {
		return 0, fmt.Errorf("acting version %d", parsed)
	}

	return uint16(parsed), nil
}

func decodeLowerHex(text string) ([]byte, error) {
	if text != strings.ToLower(text) {
		return nil, errors.New("hex must be lowercase")
	}

	if len(text)%2 != 0 {
		return nil, errors.New("hex must have even length")
	}

	raw, err := hex.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("invalid hex: %w", err)
	}

	return raw, nil
}

func decodeWireHex(hexStr string) ([]byte, error) {
	if hexStr == "" {
		return nil, errors.New("missing hex")
	}

	raw, err := decodeLowerHex(hexStr)
	if err != nil {
		return nil, err
	}

	if len(raw) < codecs.MessageHeaderEncodedLen() {
		return nil, errors.New("hex shorter than header")
	}

	return raw, nil
}
