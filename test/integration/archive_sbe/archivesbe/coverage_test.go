//go:build integration || testhooks

package archivesbe

import (
	"encoding/xml"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/archive/codecs"
	"github.com/warrenguy/luas/archive/codecs/archivewire"
)

func TestCatalog_coverage(t *testing.T) {
	t.Parallel()

	schema := loadArchiveSchema(t)

	t.Run("templates", func(t *testing.T) {
		t.Parallel()
		requireTemplateCoverage(t, schema)
	})

	t.Run("fields", func(t *testing.T) {
		t.Parallel()
		requireFieldCoverage(t, schema)
	})

	t.Run("versionBranches", func(t *testing.T) {
		t.Parallel()
		requireVersionBranches(t, schema)
	})

	t.Run("enumValues", func(t *testing.T) {
		t.Parallel()
		requireEnumCoverage(t, schema)
	})
}

func requireTemplateCoverage(t testing.TB, schema archiveSchema) {
	t.Helper()

	require.Equal(t, archivewire.MessageHeaderSbeSchemaID, schema.id)
	require.Equal(t, archivewire.MessageHeaderSbeSchemaVersion, schema.version)
	require.Len(t, schema.messages, len(templateCatalog))

	byName := make(map[string]templateDef, len(templateCatalog))
	ids := make([]uint16, 0, len(templateCatalog))

	for _, tmpl := range templateCatalog {
		_, dup := byName[tmpl.name]
		require.False(t, dup, tmpl.name)
		byName[tmpl.name] = tmpl
		ids = append(ids, tmpl.id)
	}

	for _, message := range schema.messages {
		tmpl, ok := byName[message.catalogName]
		require.True(t, ok, message.name)
		require.Equal(t, message.id, tmpl.id)

		require.True(t, catalogHas(message.catalogName, schema.version), message.catalogName)
	}

	var bounds []uint16

	for id := range uint32(math.MaxUint16) + 1 {
		_, _, ok := codecs.ArchiveRootBlockLengthBounds(uint16(id), schema.version)
		if ok {
			bounds = append(bounds, uint16(id))
		}
	}

	require.ElementsMatch(t, ids, bounds)
}

func requireFieldCoverage(t testing.TB, schema archiveSchema) {
	t.Helper()

	for _, message := range schema.messages {
		tmpl, ok := templateByName(message.catalogName)
		require.True(t, ok, message.catalogName)
		require.Equal(t, memberSpecs(message.fields), fieldSpecs(tmpl.fixed), message.catalogName)
		require.Equal(t, memberSpecs(message.ascii), fieldSpecs(tmpl.ascii), message.catalogName)
		require.Equal(t, memberSpecs(message.data), fieldSpecs(tmpl.data), message.catalogName)

		for _, member := range slices.Concat(message.ascii, message.data) {
			require.True(t, varContentPresent(message.catalogName, member), member.Name)
		}
	}
}

func requireVersionBranches(t testing.TB, schema archiveSchema) {
	t.Helper()

	for _, message := range schema.messages {
		for _, member := range slices.Concat(message.fields, message.ascii, message.data) {
			if member.Since == 0 {
				continue
			}

			below := member.Since - 1
			require.True(t, catalogHas(message.catalogName, below), message.catalogName+" "+member.Name)
		}
	}
}

func requireEnumCoverage(t testing.TB, schema archiveSchema) {
	t.Helper()

	for _, message := range schema.messages {
		for _, field := range message.fields {
			xmlValues, isEnum := schema.enums[field.Type]
			if !isEnum {
				continue
			}

			table, ok := enumTables[field.Type]
			require.True(t, ok, field.Type)
			require.Len(t, table, len(xmlValues), field.Type)

			for _, value := range xmlValues {
				label := message.catalogName + " " + field.Name + " " + value.Name
				got, present := table[value.Name]
				require.True(t, present, label)
				require.Equal(t, value.Value, got, label)
				require.True(t, enumValueInRows(message.catalogName, field.Name, value.Name), label)
			}
		}
	}
}

func catalogHas(template string, acting uint16) bool {
	for _, row := range catalogRows {
		if row.Template == template && row.ActingVersion == acting {
			return true
		}
	}

	return false
}

func varContentPresent(template string, member schemaMember) bool {
	for _, row := range catalogRows {
		if row.Template != template || row.ActingVersion < member.Since {
			continue
		}

		if nonEmptyVar(row.Fields[member.Name]) {
			return true
		}
	}

	return false
}

func nonEmptyVar(value any) bool {
	switch typed := value.(type) {
	case string:
		return typed != ""
	case map[string]any:
		text, ok := typed["hex"].(string)

		return ok && text != ""
	}

	return false
}

func enumValueInRows(template, field, valueName string) bool {
	for _, row := range catalogRows {
		if row.Template == template && row.Fields[field] == valueName {
			return true
		}
	}

	return false
}

type memberSpec struct {
	name  string
	since uint16
}

func memberSpecs(members []schemaMember) []memberSpec {
	specs := make([]memberSpec, len(members))

	for i, member := range members {
		specs[i] = memberSpec{name: member.Name, since: member.Since}
	}

	return specs
}

func fieldSpecs(fields []fieldDef) []memberSpec {
	specs := make([]memberSpec, len(fields))

	for i, field := range fields {
		specs[i] = memberSpec{name: field.key, since: field.since}
	}

	return specs
}

type archiveSchema struct {
	id       uint16
	version  uint16
	messages []schemaMessage
	enums    map[string][]schemaEnumValue
}

type schemaMessage struct {
	name        string
	catalogName string
	id          uint16
	fields      []schemaMember
	ascii       []schemaMember
	data        []schemaMember
}

type schemaMember struct {
	Name  string `xml:"name,attr"`
	Type  string `xml:"type,attr"`
	Since uint16 `xml:"sinceVersion,attr"`
}

type schemaEnumValue struct {
	Name  string `xml:"name,attr"`
	Value int32  `xml:",chardata"`
}

func loadArchiveSchema(tb testing.TB) archiveSchema {
	tb.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(tb, ok)

	path := filepath.Join(
		filepath.Dir(filename),
		"..", "..", "..", "..",
		"specs", "archive-codecs-client.xml",
	)
	data, err := os.ReadFile(path)
	require.NoError(tb, err)

	return parseArchiveSchema(tb, data)
}

type xmlSchema struct {
	ID       uint16       `xml:"id,attr"`
	Version  uint16       `xml:"version,attr"`
	Enums    []xmlEnum    `xml:"types>enum"`
	Messages []xmlMessage `xml:"message"`
}

type xmlEnum struct {
	Name   string            `xml:"name,attr"`
	Values []schemaEnumValue `xml:"validValue"`
}

type xmlMessage struct {
	Name   string         `xml:"name,attr"`
	ID     uint16         `xml:"id,attr"`
	Fields []schemaMember `xml:"field"`
	Data   []schemaMember `xml:"data"`
}

func parseArchiveSchema(tb testing.TB, data []byte) archiveSchema {
	tb.Helper()

	var parsed xmlSchema
	require.NoError(tb, xml.Unmarshal(data, &parsed))

	schema := archiveSchema{
		id:      parsed.ID,
		version: parsed.Version,
		enums:   make(map[string][]schemaEnumValue, len(parsed.Enums)),
	}

	for _, enum := range parsed.Enums {
		schema.enums[enum.Name] = enum.Values
	}

	for _, element := range parsed.Messages {
		message := schemaMessage{
			name:        element.Name,
			catalogName: lowerCamel(element.Name),
			id:          element.ID,
			fields:      element.Fields,
		}

		for _, member := range element.Data {
			switch member.Type {
			case "varAsciiEncoding":
				message.ascii = append(message.ascii, member)
			case "varDataEncoding":
				message.data = append(message.data, member)
			default:
				require.Fail(tb, "unknown data encoding "+member.Type)
			}
		}

		schema.messages = append(schema.messages, message)
	}

	return schema
}

func lowerCamel(name string) string {
	if name == "" {
		return ""
	}

	return strings.ToLower(name[:1]) + name[1:]
}
