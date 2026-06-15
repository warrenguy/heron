package trim_test

import (
	"bytes"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/tools/cluster-codecs-trim/trim"
)

func TestTrimFiltersFixture(t *testing.T) {
	t.Parallel()

	root := parseFixture(t)
	out, err := trim.Trim(root)
	require.NoError(t, err)

	types, ok := trim.FindChildForTest(out, "types")
	require.True(t, ok)

	typeNames := make(map[string]struct{})
	for _, child := range types.Children {
		name := trim.AttrValueForTest(child, "name")
		if name != "" {
			typeNames[name] = struct{}{}
		}
	}
	require.Equal(t, map[string]struct{}{
		"messageHeader": {},
		"EventCode":     {},
		"time_t":        {},
	}, typeNames)
	require.Equal(t, []string{"SessionMessageHeader"}, messageNames(out))
}

func TestTrimDropsUnknownElements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tag      string
		elemName string
		keepName string
	}{
		{name: "composite", tag: "composite", elemName: "unusedComposite", keepName: "messageHeader"},
		{name: "enum", tag: "enum", elemName: "UnusedEnum", keepName: "EventCode"},
		{name: "type", tag: "type", elemName: "unused_t", keepName: "time_t"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := minimalRoot(
				typeChild(tc.tag, tc.elemName),
				typeChild(tc.tag, tc.keepName),
			)
			out, err := trim.Trim(root)
			require.NoError(t, err)

			types, ok := trim.FindChildForTest(out, "types")
			require.True(t, ok)
			require.Len(t, types.Children, 1)
			require.Equal(t, tc.keepName, trim.AttrValueForTest(types.Children[0], "name"))
		})
	}
}

func TestTrimKeepsAllCategories(t *testing.T) {
	t.Parallel()

	root := minimalRoot(
		typeChild("composite", "messageHeader"),
		typeChild("composite", "groupSizeEncoding"),
		typeChild("composite", "varAsciiEncoding"),
		typeChild("composite", "varDataEncoding"),
		typeChild("enum", "EventCode"),
		typeChild("enum", "AdminRequestType"),
		typeChild("enum", "AdminResponseCode"),
		typeChild("type", "time_t"),
		typeChild("type", "version_t"),
	)
	out, err := trim.Trim(root)
	require.NoError(t, err)

	types, ok := trim.FindChildForTest(out, "types")
	require.True(t, ok)
	require.Len(t, types.Children, 9)
}

func TestTrimEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		root          trim.Element
		wantErr       string
		wantTypeCount int
		wantMessages  []string
	}{
		{
			name:    "missing types",
			root:    trim.Element{XMLName: xml.Name{Local: "messageSchema"}},
			wantErr: "missing <types>",
		},
		{
			name:          "empty types",
			root:          minimalRoot(),
			wantTypeCount: 0,
			wantMessages:  nil,
		},
		{
			name: "no matching messages",
			root: minimalRoot(
				typeChild("type", "time_t"),
				messageChild("UnusedMessage"),
			),
			wantTypeCount: 1,
			wantMessages:  nil,
		},
		{
			name: "keeps client messages in order",
			root: minimalRoot(
				messageChild("SessionEvent"),
				messageChild("SessionMessageHeader"),
				messageChild("UnusedMessage"),
			),
			wantMessages: []string{"SessionEvent", "SessionMessageHeader"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := trim.Trim(tc.root)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			types, ok := trim.FindChildForTest(out, "types")
			require.True(t, ok)
			require.Len(t, types.Children, tc.wantTypeCount)

			require.Equal(t, tc.wantMessages, messageNames(out))
		})
	}
}

func TestTrimDeepCopy(t *testing.T) {
	t.Parallel()

	root := parseFixture(t)
	out, err := trim.Trim(root)
	require.NoError(t, err)

	types, ok := trim.FindChildForTest(out, "types")
	require.True(t, ok)
	require.NotEmpty(t, types.Children)

	types.Children[0].Attr[0].Value = "mutated"
	rootTypes, ok := trim.FindChildForTest(root, "types")
	require.True(t, ok)
	require.NotEqual(t, "mutated", trim.AttrValueForTest(rootTypes.Children[0], "name"))
}

func TestWriteEscapes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		root trim.Element
		want string
	}{
		{
			name: "element_text",
			root: trim.Element{
				XMLName: xml.Name{Local: "enum"},
				Children: []trim.Element{{
					XMLName: xml.Name{Local: "validValue"},
					Attr:    []xml.Attr{{Name: xml.Name{Local: "name"}, Value: "EDGE"}},
					Text:    "0 & 1 < 2",
				}},
			},
			want: "0 &amp; 1 &lt; 2",
		},
		{
			name: "attribute",
			root: trim.Element{
				XMLName: xml.Name{Local: "message"},
				Attr: []xml.Attr{{
					Name:  xml.Name{Local: "description"},
					Value: `say "hi" & <bye>`,
				}},
			},
			want: `description="say &#34;hi&#34; &amp; &lt;bye&gt;"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			require.NoError(t, trim.Write(&buf, tc.root))
			require.Contains(t, buf.String(), tc.want)
		})
	}
}

func TestWriteTrimmedFixtureGolden(t *testing.T) {
	t.Parallel()

	root := parseFixture(t)
	out, err := trim.Trim(root)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, trim.Write(&buf, out))

	expected := readTestdata(t, "cluster-codecs-trimmed-fixture.xml")
	require.Equal(t, expected, buf.String())
}

func TestParseSkipsLeadingCommentAndPI(t *testing.T) {
	t.Parallel()

	const input = `<?xml version="1.0" encoding="UTF-8"?>
<!-- upstream comment -->
<?xml-stylesheet type="text/xsl" href="style.xsl"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" id="1">
    <types></types>
</sbe:messageSchema>`

	root, err := trim.Parse(strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "messageSchema", root.XMLName.Local)
	require.Equal(t, "http://fixprotocol.io/2016/sbe", root.XMLName.Space)
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "empty",
			input:   "",
			wantErr: "empty document",
		},
		{
			name:    "malformed",
			input:   "<root><unclosed>",
			wantErr: "XML syntax error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := trim.Parse(strings.NewReader(tc.input))
			require.Error(t, err)
			if tc.wantErr == "XML syntax error" {
				require.Contains(t, err.Error(), "XML syntax error")
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

var (
	fixtureOnce sync.Once
	fixtureRoot trim.Element
	fixtureErr  error
)

func parseFixture(t *testing.T) trim.Element {
	t.Helper()

	fixtureOnce.Do(func() {
		path := filepath.Join("..", "testdata", "cluster-codecs-fixture.xml")
		f, err := os.Open(path)
		if err != nil {
			fixtureErr = err
			return
		}
		fixtureRoot, fixtureErr = trim.Parse(f)
		if closeErr := f.Close(); fixtureErr == nil {
			fixtureErr = closeErr
		}
	})
	require.NoError(t, fixtureErr)
	return fixtureRoot
}

func readTestdata(t *testing.T, name string) string {
	t.Helper()

	data, err := os.ReadFile(toolTestdata(t, name))
	require.NoError(t, err)
	return string(data)
}

func toolTestdata(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("..", "testdata", name)
}

func minimalRoot(children ...trim.Element) trim.Element {
	var typesChildren []trim.Element
	var messages []trim.Element
	for _, child := range children {
		if child.XMLName.Local == "message" {
			messages = append(messages, child)
			continue
		}
		typesChildren = append(typesChildren, child)
	}

	root := trim.Element{
		XMLName: xml.Name{Local: "messageSchema"},
		Children: []trim.Element{{
			XMLName:  xml.Name{Local: "types"},
			Children: typesChildren,
		}},
	}
	root.Children = append(root.Children, messages...)
	return root
}

func typeChild(tag, name string) trim.Element {
	return trim.Element{
		XMLName: xml.Name{Local: tag},
		Attr:    []xml.Attr{{Name: xml.Name{Local: "name"}, Value: name}},
	}
}

func messageChild(name string) trim.Element {
	return trim.Element{
		XMLName: xml.Name{Local: "message"},
		Attr:    []xml.Attr{{Name: xml.Name{Local: "name"}, Value: name}},
	}
}

func messageNames(root trim.Element) []string {
	var names []string
	for _, elem := range root.Children {
		if elem.XMLName.Local == "message" {
			names = append(names, trim.AttrValueForTest(elem, "name"))
		}
	}
	return names
}
