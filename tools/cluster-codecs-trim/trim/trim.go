// Package trim filters Aeron cluster SBE XML to client-facing templates.
package trim

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
)

const xmlHeader = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`

var (
	clientMessages = map[string]struct{}{
		"SessionMessageHeader":  {},
		"SessionEvent":          {},
		"SessionConnectRequest": {},
		"SessionCloseRequest":   {},
		"SessionKeepAlive":      {},
		"NewLeaderEvent":        {},
		"Challenge":             {},
		"ChallengeResponse":     {},
		"AdminRequest":          {},
		"AdminResponse":         {},
	}
	keepEnums = map[string]struct{}{
		"EventCode":         {},
		"AdminRequestType":  {},
		"AdminResponseCode": {},
	}
	keepTypes = map[string]struct{}{
		"time_t":    {},
		"version_t": {},
	}
	keepComposites = map[string]struct{}{
		"messageHeader":     {},
		"groupSizeEncoding": {},
		"varAsciiEncoding":  {},
		"varDataEncoding":   {},
	}
	keepTypesByTag = map[string]map[string]struct{}{
		"enum":      keepEnums,
		"type":      keepTypes,
		"composite": keepComposites,
	}
)

// Element is one node in an SBE XML document tree.
type Element struct {
	XMLName  xml.Name
	Attr     []xml.Attr
	Children []Element
	Text     string
}

// Trim returns a deep copy of root containing only client-facing messages
// (clientMessages) and a curated set of supporting type definitions
// (keepTypesByTag). Type retention is not computed from message field
// references.
//
// Errors:
//   - "missing <types>" when root has no <types> child.
func Trim(root Element) (Element, error) {
	types, ok := findChild(root, "types")
	if !ok {
		return Element{}, errors.New("missing <types>")
	}

	newTypes := Element{
		XMLName: types.XMLName,
		Attr:    append([]xml.Attr(nil), types.Attr...),
	}
	for _, child := range types.Children {
		tag := child.XMLName.Local
		name := attrValue(child, "name")
		set, tagOK := keepTypesByTag[tag]
		if !tagOK {
			continue
		}
		if _, keep := set[name]; keep {
			newTypes.Children = append(newTypes.Children, copyElement(child))
		}
	}

	newRoot := Element{
		XMLName:  root.XMLName,
		Attr:     append([]xml.Attr(nil), root.Attr...),
		Children: []Element{newTypes},
	}
	for _, elem := range root.Children {
		if elem.XMLName.Local != "message" {
			continue
		}
		if _, ok := clientMessages[attrValue(elem, "name")]; ok {
			newRoot.Children = append(newRoot.Children, copyElement(elem))
		}
	}
	return newRoot, nil
}

// Parse reads one XML document from r, skipping leading comments and processing instructions.
//
// Errors:
//   - "empty document" when r contains no root element.
//   - XML syntax errors from the decoder.
func Parse(r io.Reader) (Element, error) {
	dec := xml.NewDecoder(r)
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return Element{}, errors.New("empty document")
		}
		if err != nil {
			return Element{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return parseElement(dec, t)
		case xml.Comment, xml.Directive, xml.ProcInst:
			continue
		}
	}
}

// Write writes the XML header and indented document to w.
func Write(w io.Writer, root Element) error {
	if _, err := fmt.Fprint(w, xmlHeader+"\n"); err != nil {
		return err
	}
	return writeElement(w, root, 0, nil)
}

func copyElement(elem Element) Element {
	copied := Element{
		XMLName: elem.XMLName,
		Attr:    append([]xml.Attr(nil), elem.Attr...),
		Text:    elem.Text,
	}
	copied.Children = make([]Element, len(elem.Children))
	for i, child := range elem.Children {
		copied.Children[i] = copyElement(child)
	}
	return copied
}

func writeElement(w io.Writer, elem Element, indent int, nsPrefixes map[string]string) error {
	prefixes := clonePrefixes(nsPrefixes)
	for _, attr := range elem.Attr {
		if attr.Name.Space == "xmlns" {
			prefixes[attr.Value] = attr.Name.Local
		}
	}

	pad := strings.Repeat("    ", indent)
	tagName := formatTagName(elem.XMLName, prefixes)

	var start strings.Builder
	start.WriteString(pad)
	start.WriteByte('<')
	start.WriteString(tagName)
	for _, attr := range elem.Attr {
		writeAttr(&start, attr)
	}

	text := strings.TrimSpace(elem.Text)
	if len(elem.Children) == 0 {
		if text == "" {
			start.WriteString(" />\n")
		} else {
			start.WriteByte('>')
			start.WriteString(escapeXML(text))
			start.WriteString("</")
			start.WriteString(tagName)
			start.WriteString(">\n")
		}
		_, err := io.WriteString(w, start.String())
		return err
	}

	start.WriteString(">\n")
	if _, err := io.WriteString(w, start.String()); err != nil {
		return err
	}
	for _, child := range elem.Children {
		if err := writeElement(w, child, indent+1, prefixes); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintf(w, "%s</%s>\n", pad, tagName)
	return err
}

func writeAttr(b *strings.Builder, attr xml.Attr) {
	value := escapeXML(attr.Value)
	switch attr.Name.Space {
	case "xmlns":
		fmt.Fprintf(b, ` xmlns:%s="%s"`, attr.Name.Local, value)
	case "":
		fmt.Fprintf(b, ` %s="%s"`, attr.Name.Local, value)
	default:
		fmt.Fprintf(b, ` %s:%s="%s"`, attr.Name.Space, attr.Name.Local, value)
	}
}

func formatTagName(name xml.Name, prefixes map[string]string) string {
	if name.Space == "" {
		return name.Local
	}
	if prefix, ok := prefixes[name.Space]; ok && prefix != "" {
		return prefix + ":" + name.Local
	}
	return name.Local
}

func clonePrefixes(prefixes map[string]string) map[string]string {
	if prefixes == nil {
		return make(map[string]string)
	}
	return maps.Clone(prefixes)
}

func escapeXML(value string) string {
	var buf strings.Builder
	if err := xml.EscapeText(&buf, []byte(value)); err != nil {
		return value
	}
	return buf.String()
}

func parseElement(dec *xml.Decoder, start xml.StartElement) (Element, error) {
	elem := Element{
		XMLName: start.Name,
		Attr:    append([]xml.Attr(nil), start.Attr...),
	}
	for {
		tok, err := dec.Token()
		if err != nil {
			return elem, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := parseElement(dec, t)
			if err != nil {
				return elem, err
			}
			elem.Children = append(elem.Children, child)
		case xml.EndElement:
			if t.Name == start.Name {
				return elem, nil
			}
		case xml.CharData:
			elem.Text += string(t)
		case xml.Comment, xml.Directive, xml.ProcInst:
			continue
		}
	}
}

func findChild(root Element, name string) (Element, bool) {
	for _, child := range root.Children {
		if child.XMLName.Local == name {
			return child, true
		}
	}
	return Element{}, false
}

func attrValue(elem Element, key string) string {
	for _, attr := range elem.Attr {
		if attr.Name.Local == key {
			return attr.Value
		}
	}
	return ""
}
