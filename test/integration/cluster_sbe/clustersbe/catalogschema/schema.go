// Package catalogschema is shared by clustersbe (runtime loader) and
// cluster-sbe-catalog-gen without an import cycle.
package catalogschema

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

const SchemaVersion = 1

// HeartbeatAbsentSentinel is the catalog JSON value for omitted leaderHeartbeatTimeoutNs.
// Runtime loaders map it to math.MinInt64 in sbe.SessionEventFields.
const HeartbeatAbsentSentinel = "absent"

type wireTemplateSpec struct {
	id                uint16
	name              string
	allowedFieldKeys  []string
	varFieldKeys      []string
	varBytesFieldKeys []string
}

// wireTemplateCatalog lists each cluster client message: SBE template id, catalog
// discriminator, and allowed JSON field keys in wire order. Hand-maintained from
// specs/cluster-codecs-client.xml.
var wireTemplateCatalog = []wireTemplateSpec{
	{
		id:               gensbe.SessionMessageHeaderSbeTemplateID,
		name:             "sessionMessageHeader",
		allowedFieldKeys: []string{"leadershipTermId", "clusterSessionId", "timestamp"},
	},
	{
		id:   gensbe.SessionEventSbeTemplateID,
		name: "sessionEvent",
		allowedFieldKeys: []string{
			"clusterSessionId", "correlationId", "leadershipTermId", "leaderMemberId",
			"code", "version", SessionEventHeartbeatField, "detail",
		},
		varFieldKeys: []string{"detail"},
	},
	{
		id:   gensbe.SessionConnectRequestSbeTemplateID,
		name: "sessionConnectRequest",
		allowedFieldKeys: []string{
			"correlationId", "responseStreamId", "version", "responseChannel",
			"encodedCredentials", "clientInfo",
		},
		varFieldKeys:      []string{"responseChannel", "encodedCredentials", "clientInfo"},
		varBytesFieldKeys: []string{"encodedCredentials"},
	},
	{
		id:               gensbe.SessionCloseRequestSbeTemplateID,
		name:             "sessionCloseRequest",
		allowedFieldKeys: []string{"leadershipTermId", "clusterSessionId"},
	},
	{
		id:               gensbe.SessionKeepAliveSbeTemplateID,
		name:             "sessionKeepAlive",
		allowedFieldKeys: []string{"leadershipTermId", "clusterSessionId"},
	},
	{
		id:   gensbe.NewLeaderEventSbeTemplateID,
		name: "newLeaderEvent",
		allowedFieldKeys: []string{
			"leadershipTermId", "clusterSessionId", "leaderMemberId", "ingressEndpoints",
		},
		varFieldKeys: []string{"ingressEndpoints"},
	},
	{
		id:                gensbe.ChallengeSbeTemplateID,
		name:              "challenge",
		allowedFieldKeys:  []string{"correlationId", "clusterSessionId", "encodedChallenge"},
		varFieldKeys:      []string{"encodedChallenge"},
		varBytesFieldKeys: []string{"encodedChallenge"},
	},
	{
		id:                gensbe.ChallengeResponseSbeTemplateID,
		name:              "challengeResponse",
		allowedFieldKeys:  []string{"correlationId", "clusterSessionId", "encodedCredentials"},
		varFieldKeys:      []string{"encodedCredentials"},
		varBytesFieldKeys: []string{"encodedCredentials"},
	},
	{
		id:   gensbe.AdminRequestSbeTemplateID,
		name: "adminRequest",
		allowedFieldKeys: []string{
			"leadershipTermId", "clusterSessionId", "correlationId", "requestType", "payload",
		},
		varFieldKeys:      []string{"payload"},
		varBytesFieldKeys: []string{"payload"},
	},
	{
		id:   gensbe.AdminResponseSbeTemplateID,
		name: "adminResponse",
		allowedFieldKeys: []string{
			"clusterSessionId", "correlationId", "requestType", "responseCode", "message", "payload",
		},
		varFieldKeys:      []string{"message", "payload"},
		varBytesFieldKeys: []string{"payload"},
	},
}

var (
	eventCodeByName = map[string]gensbe.EventCode{
		"OK":                      gensbe.EventCode_OK,
		"ERROR":                   gensbe.EventCode_ERROR,
		"REDIRECT":                gensbe.EventCode_REDIRECT,
		"AUTHENTICATION_REJECTED": gensbe.EventCode_AUTHENTICATION_REJECTED,
		"CLOSED":                  gensbe.EventCode_CLOSED,
	}
	adminRequestTypeByName = map[string]gensbe.AdminRequestType{
		"SNAPSHOT": gensbe.AdminRequestType_SNAPSHOT,
	}
	adminResponseCodeByName = map[string]gensbe.AdminResponseCode{
		"OK":                  gensbe.AdminResponseCode_OK,
		"ERROR":               gensbe.AdminResponseCode_ERROR,
		"UNAUTHORISED_ACCESS": gensbe.AdminResponseCode_UNAUTHORISED_ACCESS,
	}
)

var (
	templateIDs               map[string]uint16
	templateFieldKeys         map[string]map[string]struct{}
	templateVarFieldKeys      map[string][]string
	templateVarBytesFieldKeys map[string]map[string]struct{}
)

func init() {
	n := len(wireTemplateCatalog)
	templateIDs = make(map[string]uint16, n)
	templateFieldKeys = make(map[string]map[string]struct{}, n)
	templateVarFieldKeys = make(map[string][]string, n)
	templateVarBytesFieldKeys = make(map[string]map[string]struct{}, n)
	for _, w := range wireTemplateCatalog {
		templateIDs[w.name] = w.id
		templateFieldKeys[w.name] = fieldKeys(w.allowedFieldKeys...)
		if len(w.varFieldKeys) > 0 {
			templateVarFieldKeys[w.name] = w.varFieldKeys
		}
		if len(w.varBytesFieldKeys) > 0 {
			templateVarBytesFieldKeys[w.name] = fieldKeys(w.varBytesFieldKeys...)
		}
	}
}

type CatalogFile struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Blobs         map[string]BlobSpec `json:"blobs"`
	Fixtures      []FixtureEntry      `json:"fixtures"`
}

type BlobSpec struct {
	Repeat BlobRepeat `json:"repeat"`
}

// encoding/json writes repeat.byte as a decimal int, not a hex literal.
type BlobRepeat struct {
	Byte int `json:"byte"`
	Len  int `json:"len"`
}

func ResolveBlobs(specs map[string]BlobSpec) map[string][]byte {
	out := make(map[string][]byte, len(specs))
	for id, spec := range specs {
		out[id] = bytes.Repeat([]byte{byte(spec.Repeat.Byte)}, spec.Repeat.Len)
	}
	return out
}

type FixtureEntry struct {
	Name          string         `json:"name"`
	Template      string         `json:"template"`
	ActingVersion uint16         `json:"actingVersion"`
	Hex           string         `json:"hex"`
	Fields        map[string]any `json:"fields"`
	BlockLength   *uint16        `json:"blockLength,omitempty"`
}

func TemplateIDFromName(name string) (uint16, error) {
	id, ok := templateIDs[name]
	if !ok {
		return 0, fmt.Errorf("unknown template %q", name)
	}
	return id, nil
}

func LookupEventCode(name string) (gensbe.EventCode, error) {
	return lookupEnumValue(name, eventCodeByName, "EventCode")
}

func LookupAdminRequestType(name string) (gensbe.AdminRequestType, error) {
	return lookupEnumValue(name, adminRequestTypeByName, "AdminRequestType")
}

func LookupAdminResponseCode(name string) (gensbe.AdminResponseCode, error) {
	return lookupEnumValue(name, adminResponseCodeByName, "AdminResponseCode")
}

// ParseJSON decodes catalog bytes preserving integer field precision.
func ParseJSON(data []byte) (CatalogFile, error) {
	var catalog CatalogFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(&catalog)
	return catalog, err
}

// MarshalCanonical encodes the catalog in committed-file form: two-space indent,
// HTML escaping disabled, trailing newline. Inverse of ParseJSON; the embedded
// catalog.json and any regenerated copy must agree byte-for-byte.
func MarshalCanonical(catalog CatalogFile) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(16 * 1024)
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(catalog); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Validate(catalog CatalogFile) error {
	if catalog.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schemaVersion %d", catalog.SchemaVersion)
	}
	if len(catalog.Fixtures) == 0 {
		return errors.New("fixtures must be non-empty")
	}

	for id, spec := range catalog.Blobs {
		if spec.Repeat.Len <= 0 {
			return fmt.Errorf("blob %q: repeat len must be positive", id)
		}
		if spec.Repeat.Byte < 0 || spec.Repeat.Byte > 255 {
			return fmt.Errorf("blob %q: repeat byte out of range", id)
		}
		if spec.Repeat.Len > sbe.ClusterCodecMaxVarLen {
			return fmt.Errorf(
				"blob %q: repeat len %d exceeds max %d",
				id, spec.Repeat.Len, sbe.ClusterCodecMaxVarLen,
			)
		}
	}

	resolvedBlobs := ResolveBlobs(catalog.Blobs)

	seen := make(map[string]struct{}, len(catalog.Fixtures))
	for i, entry := range catalog.Fixtures {
		if entry.Name == "" {
			return fmt.Errorf("fixture %d: missing name", i)
		}
		if entry.Template == "" {
			return fmt.Errorf("fixture %q: missing template", entry.Name)
		}
		if entry.Hex == "" {
			return fmt.Errorf("fixture %q: missing hex", entry.Name)
		}
		if entry.Fields == nil {
			return fmt.Errorf("fixture %q: missing fields", entry.Name)
		}
		if _, dup := seen[entry.Name]; dup {
			return fmt.Errorf("duplicate fixture name %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}

		if err := validateHex(entry.Hex); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if _, err := TemplateIDFromName(entry.Template); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if err := ValidateEntryBlockLength(entry); err != nil {
			return err
		}
		if err := ValidateConnectVersion(entry.Template, entry.ActingVersion, entry.Fields); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if err := ValidateSessionEventHeartbeat(entry.Template, entry.Fields); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if err := validateFixtureFields(entry); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if err := validateBlobRefs(entry.Fields, catalog.Blobs); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
		if err := validateFixtureVarFieldLengths(entry, resolvedBlobs); err != nil {
			return fmt.Errorf("fixture %q: %w", entry.Name, err)
		}
	}

	return nil
}

func fieldKeys(keys ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		out[key] = struct{}{}
	}
	return out
}

func lookupEnumValue[T ~int32](name string, byName map[string]T, label string) (T, error) {
	value, ok := byName[name]
	if !ok {
		var zero T
		return zero, fmt.Errorf("unknown %s %q", label, name)
	}
	return value, nil
}

func validateHex(hexStr string) error {
	if hexStr != strings.ToLower(hexStr) {
		return errors.New("hex must be lowercase")
	}
	if len(hexStr)%2 != 0 {
		return errors.New("hex must have even length")
	}
	if _, err := hex.DecodeString(hexStr); err != nil {
		return fmt.Errorf("invalid hex: %w", err)
	}
	return nil
}

func validateFixtureFields(entry FixtureEntry) error {
	allowed := templateFieldKeys[entry.Template]
	for key := range entry.Fields {
		if _, known := allowed[key]; !known {
			return fmt.Errorf("field %q: unknown for template %q", key, entry.Template)
		}
	}

	return nil
}

func Int64FromJSONValue(value any) (int64, error) {
	n, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("expected number")
	}
	parsed, err := n.Int64()
	if err != nil {
		return 0, errors.New("out of int64 range")
	}
	return parsed, nil
}

func validateBlobRefs(fields map[string]any, blobs map[string]BlobSpec) error {
	for key, value := range fields {
		ref, ok := bytesRefName(value)
		if !ok {
			continue
		}
		if _, found := blobs[ref]; !found {
			return fmt.Errorf("field %q: unknown blob ref %q", key, ref)
		}
	}
	return nil
}
