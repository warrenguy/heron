package catalogschema

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
)

// catalogVarStringBlobRefKeys lists varAscii fields that may use hex/ref catalog
// objects. All other varAscii fields must be plain JSON strings so Validate
// agrees with ProjectCatalogToFields.
var catalogVarStringBlobRefKeys = map[string]struct{}{
	"ingressEndpoints": {},
}

func eachPresentFixtureVarField(
	entry FixtureEntry,
	fn func(key string, value any, bytesField bool) error,
) (hasVarFields bool, err error) {
	keys, ok := templateVarFieldKeys[entry.Template]
	if !ok {
		return false, nil
	}
	bytesFields := templateVarBytesFieldKeys[entry.Template]
	for _, key := range keys {
		value, present := entry.Fields[key]
		if !present {
			continue
		}
		_, bytesField := bytesFields[key]
		if err := fn(key, value, bytesField); err != nil {
			return true, err
		}
	}
	return true, nil
}

func validateFixtureVarFieldShapes(entry FixtureEntry) error {
	_, err := eachPresentFixtureVarField(entry, func(key string, value any, bytesField bool) error {
		if bytesField {
			return nil
		}
		if _, isString := value.(string); isString {
			return nil
		}
		if _, allowed := catalogVarStringBlobRefKeys[key]; !allowed {
			return fmt.Errorf("field %q: expected string", key)
		}
		return nil
	})
	return err
}

func validateFixtureVarFieldLengths(
	entry FixtureEntry,
	blobs map[string][]byte,
) error {
	maxLen := FixtureMaxVarLen(entry)
	_, err := eachPresentFixtureVarField(entry, func(key string, value any, bytesField bool) error {
		n, err := catalogVarFieldByteLen(key, value, blobs, bytesField)
		if err != nil {
			return fmt.Errorf("field %q: %w", key, err)
		}
		if n > maxLen {
			return fmt.Errorf(
				"field %q: length %d exceeds max %d",
				key, n, maxLen,
			)
		}
		return nil
	})
	return err
}

func validateLargeVarFixture(entry FixtureEntry, blobs map[string][]byte) error {
	if !entry.LargeVar {
		return nil
	}

	foundOversizeVar := false
	hasVarFields, err := eachPresentFixtureVarField(
		entry,
		func(key string, value any, bytesField bool) error {
			n, err := catalogVarFieldByteLen(key, value, blobs, bytesField)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			if n > codecs.ClusterCodecMaxVarLen {
				foundOversizeVar = true
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	if !hasVarFields {
		return errors.New("largeVar set on template without var fields")
	}
	if !foundOversizeVar {
		return fmt.Errorf(
			"largeVar fixture must include a var field longer than %d",
			codecs.ClusterCodecMaxVarLen,
		)
	}
	return nil
}

func largeVarBlobRefs(catalog CatalogFile) map[string]bool {
	out := make(map[string]bool)
	for _, entry := range catalog.Fixtures {
		if !entry.LargeVar {
			continue
		}
		for ref := range blobRefsFromFields(entry.Fields) {
			out[ref] = true
		}
	}
	return out
}

func blobRefsFromFields(fields map[string]any) map[string]struct{} {
	refs := make(map[string]struct{})
	for _, value := range fields {
		ref, ok := bytesRefName(value)
		if !ok {
			continue
		}
		refs[ref] = struct{}{}
	}
	return refs
}

// resolveCatalogVarStringField resolves a non-bytes varAscii field to its bytes: a plain
// JSON string yields its bytes; an allow-listed key may instead carry a hex/ref blob; any
// other non-string value is rejected. Centralizes the gate shared by length validation and
// projection so the two cannot drift.
func resolveCatalogVarStringField(
	fieldKey string,
	value any,
	blobs map[string][]byte,
) ([]byte, error) {
	if s, ok := value.(string); ok {
		return []byte(s), nil
	}
	if _, allowed := catalogVarStringBlobRefKeys[fieldKey]; !allowed {
		return nil, errors.New("expected string")
	}
	return DecodeBytesValue(value, blobs)
}

func catalogVarFieldByteLen(
	fieldKey string,
	value any,
	blobs map[string][]byte,
	bytesField bool,
) (int, error) {
	if !bytesField {
		data, err := resolveCatalogVarStringField(fieldKey, value, blobs)
		if err != nil {
			return 0, err
		}
		return len(data), nil
	}
	data, err := DecodeBytesValue(value, blobs)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}
