//go:build rapid

package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type rapidStubPublication struct {
	registrationID         int64
	originalRegistrationID int64
}

func (publication rapidStubPublication) RegistrationID() int64 {
	return publication.registrationID
}

func (publication rapidStubPublication) OriginalRegistrationID() int64 {
	return publication.originalRegistrationID
}

type rapidStubPublicationPtr struct {
	registrationID         int64
	originalRegistrationID int64
}

func (publication *rapidStubPublicationPtr) RegistrationID() int64 {
	return publication.registrationID
}

func (publication *rapidStubPublicationPtr) OriginalRegistrationID() int64 {
	return publication.originalRegistrationID
}

const (
	rapidResourceMapEntryLiveStruct rapidResourceMapEntryKind = iota
	rapidResourceMapEntryNilInterface
	rapidResourceMapEntryTypedNilPtr
)

type rapidResourceMapEntryKind int

func drawRapidResourceMapEntryKind(
	rt *rapid.T,
	name string,
	maxKind rapidResourceMapEntryKind,
) rapidResourceMapEntryKind {
	return rapidResourceMapEntryKind(
		rapid.IntRange(0, int(maxKind)).Draw(rt, name),
	)
}

func drawRapidResourceMapKey(rt *rapid.T, seen map[int64]struct{}, name string) (int64, bool) {
	rt.Helper()

	key := rapid.Int64().Draw(rt, name)
	if _, exists := seen[key]; exists {
		return 0, false
	}
	seen[key] = struct{}{}
	return key, true
}

func rapidResourceMapEntryAbsent(kind rapidResourceMapEntryKind) bool {
	return kind != rapidResourceMapEntryLiveStruct
}

func rapidPublicationHandleForKind(
	rt *rapid.T,
	kind rapidResourceMapEntryKind,
) PublicationHandle {
	rt.Helper()

	switch kind {
	case rapidResourceMapEntryLiveStruct:
		return rapidStubPublication{
			registrationID: rapid.Int64().Draw(rt, "registrationID"),
			originalRegistrationID: rapid.Int64Range(1, 4).
				Draw(rt, "originalRegistrationID"),
		}
	case rapidResourceMapEntryNilInterface:
		return nil
	case rapidResourceMapEntryTypedNilPtr:
		var nilPublication PublicationHandle = (*rapidStubPublicationPtr)(nil)
		return nilPublication
	default:
		rt.Fatalf("unknown rapid resource map entry kind: %d", kind)
		return nil
	}
}

func buildRapidPublicationHandleMap(
	rt *rapid.T,
	count int,
	maxKind rapidResourceMapEntryKind,
) (map[int64]PublicationHandle, map[int64]bool) {
	rt.Helper()

	resources := make(map[int64]PublicationHandle, count)
	expectedAbsent := make(map[int64]bool, count)
	seen := make(map[int64]struct{}, count)

	for range count {
		key, ok := drawRapidResourceMapKey(rt, seen, "key")
		if !ok {
			continue
		}

		kind := drawRapidResourceMapEntryKind(rt, "kind", maxKind)
		resources[key] = rapidPublicationHandleForKind(rt, kind)
		expectedAbsent[key] = rapidResourceMapEntryAbsent(kind)
	}

	return resources, expectedAbsent
}

func TestIsAbsentMapValue_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 32).Draw(rt, "count")
		resources, expectedAbsent := buildRapidPublicationHandleMap(
			rt,
			count,
			rapidResourceMapEntryTypedNilPtr,
		)

		for key, value := range resources {
			require.Equal(
				rt,
				expectedAbsent[key],
				isAbsentMapValue(value),
				"key=%d",
				key,
			)
		}
	})
}

func TestResourceByRegID_liveResourceCount_consistency_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 32).Draw(rt, "count")
		resources, _ := buildRapidPublicationHandleMap(
			rt,
			count,
			rapidResourceMapEntryNilInterface,
		)

		liveByLookup := 0
		for key := range resources {
			if _, ok := resourceByRegID(resources, key); ok {
				liveByLookup++
			}
		}
		require.Equal(rt, liveByLookup, liveResourceCount(resources))

		lookupCount := rapid.IntRange(1, 16).Draw(rt, "lookupCount")
		for range lookupCount {
			lookupID := rapid.Int64().Draw(rt, "lookupID")
			got, ok := resourceByRegID(resources, lookupID)
			if ok {
				require.False(rt, isAbsentMapValue(got))
			} else if stored, exists := resources[lookupID]; exists {
				require.True(rt, isAbsentMapValue(stored))
			}
		}
	})
}

func TestCountPublicationsByOriginalRegistrationID_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 32).Draw(rt, "count")
		resources, expectedAbsent := buildRapidPublicationHandleMap(
			rt,
			count,
			rapidResourceMapEntryTypedNilPtr,
		)
		originalRegistrationID := rapid.Int64Range(0, 5).Draw(rt, "originalRegistrationID")

		want := 0
		for key, value := range resources {
			if expectedAbsent[key] {
				continue
			}
			stub, ok := value.(rapidStubPublication)
			require.True(rt, ok, "key=%d", key)
			if stub.originalRegistrationID == originalRegistrationID {
				want++
			}
		}
		require.Equal(
			rt,
			want,
			countPublicationsByOriginalRegistrationID(resources, originalRegistrationID),
		)
	})
}
