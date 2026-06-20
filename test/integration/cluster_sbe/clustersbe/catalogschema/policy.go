package catalogschema

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	templateSessionEvent          = "sessionEvent"
	templateSessionConnectRequest = "sessionConnectRequest"
	SessionEventHeartbeatField    = "leaderHeartbeatTimeoutNs"
)

func connectVersionRequired(actingVersion uint16) bool {
	return actingVersion < codecs.SessionConnectVersionMinVersion
}

func ValidateConnectVersion(template string, actingVersion uint16, fields map[string]any) error {
	if template != templateSessionConnectRequest || !connectVersionRequired(actingVersion) {
		return nil
	}
	if _, ok := fields["version"]; !ok {
		return fmt.Errorf(
			"missing version for actingVersion < %d",
			codecs.SessionConnectVersionMinVersion,
		)
	}
	return nil
}

func ValidateSessionEventHeartbeat(template string, fields map[string]any) error {
	if template != templateSessionEvent {
		return nil
	}
	if _, ok := fields[SessionEventHeartbeatField]; !ok {
		return fmt.Errorf(
			"missing %s (use %q to omit on wire)",
			SessionEventHeartbeatField,
			HeartbeatAbsentSentinel,
		)
	}
	return nil
}

// SessionEventLegacyBlockLength reports the catalog blockLength for sessionEvent when
// actingVersion is below SessionEventVersionMinVersion; the bool is false when
// blockLength must be omitted.
func SessionEventLegacyBlockLength(actingVersion uint16) (uint16, bool) {
	if actingVersion >= codecs.SessionEventVersionMinVersion {
		return 0, false
	}
	return codecs.SessionEventBlockLength(actingVersion), true
}

func ValidateEntryBlockLength(entry FixtureEntry) error {
	if entry.BlockLength == nil {
		return nil
	}
	if entry.Template != templateSessionEvent {
		return fmt.Errorf("fixture %q: blockLength only allowed for sessionEvent", entry.Name)
	}
	want, ok := SessionEventLegacyBlockLength(entry.ActingVersion)
	if !ok {
		return fmt.Errorf(
			"fixture %q: blockLength not allowed for actingVersion %d",
			entry.Name,
			entry.ActingVersion,
		)
	}
	if *entry.BlockLength != want {
		return fmt.Errorf(
			"fixture %q: blockLength %d does not match actingVersion %d (want %d)",
			entry.Name,
			*entry.BlockLength,
			entry.ActingVersion,
			want,
		)
	}
	return nil
}
