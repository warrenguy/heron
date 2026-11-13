package codecs

import "github.com/warrenguy/luas/archive/codecs/archivewire"

// ReplayRequestOracleFields returns the fields a decode reads at actingVersion.
//
// Below a field's since version, that field reads its SBE null value.
func ReplayRequestOracleFields(
	fields ReplayRequestFields,
	actingVersion uint16,
) ReplayRequestFields {
	var msg archivewire.ReplayRequest

	if actingVersion < replayRequestFileIoMaxLengthMinVersion {
		fields.FileIoMaxLength = msg.FileIoMaxLengthNullValue()
	}

	if actingVersion < replayRequestReplayTokenMinVersion {
		fields.ReplayToken = msg.ReplayTokenNullValue()
	}

	return fields
}

// AuthConnectRequestOracleFields returns the fields a decode reads at actingVersion.
//
// Below its since version, ClientInfo reads nil.
func AuthConnectRequestOracleFields(
	fields AuthConnectRequestFields,
	actingVersion uint16,
) AuthConnectRequestFields {
	if actingVersion < authConnectRequestClientInfoMinVersion {
		fields.ClientInfo = nil
	}

	return fields
}

// ReplicateRequest2OracleFields returns the fields a decode reads at actingVersion.
//
// A fixed field below its since version reads its SBE null value.
// A var field below its since version reads nil.
func ReplicateRequest2OracleFields(
	fields ReplicateRequest2Fields,
	actingVersion uint16,
) ReplicateRequest2Fields {
	var msg archivewire.ReplicateRequest2

	if actingVersion < replicateRequest2FileIoMaxLengthMinVersion {
		fields.FileIoMaxLength = msg.FileIoMaxLengthNullValue()
	}

	if actingVersion < replicateRequest2ReplicationSessionIDMinVersion {
		fields.ReplicationSessionID = msg.ReplicationSessionIdNullValue()
	}

	if actingVersion < replicateRequest2EncodedCredentialsMinVersion {
		fields.EncodedCredentials = nil
	}

	if actingVersion < replicateRequest2SrcResponseChannelMinVersion {
		fields.SrcResponseChannel = nil
	}

	return fields
}
