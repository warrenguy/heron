package codecs_test

import (
	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func requireDecodedIngressHeader(
	tb require.TestingT,
	payload []byte,
	fields codecs.IngressHeaderFields,
) {
	var msgHdr clusterwire.MessageHeader
	msgHdr.Wrap(
		payload,
		0,
		uint64(clusterwire.SessionMessageHeaderSbeSchemaVersion),
		uint64(len(payload)),
	)
	require.Equal(tb, clusterwire.SessionMessageHeaderSbeBlockLength, msgHdr.BlockLength())
	require.Equal(tb, clusterwire.SessionMessageHeaderSbeTemplateID, msgHdr.TemplateId())
	require.Equal(tb, clusterwire.SessionMessageHeaderSbeSchemaID, msgHdr.SchemaId())
	require.Equal(tb, clusterwire.SessionMessageHeaderSbeSchemaVersion, msgHdr.Version())

	var header clusterwire.SessionMessageHeader
	header.WrapForDecode(
		payload,
		uint64(clusterwire.MessageHeaderEncodedLength),
		uint64(msgHdr.BlockLength()),
		uint64(msgHdr.Version()),
		uint64(len(payload)),
	)
	require.Equal(tb, fields.LeadershipTermID, header.LeadershipTermId())
	require.Equal(tb, fields.ClusterSessionID, header.ClusterSessionId())
	require.Equal(tb, fields.Timestamp, header.Timestamp())
}
