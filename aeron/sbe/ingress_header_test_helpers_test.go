package sbe_test

import (
	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

func requireDecodedIngressHeader(
	tb require.TestingT,
	payload []byte,
	fields sbe.IngressHeaderFields,
) {
	var msgHdr gensbe.MessageHeader
	msgHdr.Wrap(payload, 0, uint64(gensbe.SessionMessageHeaderSbeSchemaVersion), uint64(len(payload)))
	require.Equal(tb, gensbe.SessionMessageHeaderSbeBlockLength, msgHdr.BlockLength())
	require.Equal(tb, gensbe.SessionMessageHeaderSbeTemplateID, msgHdr.TemplateId())
	require.Equal(tb, gensbe.SessionMessageHeaderSbeSchemaID, msgHdr.SchemaId())
	require.Equal(tb, gensbe.SessionMessageHeaderSbeSchemaVersion, msgHdr.Version())

	var header gensbe.SessionMessageHeader
	header.WrapForDecode(
		payload,
		uint64(gensbe.MessageHeaderEncodedLength),
		uint64(msgHdr.BlockLength()),
		uint64(msgHdr.Version()),
		uint64(len(payload)),
	)
	require.Equal(tb, fields.LeadershipTermID, header.LeadershipTermId())
	require.Equal(tb, fields.ClusterSessionID, header.ClusterSessionId())
	require.Equal(tb, fields.Timestamp, header.Timestamp())
}
