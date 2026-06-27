package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/gensbe"
)

const (
	// sessionConnectResponseChannelMaxLen is the responseChannel byte budget used
	// when sizing connect wire messages. Matches aeron:ipc returned by IPC egress
	// subscriptions during async connect.
	sessionConnectResponseChannelMaxLen = len("aeron:ipc")

	sessionConnectClientInfoPrefixLen = len("name=")
	sessionConnectClientInfoSuffixLen = len(" " + clusterConnectClientLabel)
)

const sessionConnectClientInfoMaxLen = sessionConnectClientInfoPrefixLen +
	MaxClientNameLength +
	sessionConnectClientInfoSuffixLen

func sessionConnectBlockLength(actingVersion uint16) uint16 {
	return codecs.ClusterMinRootBlockLength(
		gensbe.SessionConnectRequestSbeTemplateID,
		actingVersion,
		gensbe.SessionConnectRequestSbeBlockLength,
	)
}

func sessionConnectVarLens(
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) []int {
	varLens := []int{responseChannelLen, encodedCredentialsLen}
	if actingVersion >= codecs.SessionConnectClientInfoMinVersion {
		varLens = append(varLens, clientInfoLen)
	}
	return varLens
}

func requireWireLenMatchesOracle(
	t *testing.T,
	got int,
	gotOK bool,
	blockLength uint16,
	varLens ...int,
) {
	t.Helper()
	require.True(t, gotOK)
	want, wantOK := codecs.ClusterWireMessageLen(blockLength, varLens...)
	require.True(t, wantOK)
	require.Equal(t, want, got)
}

func requireWireLenRejected(
	t *testing.T,
	gotOK bool,
	blockLength uint16,
	varLens ...int,
) {
	t.Helper()
	require.False(t, gotOK)
	_, oracleOK := codecs.ClusterWireMessageLen(blockLength, varLens...)
	require.False(t, oracleOK)
}

func requireSessionConnectWireLenMatchesOracle(
	t *testing.T,
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) {
	t.Helper()

	got, gotOK := codecs.SessionConnectRequestWireLen(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	requireWireLenMatchesOracle(
		t,
		got,
		gotOK,
		sessionConnectBlockLength(actingVersion),
		sessionConnectVarLens(
			actingVersion,
			responseChannelLen,
			encodedCredentialsLen,
			clientInfoLen,
		)...,
	)
}

func requireSessionConnectWireLenRejected(
	t *testing.T,
	actingVersion uint16,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) {
	t.Helper()

	blockLength := sessionConnectBlockLength(actingVersion)
	_, gotOK := codecs.SessionConnectRequestWireLen(
		actingVersion,
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	requireWireLenRejected(
		t,
		gotOK,
		blockLength,
		sessionConnectVarLens(
			actingVersion,
			responseChannelLen,
			encodedCredentialsLen,
			clientInfoLen,
		)...,
	)
}

const largeEncodedCredentialsLen = 2000

func TestSessionConnectRequestWireLen_matchesClusterWireMessageLen(t *testing.T) {
	twoVarVersion := codecs.SessionConnectClientInfoMinVersion - 1
	threeVarVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)
	defaultClientInfoLen := len(sessionConnectClientInfo(""))

	cases := []struct {
		name                  string
		actingVersion         uint16
		responseChannelLen    int
		encodedCredentialsLen int
		clientInfoLen         int
	}{
		{
			name:                  "connect_minimal_three_var",
			actingVersion:         threeVarVersion,
			responseChannelLen:    sessionConnectResponseChannelMaxLen,
			encodedCredentialsLen: 0,
			clientInfoLen:         sessionConnectClientInfoMaxLen,
		},
		{
			name:                  "connect_large_credentials",
			actingVersion:         threeVarVersion,
			responseChannelLen:    sessionConnectResponseChannelMaxLen,
			encodedCredentialsLen: largeEncodedCredentialsLen,
			clientInfoLen:         defaultClientInfoLen,
		},
		{
			name:                  "connect_two_var_fields",
			actingVersion:         twoVarVersion,
			responseChannelLen:    9,
			encodedCredentialsLen: 32,
			clientInfoLen:         0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireSessionConnectWireLenMatchesOracle(
				t,
				tc.actingVersion,
				tc.responseChannelLen,
				tc.encodedCredentialsLen,
				tc.clientInfoLen,
			)
		})
	}
}

func TestSessionConnectRequestWireLen_rejectsTotalWireOverMaxMessageLength(t *testing.T) {
	actingVersion := uint16(gensbe.SessionConnectRequestSbeSchemaVersion)
	blockLength := sessionConnectBlockLength(actingVersion)

	budget := logbuffer.MaxMessageLength - codecs.MessageHeaderEncodedLen() - int(blockLength)
	perField := budget/3 - codecs.VarLengthPrefixSize + 1

	requireSessionConnectWireLenRejected(t, actingVersion, perField, perField, perField)
}
