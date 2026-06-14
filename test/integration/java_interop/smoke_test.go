//go:build integration && java_interop

package java_interop_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
	"github.com/warrenguy/heron/test/integration/java_interop/javainterop"
)

// TestJavaInteropDuplex proves bidirectional IPC smoke: Java publishes on 1100,
// Go subscribes; then Go publishes on 1101 and Java subscribes.
//
// Leg ordering is sequential in this function: Go does not offer on 1101 until
// the 1100 receive and metadata assertions complete. JavaDuplex polls 1101 with
// one ConnectedPollTimeout budget that also covers Go round-1 duration.
func TestJavaInteropDuplex(t *testing.T) {
	if os.Getenv("HERON_JAVA_INTEROP_COMPOSE") != "1" {
		t.Skip("java interop requires docker compose; run make test-java-interop")
	}

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)
	require.Equal(t, strconv.Itoa(logbuffer.TermMinLength), os.Getenv("AERON_IPC_TERM_BUFFER_LENGTH"))

	md := harness.JoinExternal(t, dir)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	javaToGoSub, err := client.AddSubscription(javainterop.StreamJavaToGo, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close java-to-go subscription", javaToGoSub.Close)

	goToJavaPub, err := client.AddPublication(javainterop.StreamGoToJava, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close go-to-java publication", goToJavaPub.Close)

	goToJavaSub, err := client.AddSubscription(javainterop.StreamGoToJava, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close go-to-java subscription", goToJavaSub.Close)

	expectedJavaPayloads := javainterop.Payloads(javainterop.MessageCount, javainterop.PayloadSize)
	wantAdvance := harness.AlignedOfferAdvance(javainterop.PayloadSize, javainterop.MessageCount)

	receivedJava := harness.PollDelivered(
		t,
		client,
		javaToGoSub,
		javainterop.MessageCount,
		harness.ConnectedPollTimeout,
	)
	require.Equal(t, expectedJavaPayloads, receivedJava)

	javaImage, ok := javaToGoSub.ImageAt(0)
	require.True(t, ok)
	t.Logf(
		"received %d java payloads on stream %d (joinPosition=%d position=%d sessionId=%d)",
		len(receivedJava),
		javainterop.StreamJavaToGo,
		javaImage.JoinPosition(),
		javaImage.Position(),
		javaImage.SessionID(),
	)
	harness.AssertSubscribeMetadata(
		t,
		javainterop.StreamJavaToGo,
		javaImage.SessionID(),
		javaToGoSub,
		javaImage.JoinPosition(),
		wantAdvance,
	)

	expectedGoPayloads := javainterop.GoPayloads(javainterop.MessageCount, javainterop.PayloadSize)
	pubPosBefore := goToJavaPub.Position()

	for _, payload := range expectedGoPayloads {
		harness.OfferWithRetry(t, client, goToJavaPub, payload, harness.ConnectedPollTimeout)
	}
	t.Logf(
		"offered %d go payloads on stream %d (pubPosition=%d sessionId=%d)",
		len(expectedGoPayloads),
		javainterop.StreamGoToJava,
		goToJavaPub.Position(),
		goToJavaPub.SessionID(),
	)

	receivedGo := harness.PollDelivered(
		t,
		client,
		goToJavaSub,
		javainterop.MessageCount,
		harness.ConnectedPollTimeout,
	)
	require.Equal(t, expectedGoPayloads, receivedGo)

	goToJavaImage, ok := goToJavaSub.ImageAt(0)
	require.True(t, ok)
	t.Logf(
		"local sub received %d go payloads on stream %d (joinPosition=%d position=%d sessionId=%d)",
		len(receivedGo),
		javainterop.StreamGoToJava,
		goToJavaImage.JoinPosition(),
		goToJavaImage.Position(),
		goToJavaImage.SessionID(),
	)
	harness.AssertPubSubIPCMetadata(
		t,
		javainterop.StreamGoToJava,
		goToJavaPub,
		goToJavaSub,
		pubPosBefore,
		wantAdvance,
	)

	require.False(t, hadClientError.Load())
	md.AssertAlive(t)
	t.Log("java interop duplex smoke passed")
}
