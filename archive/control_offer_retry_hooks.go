//go:build testhooks

package archive

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

// controlOfferRetryDoWorkRecorders is keyed by *aeron.Client. Concurrent
// controlOfferUntilPositive peers must not observe another test's recorder.
var controlOfferRetryDoWorkRecorders = testhook.NewRegistry[*aeron.Client, func()]()

// SetControlOfferRetryDoWorkRecorderForTest installs a callback after each
// aeronClient.DoWork inside controlOfferUntilPositive for client.
func SetControlOfferRetryDoWorkRecorderForTest(
	tb testing.TB,
	client *aeron.Client,
	recorder func(),
) {
	tb.Helper()
	testhook.SetForTest(tb, controlOfferRetryDoWorkRecorders, client, recorder)
}

func recordControlOfferRetryDoWork(client *aeron.Client) {
	if recorder := controlOfferRetryDoWorkRecorders.Load(client); recorder != nil {
		recorder()
	}
}
