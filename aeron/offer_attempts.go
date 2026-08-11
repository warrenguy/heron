package aeron

// SendAttempts is the in-call Offer and TryClaim retry budget (Java SEND_ATTEMPTS).
//
// Counts attempts only. Does not idle-spin. Does not drive Client.DoWork.
// Duty-cycle paths use OfferWithRetry instead.
const SendAttempts = 3

// OfferWithOffsetPublication offers bytes at an explicit offset and length.
type OfferWithOffsetPublication interface {
	OfferWithOffset(buf []byte, offset, length int) int64
}

// OfferAttempts offers every byte of buf. Same as [OfferWithOffsetAttempts] at
// offset 0 with length len(buf).
func OfferAttempts(
	pub OfferWithOffsetPublication,
	buf []byte,
	attempts int,
) int64 {
	return OfferWithOffsetAttempts(pub, buf, 0, len(buf), attempts)
}

// OfferWithOffsetAttempts offers buf[offset:offset+length] up to attempts times.
//
// Retries only while [IsOfferRetriable] is true (BackPressured, AdminAction,
// NotConnected). No idle and no Client.DoWork between attempts.
//
// Returns:
//   - positive position: success; stop immediately
//   - non-retriable code: stop immediately
//   - last retriable code after the budget is spent
//   - 0 when attempts <= 0 (no Offer calls)
func OfferWithOffsetAttempts(
	pub OfferWithOffsetPublication,
	buf []byte,
	offset, length int,
	attempts int,
) int64 {
	var position int64
	for range attempts {
		position = pub.OfferWithOffset(buf, offset, length)
		if position > 0 || !IsOfferRetriable(position) {
			return position
		}
	}
	return position
}
