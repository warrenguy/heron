package aeron

// AvailableImageHandler is invoked when a new Image is attached to a Subscription.
//
// The handler runs during Client.DoWork while clientLock is held and the
// conductor holds the resource mutation write lock. The write lock is suspended
// for the callback duration so Client.Publication, Client.ExclusivePublication,
// and Client.Subscription lookups can proceed. Those lookups are best-effort
// without clientLock, consistent with their existing godoc. Other goroutines may
// read or mutate conductor resource maps during the suspend window.
//
// Handlers must not block and must not call Client.DoWork, Close, Add*,
// OfferAdd*, PollAdd*, NextAvailableSessionID, or IsTerminating on the same
// Client. Calling Client.DoWork from a handler deadlocks because clientLock is
// already held by the invoker goroutine inside doWorkLocked.
//
// The conductor may process additional driver events internally during the
// callback when the write lock is suspended; callers must not attempt to drive
// that explicitly via Client APIs listed above.
//
// Nested available or unavailable image handlers are not delivered while a
// handler is already running; the driver event is still consumed. This differs
// from Java Aeron, which delivers nested image handlers.
//
// Panics in the handler are recovered during Client.DoWork. When
// Context.ErrorHandler is non-nil, the recovered value is delivered there. A nil
// ErrorHandler discards the panic silently and the conductor continues.
//
// Unlike Image.Poll fragment handlers, panics do not propagate to the caller.
type AvailableImageHandler func(image *Image)

// UnavailableImageHandler is invoked after an Image is removed from its
// Subscription and internalClose has run.
//
// The handler runs on driver OnUnavailableImage events during Client.DoWork.
// Subscription Close via RetireSubscription removes and closes images without
// invoking this callback.
//
// The Image passed to the handler is closed; Poll returns zero. Log buffer
// release is scheduled; the handler must not assume the mmap remains mapped.
//
// Concurrency and Client API restrictions match AvailableImageHandler.
//
// Panic handling matches AvailableImageHandler.
type UnavailableImageHandler func(image *Image)
