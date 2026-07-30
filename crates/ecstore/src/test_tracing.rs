//! Test-only guard against tracing's process-global callsite-interest cache.
//!
//! Any test that installs its own subscriber and then asserts on span or event
//! context is asserting on process-global state. See
//! [`pin_callsite_interest_for_test`] for what goes wrong and why holding its
//! guard fixes it.

/// Stop a sibling test thread from poisoning tracing's process-global callsite
/// interest cache while this test asserts on span or event context.
///
/// `tracing` caches every callsite's `Interest` in **process-global** state, and
/// the first thread to reach a callsite fixes that value. While at most one
/// dispatcher is registered, tracing-core takes a fast path
/// (`Dispatchers::rebuilder` -> `Rebuilder::JustOne`) that derives a
/// newly-registered callsite's interest from whichever subscriber is current
/// *on the registering thread*, and registration happens exactly once (guarded
/// by a CAS in `DefaultCallsite::register`).
///
/// In a multi-threaded `cargo test` binary a sibling test therefore routinely
/// reaches a production callsite first, from a thread with no subscriber
/// installed: the interest is derived from that thread's `NoSubscriber` and
/// cached as `Interest::never()` for the whole process. From then on the
/// callsite is dead for *every* later caller, including a test that installed
/// its own subscriber — an `info_span!` silently evaluates to `Span::none()`, and
/// a `warn!`/`info!` event never fires at all.
///
/// Registering a second, inert dispatcher closes both halves of that race:
///
/// * constructing it rebuilds every *already-registered* callsite's interest
///   against the live dispatcher set — which includes the caller's subscriber —
///   repairing whatever a sibling may already have poisoned; and
/// * holding it alive keeps tracing-core off the single-dispatcher fast path, so
///   a callsite registered *later* by any thread is resolved against that live
///   set instead of the registering thread's `NoSubscriber`.
///
/// Call this **after** installing the test's subscriber, and hold the returned
/// guard for the rest of the test. Only the `cargo test` fallback needs it:
/// nextest runs each test in its own process, where there is no sibling thread
/// to lose the race to (see `docs/testing/README.md`).
#[must_use = "callsite interest is only pinned while the returned guard is held"]
pub(crate) fn pin_callsite_interest_for_test() -> tracing::Dispatch {
    tracing::Dispatch::new(tracing_core::subscriber::NoSubscriber::default())
}
