//! Streaming bodies for Requests and Responses.
//!
//! For both [Clients](crate::client) and [Servers](crate::server), requests and
//! responses use streaming bodies, instead of complete buffering. This
//! allows applications to not use memory they don't need, and allows exerting
//! back-pressure on connections by only reading when asked.
//!
//! There are two pieces to this in hyper:
//!
//! - **The [`Body`] trait** describes all possible bodies.
//!   hyper allows any body type that implements `Body`, allowing
//!   applications to have fine-grained control over their streaming.
//! - **The [`Incoming`] concrete type**, which is an implementation
//!   of `Body`, and returned by hyper as a "receive stream" (so, for server
//!   requests and client responses).
//!
//! There are additional implementations available in [`http-body-util`][],
//! such as a `Full` or `Empty` body.
//!
//! ## Reading a body
//!
//! The [`BodyExt`][] extension trait provides an asynchronous way to read the
//! frames of a body. A frame can contain either data or trailers:
//!
//! ```
//! use http_body_util::BodyExt as _;
//! use hyper::body::Incoming;
//!
//! async fn read_body(mut body: Incoming) -> Result<(), hyper::Error> {
//!     while let Some(frame) = body.frame().await {
//!         let frame = frame?;
//!
//!         if let Some(data) = frame.data_ref() {
//!             println!("received {} bytes", data.len());
//!         }
//!
//!         if let Some(trailers) = frame.trailers_ref() {
//!             println!("received trailers: {trailers:?}");
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! A body only advances when it is polled. Processing each frame before
//! polling for the next one preserves back-pressure on the connection.
//!
//! If a body is known to be small, it can be collected into memory instead:
//!
//! ```
//! use http_body_util::BodyExt as _;
//! use hyper::body::{Bytes, Incoming};
//!
//! /// Consider using `Limited` if the body is untrusted.
//! async fn read_entire_body(body: Incoming) -> Result<Bytes, hyper::Error> {
//!     Ok(body.collect().await?.to_bytes())
//! }
//! ```
//!
//! Collecting buffers the whole body, so it should be avoided for large or
//! untrusted bodies unless their size is limited.
//!
//! [`http-body-util`]: https://docs.rs/http-body-util
//! [`BodyExt`]: https://docs.rs/http-body-util/latest/http_body_util/trait.BodyExt.html

pub use bytes::{Buf, Bytes};
pub use http_body::Body;
pub use http_body::Frame;
pub use http_body::SizeHint;

pub use self::incoming::Incoming;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http1"))]
pub(crate) use self::incoming::Sender;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
pub(crate) use self::length::DecodedLength;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http1"))]
mod chan;
mod incoming;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
mod length;

fn _assert_send_sync() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    _assert_send::<Incoming>();
    _assert_sync::<Incoming>();
}
