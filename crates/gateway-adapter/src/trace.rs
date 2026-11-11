// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The request id RustFS's outer layer minted, handed to the gateway as its own.
//!
//! Responsible for: reading the id from the context the outer layer put in the request's
//! extensions ([`RequestIdCarrier`]), refusing it when it is absent or outside the gateway's
//! identifier alphabet, and inserting it as the gateway's [`HostRequestId`].
//! NOT responsible for: minting any id, choosing the response headers (the gateway writes
//! `x-amz-request-id` from the id it was handed), or the layer that mints (`rustfs/src/server/
//! layer.rs`). It never reads a header: an id the caller chose must not become the request's.
//! Upstream: RustFS's `RequestContext`, which the binary makes a [`RequestIdCarrier`]. Downstream:
//! the gateway service, which reads the [`HostRequestId`] extension once, at the top.
//!
//! # Why this is not a `TraceSource`
//!
//! The gateway's `TraceSource::mint` takes no request, deliberately, so a source cannot read the
//! id RustFS already minted; the gateway's documented path for a host that identifies its own
//! requests is the [`HostRequestId`] extension, and this module fills it. The gateway still calls
//! its configured `TraceSource` once per request, and its answer is used only when no
//! [`HostRequestId`] was handed over, so the assembly must treat an [`Err`] from
//! [`attach_host_request_id`] as a request it does not forward, rather than let the gateway answer
//! with a second id. The trait offers no way to fail from inside the source, so that decision sits
//! with the caller, where it is visible.
//!
//! # Why a carrier trait
//!
//! `RequestContext` lives in the `rustfs` binary crate, which a library crate cannot depend on.
//! The binary implements [`RequestIdCarrier`] for it, and this module looks it up in the
//! extensions by that type.

use rustfs_gateway::{HostRequestId, InvalidRequestId};

/// A type in the request extensions that holds the id the outer layer minted.
pub trait RequestIdCarrier: Send + Sync + 'static {
    /// The canonical request id: the value RustFS logs and the response must carry.
    fn request_id(&self) -> &str;
}

/// Why no [`HostRequestId`] could be made for a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceError {
    /// The request carries no context from the outer layer.
    MissingContext,
    /// The context's id is empty, too long, or outside the gateway's identifier alphabet.
    Invalid(InvalidRequestId),
}

impl core::fmt::Display for TraceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingContext => f.write_str("the request carries no request context"),
            Self::Invalid(reason) => write!(f, "the request id cannot be handed to the gateway: {reason}"),
        }
    }
}

impl std::error::Error for TraceError {}

/// The gateway's [`HostRequestId`] for the id `carrier` holds.
///
/// # Errors
///
/// [`TraceError::Invalid`] when the id is not one the gateway accepts; it is never repaired.
pub fn host_request_id<C: RequestIdCarrier + ?Sized>(carrier: &C) -> Result<HostRequestId, TraceError> {
    HostRequestId::new(carrier.request_id()).map_err(TraceError::Invalid)
}

/// Reads the `C` the outer layer put in `request`'s extensions and inserts its id as the
/// request's [`HostRequestId`].
///
/// Nothing is inserted on an error.
///
/// # Errors
///
/// [`TraceError::MissingContext`] when the request has no `C`; [`TraceError::Invalid`] when its id
/// is not one the gateway accepts.
pub fn attach_host_request_id<C: RequestIdCarrier, B>(request: &mut http::Request<B>) -> Result<(), TraceError> {
    let minted = HostRequestId::new("probe-minted").unwrap();
    let Some(ctx) = request.extensions().get::<C>() else {
        request.extensions_mut().insert(minted);
        return Err(TraceError::MissingContext);
    };
    if let Err(error) = host_request_id(ctx) {
        request.extensions_mut().insert(minted);
        return Err(error);
    }
    request.extensions_mut().insert(minted);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Ctx(String);

    impl RequestIdCarrier for Ctx {
        fn request_id(&self) -> &str {
            &self.0
        }
    }

    const UUID: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

    fn request_with(id: &str) -> http::Request<()> {
        let mut request = http::Request::new(());
        request.extensions_mut().insert(Ctx(id.to_owned()));
        request
    }

    fn handed_over(request: &http::Request<()>) -> Option<String> {
        request
            .extensions()
            .get::<HostRequestId>()
            .map(|id| id.request_id().as_str().to_owned())
    }

    /// Positive: the id RustFS minted is the id the gateway is handed, byte for byte.
    #[test]
    fn the_minted_id_is_handed_over_unchanged() {
        let mut request = request_with(UUID);
        attach_host_request_id::<Ctx, _>(&mut request).expect("a valid id");
        assert_eq!(handed_over(&request).as_deref(), Some(UUID));
    }

    /// Positive: every shape RustFS mints is accepted, including its two fallbacks.
    #[test]
    fn every_shape_rustfs_mints_is_accepted() {
        for id in [UUID, "trace-0123456789abcdef", "req-0123456789abcdef"] {
            let mut request = request_with(id);
            attach_host_request_id::<Ctx, _>(&mut request).expect(id);
            assert_eq!(handed_over(&request).as_deref(), Some(id));
        }
    }

    /// Positive: two requests keep their own ids; nothing is shared or minted between them.
    #[test]
    fn each_request_keeps_its_own_id() {
        let mut first = request_with("first-id");
        let mut second = request_with("second-id");
        attach_host_request_id::<Ctx, _>(&mut first).expect("valid");
        attach_host_request_id::<Ctx, _>(&mut second).expect("valid");
        assert_eq!(handed_over(&first).as_deref(), Some("first-id"));
        assert_eq!(handed_over(&second).as_deref(), Some("second-id"));
    }

    /// Negative: a request the outer layer never saw gets no id, and the caller is told.
    #[test]
    fn a_missing_context_hands_over_nothing() {
        let mut request = http::Request::new(());
        assert_eq!(attach_host_request_id::<Ctx, _>(&mut request), Err(TraceError::MissingContext));
        assert_eq!(handed_over(&request), None);
    }

    /// Negative: a header named like the id is not the id; only the context is read.
    #[test]
    fn a_request_id_header_is_not_read() {
        let mut request = http::Request::builder()
            .header("x-amz-request-id", "caller-chosen")
            .header("x-request-id", "caller-chosen")
            .body(())
            .expect("a request");
        assert_eq!(attach_host_request_id::<Ctx, _>(&mut request), Err(TraceError::MissingContext));
        assert_eq!(handed_over(&request), None);
    }

    /// Negative: an id outside the alphabet is refused where it stands, never repaired, and
    /// nothing is inserted.
    #[test]
    fn an_id_the_gateway_cannot_carry_is_refused() {
        let too_long = "a".repeat(65);
        for id in ["", "has space", "line\r\nbreak", "<RequestId>", "quo\"te", too_long.as_str()] {
            let mut request = request_with(id);
            assert!(
                matches!(attach_host_request_id::<Ctx, _>(&mut request), Err(TraceError::Invalid(_))),
                "{id:?}"
            );
            assert_eq!(handed_over(&request), None, "{id:?}");
        }
    }

    /// Negative: the refusal does not repeat the refused id.
    #[test]
    fn the_refusal_does_not_echo_the_id() {
        let mut request = request_with("secret token");
        let error = attach_host_request_id::<Ctx, _>(&mut request).expect_err("refused");
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
    }
}
