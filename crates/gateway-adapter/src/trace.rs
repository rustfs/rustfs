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

//! The request id RustFS's outer layer minted, handed to the gateway as its own when that is safe.
//!
//! Responsible for: reading the id from the context the outer layer put in the request's
//! extensions ([`RequestIdCarrier`]) and, when it is inside the gateway's identifier alphabet and
//! length, inserting it as the gateway's [`HostRequestId`].
//! NOT responsible for: minting any id, refusing any request (nothing here can fail a request),
//! choosing the response headers (the gateway writes `x-amz-request-id` from the id it was
//! handed, and RustFS's outer layer overwrites that header on S3 paths), or the layer that mints
//! (`rustfs/src/server/layer.rs`). It never reads a header itself.
//! Upstream: RustFS's `RequestContext`, which the binary makes a [`RequestIdCarrier`]. Downstream:
//! the gateway service, which reads the [`HostRequestId`] extension once, at the top.
//!
//! # Whose id it is
//!
//! On S3 paths the outer layer mints a server-owned UUID. On admin, STS and console paths the
//! context copies the caller's `x-request-id` (or `x-amz-request-id`), as legacy does, so there
//! the id can be caller-chosen. Either way the id is attached only when it is inert, that is 1 to
//! 64 bytes of ASCII letters, digits, `-`, `.`, `_` and `~`, which is safe in a header, an XML
//! document and a log line. Anything else is not attached and the request is **forwarded
//! unchanged**: the gateway then mints its own id. No request is ever refused because of its id.
//!
//! # Known difference when nothing is attached
//!
//! The gateway answers with the id it minted (error body `<RequestId>`, its events and audit
//! records) while, on S3 paths, the outer layer overwrites the response header with RustFS's id.
//! So for such a request the header and the error body name different ids. That needs a caller-
//! chosen id outside the alphabet, or a request that never passed the outer layer.
//!
//! # Why this is not a `TraceSource`
//!
//! The gateway's `TraceSource::mint` takes no request, so a source cannot read the id RustFS
//! already minted; the gateway's documented path for a host that identifies its own requests is
//! the [`HostRequestId`] extension, and this module fills it.
//!
//! # Why a carrier trait
//!
//! `RequestContext` lives in the `rustfs` binary crate, which a library crate cannot depend on.
//! The binary implements [`RequestIdCarrier`] for it, and this module looks it up in the
//! extensions by that type.

use rustfs_gateway::HostRequestId;

/// A type in the request extensions that holds the id the outer layer minted or propagated.
pub trait RequestIdCarrier: Send + Sync + 'static {
    /// The canonical request id: the value RustFS logs and the response should carry.
    fn request_id(&self) -> &str;
}

/// What [`attach_host_request_id`] did. Ignoring it is harmless: every outcome forwards the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachOutcome {
    /// The carrier's id was inserted as the request's [`HostRequestId`].
    Attached,
    /// The request carries no context from the outer layer; nothing was inserted.
    NoContext,
    /// The carrier's id is empty, too long, or outside the gateway's alphabet; nothing was
    /// inserted and the id was not repaired.
    UnusableId,
}

/// The gateway's [`HostRequestId`] for the id `carrier` holds, or `None` when it is not inert.
#[must_use]
pub fn host_request_id<C: RequestIdCarrier + ?Sized>(carrier: &C) -> Option<HostRequestId> {
    HostRequestId::new(carrier.request_id()).ok()
}

/// Reads the `C` the outer layer put in `request`'s extensions and, when its id is inert, inserts
/// it as the request's [`HostRequestId`].
///
/// Never refuses the request: the caller forwards it whatever the outcome.
pub fn attach_host_request_id<C: RequestIdCarrier, B>(request: &mut http::Request<B>) -> AttachOutcome {
    let Some(carrier) = request.extensions().get::<C>() else {
        return AttachOutcome::NoContext;
    };
    let Some(id) = host_request_id(carrier) else {
        return AttachOutcome::UnusableId;
    };
    request.extensions_mut().insert(id);
    AttachOutcome::Attached
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

    fn attach(request: &mut http::Request<()>) -> AttachOutcome {
        attach_host_request_id::<Ctx, _>(request)
    }

    /// Positive: the id RustFS minted is the id the gateway is handed, byte for byte.
    #[test]
    fn the_minted_id_is_handed_over_unchanged() {
        let mut request = request_with(UUID);
        assert_eq!(attach(&mut request), AttachOutcome::Attached);
        assert_eq!(handed_over(&request).as_deref(), Some(UUID));
    }

    /// Positive: every shape RustFS mints is accepted, including its two fallbacks.
    #[test]
    fn every_shape_rustfs_mints_is_accepted() {
        for id in [UUID, "trace-0123456789abcdef", "req-0123456789abcdef"] {
            let mut request = request_with(id);
            assert_eq!(attach(&mut request), AttachOutcome::Attached, "{id}");
            assert_eq!(handed_over(&request).as_deref(), Some(id));
        }
    }

    /// Positive: a caller-supplied id that is inert (admin and STS paths copy it) is attached, and
    /// the longest allowed one is accepted.
    #[test]
    fn an_inert_caller_supplied_id_is_attached() {
        let longest = "a".repeat(64);
        for id in ["abc", "my.trace_id~1-2", longest.as_str()] {
            let mut request = request_with(id);
            assert_eq!(attach(&mut request), AttachOutcome::Attached, "{id}");
            assert_eq!(handed_over(&request).as_deref(), Some(id));
        }
    }

    /// Positive: two requests keep their own ids; nothing is shared or minted between them.
    #[test]
    fn each_request_keeps_its_own_id() {
        let mut first = request_with("first-id");
        let mut second = request_with("second-id");
        attach(&mut first);
        attach(&mut second);
        assert_eq!(handed_over(&first).as_deref(), Some("first-id"));
        assert_eq!(handed_over(&second).as_deref(), Some("second-id"));
    }

    /// Positive: with both a carrier and an `x-amz-request-id` header, the carrier's id wins.
    #[test]
    fn the_carrier_wins_over_a_request_id_header() {
        let mut request = http::Request::builder()
            .header("x-amz-request-id", "caller-chosen")
            .header("x-request-id", "caller-chosen")
            .body(())
            .expect("a request");
        request.extensions_mut().insert(Ctx(UUID.to_owned()));
        assert_eq!(attach(&mut request), AttachOutcome::Attached);
        assert_eq!(handed_over(&request).as_deref(), Some(UUID));
    }

    /// Negative: a request the outer layer never saw gets no id, and the request is not refused.
    #[test]
    fn a_missing_context_hands_over_nothing() {
        let mut request = http::Request::new(());
        assert_eq!(attach(&mut request), AttachOutcome::NoContext);
        assert_eq!(handed_over(&request), None);
    }

    /// Negative: a header named like the id is not the id; only the context is read.
    #[test]
    fn a_request_id_header_alone_is_not_read() {
        let mut request = http::Request::builder()
            .header("x-amz-request-id", "caller-chosen")
            .header("x-request-id", "caller-chosen")
            .body(())
            .expect("a request");
        assert_eq!(attach(&mut request), AttachOutcome::NoContext);
        assert_eq!(handed_over(&request), None);
    }

    /// Negative: an id outside the alphabet or the length is never attached or repaired, and the
    /// outcome says so rather than failing the request.
    #[test]
    fn an_id_the_gateway_cannot_carry_is_not_attached() {
        let too_long = "a".repeat(65);
        for id in [
            "",
            "has space",
            "line\r\nbreak",
            "<RequestId>",
            "quo\"te",
            "Root=1-5759e988",
            too_long.as_str(),
        ] {
            let mut request = request_with(id);
            assert_eq!(attach(&mut request), AttachOutcome::UnusableId, "{id:?}");
            assert_eq!(handed_over(&request), None, "{id:?}");
        }
    }

    /// Negative: the unusable id is not repaired into a shorter or cleaned-up one.
    #[test]
    fn host_request_id_does_not_repair() {
        assert!(host_request_id(&Ctx("has space".to_owned())).is_none());
        assert!(host_request_id(&Ctx(format!("{UUID} "))).is_none());
        assert!(host_request_id(&Ctx(UUID.to_owned())).is_some());
    }
}
