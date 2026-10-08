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

//! Builds a [`RequestEnvelope`] from an s3s request at the legacy edge.
//!
//! The request is only borrowed: until its call site moves to the envelope
//! (T1.8), the legacy handler keeps reading the same request.

use crate::app::storage_api::s3::S3Request;
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::get_session_token;

impl RequestEnvelope {
    /// Copies every request fact of `req` the app layer reads.
    pub fn from_s3s<T>(req: &S3Request<T>) -> Self {
        let credentials = req.credentials.as_ref().map(|credentials| {
            RequestCredentials::new(
                credentials.access_key.clone(),
                credentials.secret_key.expose().to_owned(),
                get_session_token(&req.uri, &req.headers).map(str::to_owned),
            )
        });
        let head = RequestHead {
            method: req.method.clone(),
            uri: req.uri.clone(),
            headers: req.headers.clone(),
            credentials,
            region: req.region.as_ref().map(|region| region.as_str().to_owned()),
            service: req.service.clone(),
        };
        Self::from_parts(head, req.extensions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestCredentials, RequestEnvelope, S3Request};
    use crate::app::metadata_route::WebsiteRead;
    use crate::app::object::request_body::BodyReadControl;
    use crate::app::storage_api::s3::Credentials as S3sCredentials;
    use crate::auth::{VerifiedPresignedRequest, VerifiedSigV4Request};
    use crate::runtime_sources::ServerContextSlot;
    use crate::shared_types::RemoteAddr;
    use crate::storage::access::envelope_payloads;
    use crate::storage::access::{PostObjectRequestMarker, ReqInfo, TableDataPlanePublicationGuards};
    use crate::storage::request_context::RequestContext;
    use http::{Extensions, HeaderMap, HeaderValue, Method};
    use rustfs_trusted_proxies::ClientInfo;
    use std::fmt::Debug;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Arc;
    use std::time::Instant;

    const PEER: ([u8; 4], u16) = ([10, 0, 0, 7], 41000);
    const CLIENT: [u8; 4] = [203, 0, 113, 9];
    const REQUEST_ID: &str = "envelope-request-id";

    /// A type no envelope field names.
    #[derive(Clone, Debug, PartialEq)]
    struct UnrelatedPayload(u8);

    fn request(extensions: Extensions) -> S3Request<()> {
        S3Request {
            input: (),
            method: Method::PUT,
            uri: "/bucket/key".parse().expect("static uri"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn credentials() -> Option<S3sCredentials> {
        Some(S3sCredentials {
            access_key: "AKIAENVELOPE".to_owned(),
            secret_key: "envelope-secret".into(),
        })
    }

    fn request_context() -> RequestContext {
        RequestContext {
            request_id: REQUEST_ID.to_owned(),
            x_amz_request_id: REQUEST_ID.to_owned(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        }
    }

    /// One value of every payload type the envelope names, each with a value
    /// that identifies it.
    fn every_named_payload(server_context: &Arc<ServerContextSlot>) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(Some(RemoteAddr(SocketAddr::from(PEER))));
        extensions.insert(ClientInfo::direct(SocketAddr::new(IpAddr::from(CLIENT), 443)));
        extensions.insert(request_context());
        extensions.insert(Arc::clone(server_context));
        extensions.insert(BodyReadControl::default());
        extensions.insert(VerifiedSigV4Request);
        extensions.insert(VerifiedPresignedRequest);
        extensions.insert(WebsiteRead);
        extensions.insert(PostObjectRequestMarker);
        extensions.insert(ReqInfo {
            bucket: Some("req-info-bucket".to_owned()),
            ..ReqInfo::default()
        });
        extensions.insert(envelope_payloads::bucket_generation("generation-bucket"));
        extensions.insert(envelope_payloads::pending_delete_bucket_generation("pending-delete-bucket"));
        extensions.insert(envelope_payloads::copy_source_bucket_generation("copy-source-bucket"));
        extensions.insert(envelope_payloads::odm_read_generation("odm-read-bucket"));
        extensions.insert(envelope_payloads::bucket_config_mutation("config-mutation-bucket"));
        extensions.insert(TableDataPlanePublicationGuards::default());
        extensions.insert(envelope_payloads::table_list_access("table-list-bucket"));
        extensions.insert(envelope_payloads::object_tag_conditions("object-tag-bucket"));
        extensions
    }

    /// The payload is present and is the instance built for `bucket`.
    fn assert_payload_for<T: Debug>(name: &str, payload: Option<&T>, bucket: &str) {
        let debug = format!("{payload:?}");
        assert!(payload.is_some() && debug.contains(&format!("\"{bucket}\"")), "{name}: {debug}");
    }

    fn assert_every_named_payload_absent(envelope: &RequestEnvelope) {
        assert_eq!(envelope.remote_addr(), None, "remote_addr");
        assert!(envelope.client_info().is_none(), "client_info");
        assert!(envelope.request_context().is_none(), "request_context");
        assert!(envelope.server_context().is_none(), "server_context");
        assert!(envelope.body_read_control().is_none(), "body_read_control");
        assert!(envelope.verified_sigv4().is_none(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_none(), "verified_presigned");
        assert!(envelope.website_read().is_none(), "website_read");
        assert!(envelope.post_object().is_none(), "post_object");
        assert!(envelope.req_info().is_none(), "req_info");
        assert!(envelope.bucket_generation().is_none(), "bucket_generation");
        assert!(envelope.pending_delete_bucket_generation().is_none(), "pending_delete_bucket_generation");
        assert!(envelope.copy_source_bucket_generation().is_none(), "copy_source_bucket_generation");
        assert!(envelope.odm_read_generation().is_none(), "odm_read_generation");
        assert!(envelope.bucket_config_mutation().is_none(), "bucket_config_mutation");
        assert!(envelope.table_publication_guards().is_none(), "table_publication_guards");
        assert!(envelope.table_list_access().is_none(), "table_list_access");
        assert!(envelope.object_tag_conditions().is_none(), "object_tag_conditions");
    }

    #[test]
    fn from_s3s_copies_every_named_extension() {
        let server_context = ServerContextSlot::new();
        let mut req = request(every_named_payload(&server_context));
        req.method = Method::POST;
        req.uri = "/bucket/key?uploads".parse().expect("static uri");
        req.headers.insert("x-envelope-probe", HeaderValue::from_static("probe"));
        req.headers
            .insert("x-amz-security-token", HeaderValue::from_static("header-token"));
        req.credentials = credentials();
        req.region = Some("eu-west-3".parse().expect("valid region"));
        req.service = Some("s3".to_owned());

        let envelope = RequestEnvelope::from_s3s(&req);

        assert_eq!(envelope.method(), Method::POST);
        assert_eq!(envelope.uri(), &req.uri);
        assert_eq!(envelope.headers(), &req.headers);
        let credentials = envelope.credentials().expect("credentials");
        assert_eq!(credentials.access_key(), "AKIAENVELOPE");
        assert_eq!(credentials.secret_key(), "envelope-secret");
        assert_eq!(credentials.session_token(), Some("header-token"));
        assert_eq!(envelope.region(), Some("eu-west-3"));
        assert_eq!(envelope.service(), Some("s3"));

        assert_eq!(envelope.remote_addr(), Some(SocketAddr::from(PEER)));
        assert_eq!(envelope.client_info().map(|info| info.real_ip), Some(IpAddr::from(CLIENT)));
        assert_eq!(envelope.request_context().map(|context| context.request_id.as_str()), Some(REQUEST_ID));
        assert!(
            envelope
                .server_context()
                .is_some_and(|slot| Arc::ptr_eq(slot, &server_context))
        );
        assert!(envelope.body_read_control().is_some(), "body_read_control");
        assert!(envelope.verified_sigv4().is_some(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_some(), "verified_presigned");
        assert!(envelope.website_read().is_some(), "website_read");
        assert!(envelope.post_object().is_some(), "post_object");
        assert_eq!(envelope.req_info().and_then(|info| info.bucket.as_deref()), Some("req-info-bucket"));
        assert_payload_for("bucket_generation", envelope.bucket_generation(), "generation-bucket");
        assert_payload_for(
            "pending_delete_bucket_generation",
            envelope.pending_delete_bucket_generation(),
            "pending-delete-bucket",
        );
        assert_payload_for(
            "copy_source_bucket_generation",
            envelope.copy_source_bucket_generation(),
            "copy-source-bucket",
        );
        assert_payload_for("odm_read_generation", envelope.odm_read_generation(), "odm-read-bucket");
        assert_payload_for("bucket_config_mutation", envelope.bucket_config_mutation(), "config-mutation-bucket");
        assert!(envelope.table_publication_guards().is_some(), "table_publication_guards");
        assert!(envelope.table_list_access().is_some(), "table_list_access");
        assert_payload_for("object_tag_conditions", envelope.object_tag_conditions(), "object-tag-bucket");
    }

    #[test]
    fn from_s3s_leaves_absent_payloads_none() {
        let mut req = request(Extensions::new());
        // A token on an anonymous request must not conjure credentials.
        req.headers
            .insert("x-amz-security-token", HeaderValue::from_static("anonymous-token"));

        let envelope = RequestEnvelope::from_s3s(&req);

        assert!(envelope.credentials().is_none(), "credentials");
        assert_eq!(envelope.region(), None, "region");
        assert_eq!(envelope.service(), None, "service");
        assert_every_named_payload_absent(&envelope);
        assert!(envelope.residual_extensions().is_empty(), "residual bag");
    }

    #[test]
    fn from_s3s_treats_a_peerless_remote_addr_as_absent() {
        let mut extensions = Extensions::new();
        extensions.insert(None::<RemoteAddr>);

        let envelope = RequestEnvelope::from_s3s(&request(extensions));

        assert_eq!(envelope.remote_addr(), None);
        assert!(envelope.residual_extensions().is_empty(), "the peerless marker is consumed, not kept");
    }

    #[test]
    fn from_s3s_keeps_unrelated_types_out_of_named_fields() {
        // Types adjacent to named payloads: the bare and raw peer addresses the
        // connection layer also inserts, wrapped variants of named types, and an
        // unrelated type. Today's readers never see any of them.
        let peer = SocketAddr::from(PEER);
        let mut extensions = Extensions::new();
        extensions.insert(RemoteAddr(peer));
        extensions.insert(peer);
        extensions.insert(Arc::new(request_context()));
        extensions.insert(Some(ClientInfo::direct(peer)));
        extensions.insert(UnrelatedPayload(7));

        let envelope = RequestEnvelope::from_s3s(&request(extensions));

        assert_every_named_payload_absent(&envelope);
        let residual = envelope.residual_extensions();
        assert_eq!(residual.len(), 5, "every unrelated type stays in the residual bag");
        assert_eq!(residual.get::<RemoteAddr>().map(|addr| addr.0), Some(peer));
        assert_eq!(residual.get::<SocketAddr>(), Some(&peer));
        assert!(residual.get::<Arc<RequestContext>>().is_some());
        assert!(residual.get::<Option<ClientInfo>>().is_some());
        assert_eq!(residual.get::<UnrelatedPayload>(), Some(&UnrelatedPayload(7)));
    }

    #[test]
    fn from_s3s_moves_named_payloads_out_of_the_residual_bag() {
        let server_context = ServerContextSlot::new();
        let mut extensions = every_named_payload(&server_context);
        extensions.insert(UnrelatedPayload(9));

        let envelope = RequestEnvelope::from_s3s(&request(extensions));

        let residual = envelope.residual_extensions();
        assert_eq!(residual.len(), 1, "only the unrelated type may remain");
        assert_eq!(residual.get::<UnrelatedPayload>(), Some(&UnrelatedPayload(9)));
    }

    #[test]
    fn from_s3s_takes_the_session_token_from_the_query_without_a_header() {
        let mut req = request(Extensions::new());
        req.uri = "/bucket/key?X-Amz-Security-Token=query-token".parse().expect("static uri");
        req.credentials = credentials();

        let envelope = RequestEnvelope::from_s3s(&req);

        assert_eq!(envelope.credentials().and_then(RequestCredentials::session_token), Some("query-token"));
    }

    #[test]
    fn from_s3s_leaves_the_session_token_none_when_the_request_carries_none() {
        let mut req = request(Extensions::new());
        req.credentials = credentials();

        let envelope = RequestEnvelope::from_s3s(&req);

        let credentials = envelope.credentials().expect("credentials");
        assert_eq!(credentials.access_key(), "AKIAENVELOPE");
        assert_eq!(credentials.session_token(), None);
    }
}
