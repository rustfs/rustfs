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

//! Refusal-matrix rows and behaviour of migration batch A1 (rustfs/backlog#2765): bucket CORS,
//! tagging, public access block and encryption. Every behaviour test runs on both stacks.

use super::{
    GATEWAY, LEGACY, MatrixRow, Outcome, USER_SECRET_KEY, create_bucket_in_the_store, create_user_allowed_on_one_bucket, outcome,
    s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::interceptors::{
    BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef,
};
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::operation::put_bucket_cors::builders::PutBucketCorsFluentBuilder;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use futures::future::BoxFuture;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::bucket::metadata_sys;
use rustfs_s3_ops::S3Operation;
use std::sync::{Arc, Mutex};

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "PutBucketCors",
        metric: S3Operation::PutBucketCors,
        has_error_body: true,
        call: put_bucket_cors,
    },
    MatrixRow {
        operation: "DeleteBucketCors",
        metric: S3Operation::DeleteBucketCors,
        has_error_body: true,
        call: delete_bucket_cors,
    },
];

fn put_bucket_cors<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(put_cors(client, bucket, cors_document()).send().await) })
}

fn delete_bucket_cors<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_bucket_cors().bucket(bucket).send().await) })
}

fn put_cors(client: &Client, bucket: &str, document: CorsConfiguration) -> PutBucketCorsFluentBuilder {
    client.put_bucket_cors().bucket(bucket).cors_configuration(document)
}

/// A document with every member of a rule, and a rule with only the required ones.
fn cors_document() -> CorsConfiguration {
    let full = CorsRule::builder()
        .id("full")
        .allowed_headers("x-a")
        .allowed_headers("x-b")
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_origins("https://a.example")
        .expose_headers("ETag")
        .max_age_seconds(300)
        .build();
    let minimal = CorsRule::builder().allowed_methods("HEAD").allowed_origins("*").build();
    CorsConfiguration::builder()
        .set_cors_rules(Some([full, minimal].map(|rule| rule.expect("valid rule")).to_vec()))
        .build()
        .expect("valid configuration")
}

/// One rule allowing `method` from `origin`.
fn single_rule(method: &str, origin: &str) -> CorsConfiguration {
    let rule = CorsRule::builder().allowed_methods(method).allowed_origins(origin).build();
    CorsConfiguration::builder()
        .cors_rules(rule.expect("valid rule"))
        .build()
        .expect("valid configuration")
}

/// Records the status of the one response a request receives.
#[derive(Clone, Debug, Default)]
pub(super) struct StatusCapture(Arc<Mutex<Option<u16>>>);

impl StatusCapture {
    pub(super) fn take(&self) -> Option<u16> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Intercept for StatusCapture {
    fn name(&self) -> &'static str {
        "bucket-configs-status-capture"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let status = context.response().status().as_u16();
        *self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("status capture mutex was poisoned"))? = Some(status);
        Ok(())
    }
}

fn is_integrity_claim(name: &str) -> bool {
    name == "content-md5" || name.starts_with("x-amz-checksum-") || name == "x-amz-sdk-checksum-algorithm"
}

/// Removes every integrity claim the SDK attaches to a request body, before the request is signed,
/// and records the claims still on the request as it is sent.
#[derive(Clone, Debug, Default)]
struct WithoutIntegrityClaim(Arc<Mutex<Option<Vec<String>>>>);

impl WithoutIntegrityClaim {
    fn sent_claims(&self) -> Option<Vec<String>> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Intercept for WithoutIntegrityClaim {
    fn name(&self) -> &'static str {
        "bucket-configs-without-integrity-claim"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request_mut().headers_mut();
        let claims: Vec<String> = headers
            .iter()
            .map(|(name, _)| name.to_owned())
            .filter(|name| is_integrity_claim(name))
            .collect();
        for name in claims {
            headers.remove(name);
        }
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let sent = context
            .request()
            .headers()
            .iter()
            .map(|(name, _)| name.to_owned())
            .filter(|name| is_integrity_claim(name))
            .collect();
        *self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("claim capture mutex was poisoned"))? = Some(sent);
        Ok(())
    }
}

/// The bytes PutBucketCors stores for [`cors_document`]: the persisted writer's form.
const PERSISTED_CORS: &str = concat!(
    "<CORSConfiguration><CORSRule><AllowedHeader>x-a</AllowedHeader><AllowedHeader>x-b</AllowedHeader>",
    "<AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>https://a.example</AllowedOrigin>",
    "<ExposeHeader>ETag</ExposeHeader><ID>full</ID><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule>",
    "<CORSRule><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>",
);

const CORS_BUCKET: &str = "gateway-stack-cors";
const CORS_GRANTED_BUCKET: &str = "gateway-stack-cors-granted";
/// A name RustFS creates and the AWS bucket grammar refuses (reserved suffix).
const CORS_RUSTFS_ONLY_BUCKET: &str = "gateway-stack-cors-s3alias";
const CORS_WRITER: &str = "gatewaystackcorswriter";

async fn stored_cors(bucket: &str) -> String {
    let metadata = metadata_sys::get(bucket).await.expect("bucket metadata");
    String::from_utf8_lossy(&metadata.cors_config_xml).into_owned()
}

/// PutBucketCors stores the persisted writer's bytes for a document, also on a bucket name only
/// RustFS admits; DeleteBucketCors answers 204 and removes them, whether or not a document exists.
/// Refused: a caller whose policy names another bucket, and the documents the codecs refuse.
async fn the_cors_document_is_stored_and_removed(server: RustFSServer) {
    for bucket in [CORS_BUCKET, CORS_GRANTED_BUCKET, CORS_RUSTFS_ONLY_BUCKET] {
        create_bucket_in_the_store(bucket).await;
    }
    create_user_allowed_on_one_bucket(CORS_WRITER, "s3:PutBucketCors", CORS_GRANTED_BUCKET).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let writer = s3_client(&endpoint, CORS_WRITER, USER_SECRET_KEY);
    let refused = |result: Outcome| result.map_err(|refusal| (refusal.code, refusal.status));

    let wrong_bucket = outcome(put_cors(&writer, CORS_BUCKET, cors_document()).send().await);
    assert_eq!(refused(wrong_bucket), Err((Some("AccessDenied".to_owned()), Some(403))));
    assert_eq!(stored_cors(CORS_BUCKET).await, "", "a refused write stores nothing");
    let granted = outcome(put_cors(&writer, CORS_GRANTED_BUCKET, cors_document()).send().await);
    assert_eq!(refused(granted), Ok(()));

    let unknown_method = outcome(put_cors(&root, CORS_BUCKET, single_rule("PATCH", "*")).send().await);
    let two_wildcards = outcome(
        put_cors(&root, CORS_BUCKET, single_rule("GET", "https://*.*.example"))
            .send()
            .await,
    );
    let without_claim = WithoutIntegrityClaim::default();
    let no_integrity_claim = outcome(
        put_cors(&root, CORS_BUCKET, cors_document())
            .customize()
            .interceptor(without_claim.clone())
            .send()
            .await,
    );
    assert_eq!(without_claim.sent_claims(), Some(vec![]), "the request left without an integrity claim");
    let observed = [unknown_method, two_wildcards, no_integrity_claim].map(refused);
    assert_eq!(observed, [Ok(()), Ok(()), Ok(())], "unknown method, two wildcards, no integrity claim");

    for bucket in [CORS_BUCKET, CORS_RUSTFS_ONLY_BUCKET] {
        assert_eq!(
            refused(outcome(put_cors(&root, bucket, cors_document()).send().await)),
            Ok(()),
            "{bucket}"
        );
        assert_eq!(stored_cors(bucket).await, PERSISTED_CORS, "{bucket}");
    }

    for attempt in ["configured", "unconfigured"] {
        let status = StatusCapture::default();
        let deleted = root
            .delete_bucket_cors()
            .bucket(CORS_BUCKET)
            .customize()
            .interceptor(status.clone())
            .send()
            .await;
        assert_eq!((refused(outcome(deleted)), status.take()), (Ok(()), Some(204)), "{attempt}");
        assert_eq!(stored_cors(CORS_BUCKET).await, "", "{attempt}");
    }

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_stores_and_removes_the_cors_document() {
    with_stack(GATEWAY, the_cors_document_is_stored_and_removed);
}

#[test]
fn the_legacy_stack_stores_and_removes_the_cors_document() {
    with_stack(LEGACY, the_cors_document_is_stored_and_removed);
}
