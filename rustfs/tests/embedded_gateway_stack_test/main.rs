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

// The gateway S3 stack inside an embedded RustFS server (rustfs/backlog#2734 T2.5): one
// operation, ListBuckets, is served end to end, and every other S3 operation is answered
// `501 NotImplemented`. Requests are signed by the AWS SDK and travel through the whole
// middleware stack, so what is asserted here is what a client sees.

#![recursion_limit = "256"]

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::{RustFSServer, RustFSServerBuilder, find_available_port};
use rustfs_io_metrics::s3_http_metrics::s3_http_metrics_snapshot;
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{BucketOperations, MakeBucketOptions};
use std::collections::BTreeMap;
use std::future::Future;

mod common;

const ROOT_ACCESS_KEY: &str = "gatewaystackroot";
const ROOT_SECRET_KEY: &str = "gateway-stack-root-secret";
const USER_SECRET_KEY: &str = "gateway-stack-user-secret";
const ENV_S3_STACK: &str = "RUSTFS_S3_STACK";
const ENV_KEYSTONE_ENABLE: &str = "RUSTFS_KEYSTONE_ENABLE";
const ENV_KEYSTONE_AUTH_URL: &str = "RUSTFS_KEYSTONE_AUTH_URL";
/// A Keystone endpoint nothing listens on: these tests are about start-up, not about tokens.
const UNREACHABLE_KEYSTONE: &str = "http://127.0.0.1:1/v3";

fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let config = Config::builder()
        .credentials_provider(Credentials::new(access_key, secret_key, None, None, "test"))
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

/// Runs `test` against an embedded server started with `RUSTFS_S3_STACK` set to `stack`.
fn with_stack<F, Fut>(stack: Option<&'static str>, test: F)
where
    F: FnOnce(RustFSServer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    with_environment(vec![(ENV_S3_STACK, stack)], test);
}

/// Runs `test` against an embedded server started under `environment`.
fn with_environment<F, Fut>(environment: Vec<(&'static str, Option<&'static str>)>, test: F)
where
    F: FnOnce(RustFSServer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    temp_env::with_vars(environment, || {
        common::run_embedded_test(move || async move {
            let Some(started) = start_server().await else { return };
            test(started.expect("start embedded server")).await;
        });
    });
}

/// Starts an embedded server on a free port; `None` when the sandbox allows no port at all.
async fn start_server() -> Option<Result<RustFSServer, String>> {
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("find free port: {err}"),
    };
    let started = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key(ROOT_ACCESS_KEY)
        .secret_key(ROOT_SECRET_KEY)
        .build()
        .await;
    // Both renderings: the refusal's text may be in either.
    Some(started.map_err(|error| format!("{error} {error:?}")))
}

/// The S3 error code and HTTP status an SDK call was refused with.
fn refusal<E: ProvideErrorMetadata>(error: &SdkError<E>) -> (Option<String>, Option<u16>) {
    let status = error.raw_response().map(|response| response.status().as_u16());
    (error.code().map(str::to_owned), status)
}

/// One series of the S3 request counter: method, operation and outcome.
type Series = (&'static str, &'static str, &'static str);

/// How often each series of the S3 request counter has been counted in this process.
fn s3_request_counts() -> BTreeMap<Series, u64> {
    s3_http_metrics_snapshot()
        .into_iter()
        .map(|series| ((series.method, series.operation, series.outcome), series.total))
        .collect()
}

/// Every series counted since `before`, with how often. Reading the whole counter, not one
/// series, is what makes a second count under another label visible.
fn counted_since(before: &BTreeMap<Series, u64>) -> BTreeMap<Series, u64> {
    s3_request_counts()
        .into_iter()
        .filter_map(|(series, total)| {
            let counted = total - before.get(&series).copied().unwrap_or(0);
            (counted > 0).then_some((series, counted))
        })
        .collect()
}

async fn create_bucket_in_the_store(bucket: &str) {
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket in the store");
}

async fn create_user_without_a_policy(access_key: &str) {
    get_global_app_context()
        .expect("embedded server context")
        .iam()
        .handle()
        .create_user(
            access_key,
            &rustfs_madmin::AddOrUpdateUserReq {
                secret_key: USER_SECRET_KEY.to_string(),
                policy: None,
                status: rustfs_madmin::AccountStatus::Enabled,
            },
        )
        .await
        .expect("create user without a policy");
}

#[test]
fn the_gateway_stack_lists_the_caller_s_buckets_and_answers_every_other_operation_501() {
    with_stack(Some("gateway"), |server| async move {
        // CreateBucket is not served by this stack yet, so the buckets are made in the store.
        create_bucket_in_the_store("gateway-stack-alpha").await;
        create_bucket_in_the_store("gateway-stack-beta").await;
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

        let listing = client.list_buckets().send().await.expect("signed ListBuckets");
        let mut names: Vec<&str> = listing.buckets().iter().filter_map(|bucket| bucket.name()).collect();
        names.sort_unstable();
        assert_eq!(names, ["gateway-stack-alpha", "gateway-stack-beta"]);
        assert!(listing.owner().and_then(|owner| owner.id()).is_some(), "the listing names its owner");
        assert!(
            listing.buckets().iter().all(|bucket| bucket.creation_date().is_some()),
            "every bucket carries its creation date"
        );

        let get = client
            .get_object()
            .bucket("gateway-stack-alpha")
            .key("object")
            .send()
            .await
            .expect_err("GetObject is not registered");
        assert_eq!(refusal(&get), (Some("NotImplemented".to_owned()), Some(501)));

        let put = client
            .put_object()
            .bucket("gateway-stack-alpha")
            .key("object")
            .body(ByteStream::from_static(b"payload"))
            .send()
            .await
            .expect_err("PutObject is not registered");
        assert_eq!(refusal(&put), (Some("NotImplemented".to_owned()), Some(501)));

        let create = client
            .create_bucket()
            .bucket("gateway-stack-gamma")
            .send()
            .await
            .expect_err("CreateBucket is not registered");
        assert_eq!(refusal(&create), (Some("NotImplemented".to_owned()), Some(501)));

        let listing = client.list_buckets().send().await.expect("ListBuckets after the refusals");
        assert_eq!(listing.buckets().len(), 2, "a refused write created nothing");

        server.shutdown().await;
    });
}

#[test]
fn the_gateway_stack_refuses_callers_it_cannot_admit() {
    with_stack(Some("gateway"), |server| async move {
        create_bucket_in_the_store("gateway-stack-alpha").await;
        create_user_without_a_policy("gatewaystacknopolicy").await;
        let endpoint = server.endpoint();

        // Anonymous: no credentials at all.
        let anonymous = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("http client")
            .get(format!("{endpoint}/"))
            .send()
            .await
            .expect("anonymous ListBuckets is answered");
        assert_eq!(anonymous.status().as_u16(), 403);
        let request_id = anonymous
            .headers()
            .get("x-amz-request-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .expect("the refusal names its request");
        assert_eq!(
            anonymous.headers().get("x-request-id").and_then(|value| value.to_str().ok()),
            Some(request_id.as_str()),
            "one identifier names the request in both headers"
        );
        let body = anonymous.text().await.expect("refusal body");
        assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");

        // The right access key with the wrong secret.
        let wrong_secret = s3_client(&endpoint, ROOT_ACCESS_KEY, "not-the-root-secret")
            .list_buckets()
            .send()
            .await
            .expect_err("a wrong secret signs nothing");
        assert_eq!(refusal(&wrong_secret), (Some("SignatureDoesNotMatch".to_owned()), Some(403)));

        // An access key nobody issued: the caller's fault, not the server's.
        let unknown_key = s3_client(&endpoint, "gatewaystackunknown", USER_SECRET_KEY)
            .list_buckets()
            .send()
            .await
            .expect_err("an unknown access key is refused");
        assert_eq!(refusal(&unknown_key), (Some("InvalidAccessKeyId".to_owned()), Some(403)));

        // A real user whose policy grants nothing: it may neither list every bucket nor see one.
        let no_permission = s3_client(&endpoint, "gatewaystacknopolicy", USER_SECRET_KEY)
            .list_buckets()
            .send()
            .await
            .expect_err("a caller without permission lists nothing");
        assert_eq!(refusal(&no_permission), (Some("AccessDenied".to_owned()), Some(403)));

        // The root caller is still served by the same server.
        let listing = s3_client(&endpoint, server.access_key(), server.secret_key())
            .list_buckets()
            .send()
            .await
            .expect("signed ListBuckets");
        assert_eq!(listing.buckets().len(), 1);

        server.shutdown().await;
    });
}

/// The S3 request counter belongs to RustFS's layer in front of both stacks, and the operation
/// label is set by the code a handler calls. That only works if the gateway polls its handler
/// inside that layer's scope, on the request's own task, which is what this observes.
#[test]
fn the_gateway_stack_counts_each_request_once_under_the_label_the_legacy_stack_uses() {
    with_stack(Some("gateway"), |server| async move {
        let endpoint = server.endpoint();

        let before = s3_request_counts();
        s3_client(&endpoint, server.access_key(), server.secret_key())
            .list_buckets()
            .send()
            .await
            .expect("signed ListBuckets");
        assert_eq!(
            counted_since(&before),
            BTreeMap::from([(("GET", S3Operation::ListBuckets.as_str(), "2xx"), 1)]),
            "one served request is counted once, as ListBuckets"
        );

        let before = s3_request_counts();
        let refused = s3_client(&endpoint, ROOT_ACCESS_KEY, "not-the-root-secret")
            .list_buckets()
            .send()
            .await
            .expect_err("a wrong secret signs nothing");
        assert_eq!(refusal(&refused), (Some("SignatureDoesNotMatch".to_owned()), Some(403)));
        assert_eq!(
            counted_since(&before),
            BTreeMap::from([(("GET", "unknown", "4xx"), 1)]),
            "a request refused by authentication is counted once, under no operation"
        );

        server.shutdown().await;
    });
}

/// The pinned gateway admits a request that carries only `X-Auth-Token` as anonymous, while the
/// RustFS code behind it reads the Keystone identity. Until the gateway authenticates the token
/// (rustfs/gateway#1586) the two settings together refuse to start, and nothing is served.
#[test]
fn the_gateway_stack_refuses_to_start_with_keystone_enabled() {
    let environment = [
        (ENV_S3_STACK, Some("gateway")),
        (ENV_KEYSTONE_ENABLE, Some("true")),
        (ENV_KEYSTONE_AUTH_URL, Some(UNREACHABLE_KEYSTONE)),
    ];
    temp_env::with_vars(environment, || {
        common::run_embedded_test(|| async {
            let Some(started) = start_server().await else { return };
            let refusal = match started {
                Ok(server) => {
                    server.shutdown().await;
                    panic!("the gateway stack started with Keystone enabled");
                }
                Err(refusal) => refusal,
            };
            for named in ["RUSTFS_S3_STACK=gateway", "RUSTFS_KEYSTONE_ENABLE", "rustfs/gateway#1586"] {
                assert!(refusal.contains(named), "{named} is missing from: {refusal}");
            }
        });
    });
}

#[test]
fn the_legacy_stack_starts_with_keystone_enabled() {
    let environment = vec![
        (ENV_S3_STACK, None),
        (ENV_KEYSTONE_ENABLE, Some("true")),
        (ENV_KEYSTONE_AUTH_URL, Some(UNREACHABLE_KEYSTONE)),
    ];
    with_environment(environment, |server| async move {
        let listing = s3_client(&server.endpoint(), server.access_key(), server.secret_key())
            .list_buckets()
            .send()
            .await
            .expect("signed ListBuckets on the legacy stack with Keystone enabled");
        assert!(listing.buckets().is_empty());

        server.shutdown().await;
    });
}

#[test]
fn the_legacy_stack_still_serves_every_operation() {
    with_stack(None, |server| async move {
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

        client
            .create_bucket()
            .bucket("legacy-stack-alpha")
            .send()
            .await
            .expect("CreateBucket on the legacy stack");
        client
            .put_object()
            .bucket("legacy-stack-alpha")
            .key("object")
            .body(ByteStream::from_static(b"payload"))
            .send()
            .await
            .expect("PutObject on the legacy stack");
        let before = s3_request_counts();
        let listing = client.list_buckets().send().await.expect("ListBuckets on the legacy stack");
        assert_eq!(
            counted_since(&before),
            BTreeMap::from([(("GET", S3Operation::ListBuckets.as_str(), "2xx"), 1)]),
            "the label the gateway stack is held to is the one this stack counts under"
        );
        let names: Vec<&str> = listing.buckets().iter().filter_map(|bucket| bucket.name()).collect();
        assert_eq!(names, ["legacy-stack-alpha"]);
        assert!(listing.owner().and_then(|owner| owner.id()).is_some(), "the listing names its owner");

        let missing = client
            .get_object()
            .bucket("legacy-stack-alpha")
            .key("missing")
            .send()
            .await
            .expect_err("a missing key");
        assert_eq!(refusal(&missing), (Some("NoSuchKey".to_owned()), Some(404)));

        create_user_without_a_policy("legacystacknopolicy").await;
        let no_permission = s3_client(&server.endpoint(), "legacystacknopolicy", USER_SECRET_KEY)
            .list_buckets()
            .send()
            .await
            .expect_err("a caller without permission lists nothing on either stack");
        assert_eq!(refusal(&no_permission), (Some("AccessDenied".to_owned()), Some(403)));

        server.shutdown().await;
    });
}
