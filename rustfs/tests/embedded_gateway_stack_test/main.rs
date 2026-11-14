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

// The gateway S3 stack inside an embedded RustFS server (rustfs/backlog#2734). Requests are signed
// by the AWS SDK and travel through the whole middleware stack, so what is asserted here is what a
// client sees. Every served operation has a row in the refusal matrix, kept per migration family in
// the module named after it, or an entry in `ANSWERS_EVERY_CALLER_ALIKE`; both run on both stacks.

#![recursion_limit = "256"]

#[path = "../common/mod.rs"]
mod common;

mod bucket_access;
mod bucket_configs;
mod bucket_core;
mod bucket_replication_lifecycle;
mod listing_multipart;
mod object_commit;
mod object_meta;
mod object_misc;
mod object_read;
mod object_write;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::{RustFSServer, RustFSServerBuilder, find_available_port, gateway_operations};
use rustfs_ecstore::api::object::PutObjReader;
use rustfs_io_metrics::s3_http_metrics::s3_http_metrics_snapshot;
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{BucketOperations, BucketOptions, MakeBucketOptions, ObjectIO};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

const ROOT_ACCESS_KEY: &str = "gatewaystackroot";
const ROOT_SECRET_KEY: &str = "gateway-stack-root-secret";
const USER_SECRET_KEY: &str = "gateway-stack-user-secret";
const NO_POLICY_USER: &str = "gatewaystacknopolicy";
const MATRIX_BUCKET: &str = "gateway-stack-matrix";
const ENV_S3_STACK: &str = "RUSTFS_S3_STACK";
const ENV_KEYSTONE_ENABLE: &str = "RUSTFS_KEYSTONE_ENABLE";
const ENV_KEYSTONE_AUTH_URL: &str = "RUSTFS_KEYSTONE_AUTH_URL";
/// A Keystone endpoint nothing listens on: these tests are about start-up, not about tokens.
const UNREACHABLE_KEYSTONE: &str = "http://127.0.0.1:1/v3";
const GATEWAY: Option<&str> = Some("gateway");
const LEGACY: Option<&str> = None;

/// One served operation as a client calls it.
pub(crate) struct MatrixRow {
    pub(crate) operation: &'static str,
    /// The label the S3 request counter counts a served call under.
    pub(crate) metric: S3Operation,
    /// Whether a refusal carries an error document. A HEAD answer carries none, so no code.
    pub(crate) has_error_body: bool,
    /// Calls the operation on the bucket named by the second argument.
    pub(crate) call: for<'a> fn(&'a Client, &'a str) -> BoxFuture<'a, Outcome>,
}

/// Every family's rows, in the order of the migration batches. A batch appends to its own module.
const MATRIX: &[&[MatrixRow]] = &[
    bucket_core::MATRIX,
    bucket_configs::MATRIX,
    bucket_access::MATRIX,
    bucket_replication_lifecycle::MATRIX,
    object_meta::MATRIX,
    listing_multipart::MATRIX,
    object_read::MATRIX,
    object_write::MATRIX,
    object_commit::MATRIX,
    object_misc::MATRIX,
];

fn matrix() -> impl Iterator<Item = &'static MatrixRow> {
    MATRIX.iter().flat_map(|family| family.iter())
}

/// A served operation whose answer does not depend on the caller, so it has no refusal rows. It
/// must refuse every caller it serves alike, never serve data: an operation here cannot dodge the
/// matrix while answering `2xx`.
struct AnswersEveryCallerAlike {
    operation: &'static str,
    reason: &'static str,
    metric: S3Operation,
    /// The code and status every admitted caller gets.
    answer: (&'static str, u16),
    call: for<'a> fn(&'a Client, &'a str) -> BoxFuture<'a, Outcome>,
}

const ANSWERS_EVERY_CALLER_ALIKE: &[AnswersEveryCallerAlike] = &[AnswersEveryCallerAlike {
    operation: "GetObjectTorrent",
    reason: "always NoSuchKey: the legacy hook checks nothing and no torrent is ever served",
    metric: S3Operation::GetObjectTorrent,
    answer: ("NoSuchKey", 404),
    call: object_meta::get_object_torrent,
}];

/// What one call observed: served, or refused.
pub(crate) type Outcome = Result<(), Refusal>;

/// A refusal as the client saw it: the error code, the status, and both request-id headers.
#[derive(Debug, PartialEq)]
pub(crate) struct Refusal {
    code: Option<String>,
    status: Option<u16>,
    request_ids: [Option<String>; 2],
}

pub(crate) fn outcome<T, E: ProvideErrorMetadata>(result: Result<T, SdkError<E>>) -> Outcome {
    let error = match result {
        Ok(_) => return Ok(()),
        Err(error) => error,
    };
    let response = error.raw_response();
    let header = |name: &str| response.and_then(|response| response.headers().get(name)).map(str::to_owned);
    Err(Refusal {
        code: error.code().map(str::to_owned),
        status: response.map(|response| response.status().as_u16()),
        request_ids: [header("x-amz-request-id"), header("x-request-id")],
    })
}

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

/// A client that signs nothing.
async fn anonymous_client(endpoint: &str) -> Client {
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .no_credentials()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .load()
        .await;
    Client::from_conf(aws_sdk_s3::config::Builder::from(&shared).force_path_style(true).build())
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
            test(start_server().await.expect("start embedded server")).await;
        });
    });
}

/// Starts an embedded server on a free port. A host without a free port fails the test rather
/// than passing it unobserved.
async fn start_server() -> Result<RustFSServer, String> {
    let port = find_available_port().expect("a free local port");
    let started = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key(ROOT_ACCESS_KEY)
        .secret_key(ROOT_SECRET_KEY)
        .build()
        .await;
    // Both renderings: the refusal's text may be in either.
    started.map_err(|error| format!("{error} {error:?}"))
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

async fn put_object_in_the_store(bucket: &str, key: &str) {
    let mut data = PutObjReader::from_vec(b"payload".to_vec());
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .put_object(bucket, key, &mut data, &Default::default())
        .await
        .expect("put object in the store");
}

async fn bucket_exists_in_the_store(bucket: &str) -> bool {
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .is_ok()
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

/// Creates `access_key` with the user secret and attaches a policy granting `action` on `bucket`
/// alone, so a call on any other bucket is a wrong-resource refusal.
async fn create_user_allowed_on_one_bucket(access_key: &str, action: &str, bucket: &str) {
    create_user_without_a_policy(access_key).await;
    let document = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["{action}"],"Resource":["arn:aws:s3:::{bucket}"]}}]}}"#
    );
    let policy = rustfs_policy::policy::Policy::parse_config(document.as_bytes()).expect("parse policy");
    let iam = get_global_app_context().expect("embedded server context").iam().handle();
    iam.set_policy(access_key, policy).await.expect("store policy");
    iam.policy_db_set(access_key, rustfs_iam::store::UserType::Reg, false, access_key)
        .await
        .expect("attach policy");
}

#[test]
fn every_served_operation_has_one_matrix_row_and_every_row_a_served_operation() {
    let rows: Vec<&str> = matrix().map(|row| row.operation).collect();
    let distinct: BTreeSet<&str> = rows.iter().copied().collect();

    let alike: BTreeSet<&str> = ANSWERS_EVERY_CALLER_ALIKE.iter().map(|entry| entry.operation).collect();

    assert_eq!(rows.len(), distinct.len(), "an operation has two rows: {rows:?}");
    assert_eq!(alike.len(), ANSWERS_EVERY_CALLER_ALIKE.len(), "an operation is listed twice: {alike:?}");
    assert!(distinct.is_disjoint(&alike), "{:?}", distinct.intersection(&alike));
    assert_eq!(&distinct | &alike, gateway_operations().into_iter().collect());
}

/// Every operation that answers every caller alike gives root, the user without a policy and the
/// anonymous caller its one non-`2xx` answer, counted once under its label, while authentication
/// still refuses a wrong secret and an unknown key before any operation is known.
async fn every_caller_gets_the_same_refusal_from_an_operation_that_answers_all_alike(server: RustFSServer) {
    create_bucket_in_the_store(MATRIX_BUCKET).await;
    create_user_without_a_policy(NO_POLICY_USER).await;
    let endpoint = server.endpoint();
    let admitted = [
        ("root", s3_client(&endpoint, server.access_key(), server.secret_key())),
        ("no policy", s3_client(&endpoint, NO_POLICY_USER, USER_SECRET_KEY)),
        ("anonymous", anonymous_client(&endpoint).await),
    ];
    let unauthenticated = [
        ("wrong secret", s3_client(&endpoint, ROOT_ACCESS_KEY, "not-the-root-secret")),
        ("unknown key", s3_client(&endpoint, "gatewaystackunknown", USER_SECRET_KEY)),
    ];

    for entry in ANSWERS_EVERY_CALLER_ALIKE {
        let (code, status) = entry.answer;
        assert!(!(200..300).contains(&status), "{} ({}) serves", entry.operation, entry.reason);
        for (caller, client) in &admitted {
            let before = s3_request_counts();
            let context = format!("{} as the {caller} caller", entry.operation);
            let refusal = (entry.call)(client, MATRIX_BUCKET).await.expect_err(&context);
            assert_eq!((refusal.code.as_deref(), refusal.status), (Some(code), Some(status)), "{context}");
            let counted = counted_since(&before);
            let series = [(entry.metric.as_str(), "4xx")];
            assert!(counted.values().all(|total| *total == 1), "{context} {counted:?}");
            assert!(
                counted.keys().map(|(_, label, outcome)| (*label, *outcome)).eq(series),
                "{context} {counted:?}"
            );
        }
        for (caller, client) in &unauthenticated {
            let context = format!("{} as the {caller} caller", entry.operation);
            let refusal = (entry.call)(client, MATRIX_BUCKET).await.expect_err(&context);
            assert_eq!(refusal.status, Some(403), "{context}");
        }
    }

    server.shutdown().await;
}

#[test]
fn operations_that_answer_every_caller_alike_do_so_on_the_gateway_stack() {
    with_stack(GATEWAY, every_caller_gets_the_same_refusal_from_an_operation_that_answers_all_alike);
}

#[test]
fn operations_that_answer_every_caller_alike_do_so_on_the_legacy_stack() {
    with_stack(LEGACY, every_caller_gets_the_same_refusal_from_an_operation_that_answers_all_alike);
}

/// Every served operation serves the root caller, counted once under its label, and refuses
/// each caller it cannot admit with the code the legacy stack answers, counted once.
async fn every_served_operation_serves_root_and_refuses_the_callers_it_cannot_admit(server: RustFSServer) {
    create_bucket_in_the_store(MATRIX_BUCKET).await;
    create_bucket_in_the_store(object_meta::MATRIX_BUCKET).await;
    create_bucket_in_the_store(object_misc::MATRIX_BUCKET).await;
    create_bucket_in_the_store(object_read::MATRIX_BUCKET).await;
    create_user_without_a_policy(NO_POLICY_USER).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    // The caller, its client, the code it is refused with, and whether authentication refuses it
    // before any operation is known.
    let refused = [
        ("anonymous", anonymous_client(&endpoint).await, "AccessDenied", false),
        (
            "wrong secret",
            s3_client(&endpoint, ROOT_ACCESS_KEY, "not-the-root-secret"),
            "SignatureDoesNotMatch",
            true,
        ),
        (
            "unknown key",
            s3_client(&endpoint, "gatewaystackunknown", USER_SECRET_KEY),
            "InvalidAccessKeyId",
            true,
        ),
        ("no policy", s3_client(&endpoint, NO_POLICY_USER, USER_SECRET_KEY), "AccessDenied", false),
    ];

    // A caller served where it must be refused is collected, so one run names every such caller.
    let mut served = Vec::new();
    for row in matrix() {
        let before = s3_request_counts();
        assert_eq!((row.call)(&root, MATRIX_BUCKET).await, Ok(()), "{} as root", row.operation);
        let counted = counted_since(&before);
        assert_eq!(counted.len(), 1, "{}: {counted:?}", row.operation);
        assert!(
            counted
                .iter()
                .all(|((_, label, outcome), total)| (*label, *outcome, *total) == (row.metric.as_str(), "2xx", 1)),
            "{}: {counted:?}",
            row.operation
        );

        for (caller, client, code, by_authentication) in &refused {
            let before = s3_request_counts();
            let refusal = match (row.call)(client, MATRIX_BUCKET).await {
                Ok(()) => {
                    served.push(format!("{} serves the {caller} caller", row.operation));
                    continue;
                }
                Err(refusal) => refusal,
            };
            let context = format!("{} as the {caller} caller: {refusal:?}", row.operation);
            assert_eq!(refusal.status, Some(403), "{context}");
            if row.has_error_body {
                assert_eq!(refusal.code.as_deref(), Some(*code), "{context}");
            }
            assert!(refusal.request_ids[0].is_some(), "the refusal names its request: {context}");
            assert_eq!(
                refusal.request_ids[0], refusal.request_ids[1],
                "one identifier in both headers: {context}"
            );
            let counted = counted_since(&before);
            assert_eq!(counted.values().sum::<u64>(), 1, "counted once: {context} {counted:?}");
            assert!(counted.keys().all(|(_, _, outcome)| *outcome == "4xx"), "{context} {counted:?}");
            if *by_authentication {
                assert!(
                    counted.keys().all(|(_, label, _)| *label == "unknown"),
                    "under no operation: {context} {counted:?}"
                );
            }
        }
    }
    assert!(served.is_empty(), "{served:?}");

    server.shutdown().await;
}

#[test]
fn the_refusal_matrix_holds_on_the_gateway_stack() {
    with_stack(GATEWAY, every_served_operation_serves_root_and_refuses_the_callers_it_cannot_admit);
}

#[test]
fn the_refusal_matrix_holds_on_the_legacy_stack() {
    with_stack(LEGACY, every_served_operation_serves_root_and_refuses_the_callers_it_cannot_admit);
}

/// Names RustFS creates and the AWS bucket grammar refuses. An IP-shaped name such as `256.1.1.1`
/// is also outside the AWS grammar, but RustFS's own create rule refuses it, so no store holds one.
const RUSTFS_ONLY_NAMES: [&str; 5] = ["a-s3alias", "sthree-x", "x--ol-s3", "x--x-s3", "xn--abc"];

/// A listing names every bucket the store holds, as it is stored, with its date and the owner.
async fn the_listing_names_every_stored_bucket(server: RustFSServer) {
    for name in RUSTFS_ONLY_NAMES {
        create_bucket_in_the_store(name).await;
    }

    let listing = s3_client(&server.endpoint(), server.access_key(), server.secret_key())
        .list_buckets()
        .send()
        .await
        .expect("signed ListBuckets");

    let mut names: Vec<&str> = listing.buckets().iter().filter_map(|bucket| bucket.name()).collect();
    names.sort_unstable();
    let mut expected = RUSTFS_ONLY_NAMES;
    expected.sort_unstable();
    assert_eq!(names, expected);
    assert!(listing.owner().and_then(|owner| owner.id()).is_some(), "the listing names its owner");
    assert!(
        listing.buckets().iter().all(|bucket| bucket.creation_date().is_some()),
        "every bucket carries its creation date"
    );
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_lists_every_stored_bucket() {
    with_stack(GATEWAY, the_listing_names_every_stored_bucket);
}

#[test]
fn the_legacy_stack_lists_every_stored_bucket() {
    with_stack(LEGACY, the_listing_names_every_stored_bucket);
}

/// HeadBucket on the legacy stack answers every name as before the use case moved onto the
/// gateway's types: a stored name the AWS grammar refuses is found, a missing one is not, and a
/// name the edge refuses is refused there, never with a server fault. Every status was measured
/// on `main` before the move. The SDK sends nothing for a `--x-s3` name: it treats it as a
/// directory bucket and refuses it on the client.
#[test]
fn the_legacy_stack_heads_every_bucket_name_as_before() {
    with_stack(LEGACY, |server| async move {
        for name in RUSTFS_ONLY_NAMES {
            create_bucket_in_the_store(name).await;
        }
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        let too_long = "a".repeat(64);
        let expected = [
            ("a-s3alias", Some(200)),
            ("sthree-x", Some(200)),
            ("x--ol-s3", Some(200)),
            ("x--x-s3", None),
            ("xn--abc", Some(400)),
            ("missing-bucket", Some(404)),
            ("UPPER", Some(400)),
            ("256.1.1.1", Some(404)),
            ("ab", Some(400)),
            (too_long.as_str(), Some(400)),
        ];

        let mut answered = Vec::new();
        let mut wanted = Vec::new();
        for (name, status) in expected {
            let observed = match client.head_bucket().bucket(name).send().await {
                Ok(_) => Some(200),
                Err(error) => refusal(&error).1,
            };
            answered.push((name.to_owned(), observed));
            wanted.push((name.to_owned(), status));
        }
        assert_eq!(answered, wanted);
        server.shutdown().await;
    });
}

/// The PutObject-only upload-size capability is refused on any other operation, before it is
/// served, with the same answer on both stacks. The legacy stack refuses it in the admission both
/// edges share; the gateway refuses the query before its handler runs.
async fn a_misplaced_capability_query_is_refused(server: RustFSServer) {
    let refused = s3_client(&server.endpoint(), server.access_key(), server.secret_key())
        .list_buckets()
        .customize()
        .mutate_request(|request| {
            let target = format!("{}?x-rustfs-max-content-length=1", request.uri());
            request.set_uri(target).expect("a valid target");
        })
        .send()
        .await
        .expect_err("a capability query is refused on ListBuckets");

    assert_eq!(refusal(&refused), (Some("InvalidRequest".to_owned()), Some(400)));
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_refuses_a_misplaced_capability_query() {
    with_stack(GATEWAY, a_misplaced_capability_query_is_refused);
}

#[test]
fn the_legacy_stack_refuses_a_misplaced_capability_query() {
    with_stack(LEGACY, a_misplaced_capability_query_is_refused);
}

/// The pinned gateway admits a request that carries only `X-Auth-Token` as anonymous, while the
/// RustFS code behind it reads the Keystone identity. Until the gateway authenticates the token
/// (rustfs/gateway#1586) the two settings together refuse to start, and nothing is served.
#[test]
fn the_gateway_stack_refuses_to_start_with_keystone_enabled() {
    let environment = [
        (ENV_S3_STACK, GATEWAY),
        (ENV_KEYSTONE_ENABLE, Some("true")),
        (ENV_KEYSTONE_AUTH_URL, Some(UNREACHABLE_KEYSTONE)),
    ];
    temp_env::with_vars(environment, || {
        common::run_embedded_test(|| async {
            let refusal = match start_server().await {
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
        (ENV_S3_STACK, LEGACY),
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

/// The legacy stack still serves operations the gateway stack does not, in the same binary.
#[test]
fn the_legacy_stack_still_serves_writes_and_reads() {
    with_stack(LEGACY, |server| async move {
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
        let missing = client
            .get_object()
            .bucket("legacy-stack-alpha")
            .key("missing")
            .send()
            .await
            .expect_err("a missing key");
        assert_eq!(refusal(&missing), (Some("NoSuchKey".to_owned()), Some(404)));

        server.shutdown().await;
    });
}

/// HeadBucket's outcome with `x-amz-expected-bucket-owner` set to `owner`, or without it.
async fn head_bucket_owned_by(client: &Client, bucket: &str, owner: Option<&str>) -> Result<(), Option<u16>> {
    let sent = client
        .head_bucket()
        .bucket(bucket)
        .set_expected_bucket_owner(owner.map(str::to_owned))
        .send()
        .await;
    outcome(sent).map_err(|refusal| refusal.status)
}

/// ListBuckets' outcome with `x-amz-expected-bucket-owner` set to `owner`.
async fn list_buckets_owned_by(client: &Client, owner: &str) -> Result<(), Option<u16>> {
    let owner = owner.to_owned();
    let sent = client
        .list_buckets()
        .customize()
        .mutate_request(move |request| {
            request.headers_mut().insert("x-amz-expected-bucket-owner", owner.clone());
        })
        .send()
        .await;
    outcome(sent).map_err(|refusal| refusal.status)
}

/// `x-amz-expected-bucket-owner` against the owner id the listing reports. `expected` is the
/// outcome of the same id, another id and none, first on a bucket that exists and then on one that
/// does not, then on a reserved-shaped bucket, and of the same id on ListBuckets.
async fn expected_bucket_owner_answers(server: RustFSServer, expected: [Result<(), Option<u16>>; 10]) {
    const OWNED: &str = "gateway-stack-owned";
    const MISSING: &str = "gateway-stack-missing";
    const RESERVED: &str = "sthree-gateway-stack-owned";
    create_bucket_in_the_store(OWNED).await;
    create_bucket_in_the_store(RESERVED).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let listing = client.list_buckets().send().await.expect("signed ListBuckets");
    let reported = listing
        .owner()
        .and_then(|owner| owner.id())
        .expect("the listing names its owner")
        .to_owned();
    let other = format!("{reported}0");

    let mut answered = Vec::new();
    for bucket in [OWNED, MISSING, RESERVED] {
        for owner in [Some(reported.as_str()), Some(other.as_str()), None] {
            answered.push(head_bucket_owned_by(&client, bucket, owner).await);
        }
    }
    answered.push(list_buckets_owned_by(&client, &reported).await);

    assert_eq!(answered, expected, "same id, other id, absent for each bucket; ListBuckets");
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_serves_only_the_reported_bucket_owner() {
    with_stack(GATEWAY, |server| {
        expected_bucket_owner_answers(
            server,
            [
                Ok(()),
                Err(Some(403)),
                Ok(()),
                Err(Some(404)),
                Err(Some(403)),
                Err(Some(404)),
                Ok(()),
                Err(Some(403)),
                Ok(()),
                Err(Some(403)),
            ],
        )
    });
}

#[test]
fn the_legacy_stack_answers_expected_bucket_owner_requests() {
    with_stack(LEGACY, |server| {
        expected_bucket_owner_answers(
            server,
            [
                Ok(()),
                Ok(()),
                Ok(()),
                Err(Some(404)),
                Err(Some(404)),
                Err(Some(404)),
                Ok(()),
                Ok(()),
                Ok(()),
                Ok(()),
            ],
        )
    });
}
