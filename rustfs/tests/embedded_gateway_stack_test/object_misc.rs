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

//! Refusal-matrix rows and behaviour of DeleteObject and RestoreObject.

use super::bucket_configs::StatusCapture;
use super::{
    GATEWAY, LEGACY, MatrixRow, NO_POLICY_USER, Outcome, USER_SECRET_KEY, anonymous_client, create_bucket_in_the_store,
    create_user_without_a_policy, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CsvInput, CsvOutput, Delete, ExpressionType, FileHeaderInfo, InputSerialization, ObjectIdentifier,
    OutputSerialization, RestoreRequest, VersioningConfiguration,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::error::is_err_object_not_found;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_ecstore::api::runtime::global_tier_config_mgr;
use rustfs_ecstore::api::tier::test_util::{MockWarmBackend, register_mock_tier};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{ObjectIO as _, ObjectOperations as _};

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "DeleteObject",
        metric: S3Operation::DeleteObject,
        has_error_body: true,
        call: delete_object,
    },
    MatrixRow {
        operation: "DeleteObjects",
        metric: S3Operation::DeleteObjects,
        has_error_body: true,
        call: delete_objects,
    },
    MatrixRow {
        operation: "RestoreObject",
        metric: S3Operation::RestoreObject,
        has_error_body: true,
        call: restore_object,
    },
    MatrixRow {
        operation: "SelectObjectContent",
        metric: S3Operation::SelectObjectContent,
        has_error_body: true,
        call: select_object,
    },
];

pub(super) const MATRIX_BUCKET: &str = "e2-delete-matrix";

fn delete_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.delete_object().bucket(MATRIX_BUCKET).key("matrix-absent").send().await) })
}

const RESTORE_KEY: &str = "archive";
const RESTORE_TIER: &str = "RESTORETIER";

fn restore_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(
            client
                .restore_object()
                .bucket(MATRIX_BUCKET)
                .key(RESTORE_KEY)
                .restore_request(RestoreRequest::builder().days(1).build())
                .send()
                .await,
        )
    })
}

pub(super) async fn seed_restore_object(bucket: &str, versioned: bool) -> (MockWarmBackend, Option<String>) {
    let backend = register_mock_tier(&global_tier_config_mgr(), RESTORE_TIER).await;
    let store = get_global_app_context().expect("embedded server context").object_store();
    let uploaded = store
        .put_object(
            bucket,
            RESTORE_KEY,
            &mut PutObjReader::from_vec(b"archived bytes".to_vec()),
            &ObjectOptions {
                versioned,
                ..Default::default()
            },
        )
        .await
        .expect("write restore fixture");
    let mut opts = ObjectOptions {
        version_id: uploaded.version_id.map(|version| version.to_string()),
        versioned,
        mod_time: uploaded.mod_time,
        ..Default::default()
    };
    opts.transition.status = "pending".to_owned();
    opts.transition.tier = RESTORE_TIER.to_owned();
    opts.transition.etag = uploaded.etag.unwrap_or_default();
    store
        .transition_object(bucket, RESTORE_KEY, &opts)
        .await
        .expect("transition restore fixture");
    let archived = store
        .get_object_info(bucket, RESTORE_KEY, &ObjectOptions::default())
        .await
        .expect("read transitioned fixture");
    assert_eq!(archived.transitioned_object.status, "complete");
    (backend, opts.version_id)
}

async fn restore_admits_once_and_renews_in_place(server: RustFSServer) {
    const BUCKET: &str = "e2-restore";
    create_bucket_in_the_store(BUCKET).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable fixture versioning");
    let (backend, version) = seed_restore_object(BUCKET, true).await;
    let version = version.expect("archive has a UUID version");
    client
        .put_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .body(ByteStream::from_static(b"current before restore"))
        .send()
        .await
        .expect("create a distinct current version before restore");
    let restore = |days| {
        client
            .restore_object()
            .bucket(BUCKET)
            .key(RESTORE_KEY)
            .version_id(&version)
            .restore_request(RestoreRequest::builder().days(days).build())
    };
    client.put_bucket_policy().bucket(BUCKET).policy(format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":"*","Action":"s3:RestoreObject","Resource":"arn:aws:s3:::{BUCKET}/{RESTORE_KEY}"}}]}}"#
    )).send().await.expect("set explicit RestoreObject deny");
    let denied = outcome(restore(1).send().await);
    let untouched = client
        .head_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .version_id(&version)
        .send()
        .await
        .expect("head denied fixture");
    assert_eq!(
        (denied.err().map(|error| (error.status, error.code)), untouched.restore()),
        (Some((Some(403), Some("AccessDenied".to_owned()))), None),
        "denied restore changed admission"
    );
    assert_eq!(backend.get_count().await, 0, "denial triggered a tier read");
    client
        .delete_bucket_policy()
        .bucket(BUCKET)
        .send()
        .await
        .expect("remove explicit deny");

    let get_barrier = backend.arm_get_barrier().await;
    let status = StatusCapture::default();
    let admitted = restore(1)
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("restore admission");
    assert_eq!(status.take(), Some(200));
    assert_eq!(admitted.request_charged().map(|value| value.as_str()), Some("requester"));
    assert_eq!(admitted.restore_output_path(), None);
    get_barrier.wait_until_paused().await;
    let conflict = restore(1).send().await.expect_err("duplicate ongoing restore");
    assert_eq!(refusal(&conflict), (Some("RestoreAlreadyInProgress".to_owned()), Some(409)));
    let current = client
        .put_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .body(ByteStream::from_static(b"intervening PUT"))
        .send()
        .await
        .expect("PUT while copy-back is reading the tier");
    let replacement_version = current.version_id().expect("replacement UUID version");
    get_barrier.release();

    let before = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let head = client
                .head_object()
                .bucket(BUCKET)
                .key(RESTORE_KEY)
                .version_id(&version)
                .send()
                .await
                .expect("head restoring object");
            if let Some(header) = head
                .restore()
                .filter(|header| header.starts_with("ongoing-request=\"false\""))
            {
                break header.to_owned();
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("wait for restored copy");
    let renewed = restore(2)
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("restore renewal");
    assert_eq!(status.take(), Some(200));
    assert_eq!(renewed.request_charged().map(|value| value.as_str()), Some("requester"));
    assert_eq!(renewed.restore_output_path(), None);
    let output = client
        .get_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .version_id(&version)
        .send()
        .await
        .expect("read renewed copy");
    assert!(
        output
            .restore()
            .is_some_and(|header| header.starts_with("ongoing-request=\"false\"") && header != before)
    );
    assert_eq!(
        output
            .body
            .collect()
            .await
            .expect("collect renewed copy")
            .into_bytes()
            .as_ref(),
        b"archived bytes"
    );
    assert_eq!(backend.get_count().await, 1, "renewal must reuse the local copy");
    let current = client
        .get_object()
        .bucket(BUCKET)
        .key(RESTORE_KEY)
        .send()
        .await
        .expect("read intervening PUT");
    assert_eq!(current.version_id(), Some(replacement_version));
    assert_eq!(current.restore(), None);
    assert_eq!(
        current
            .body
            .collect()
            .await
            .expect("collect intervening PUT")
            .into_bytes()
            .as_ref(),
        b"intervening PUT"
    );
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_admits_and_renews_restore_object() {
    with_stack(GATEWAY, restore_admits_once_and_renews_in_place);
}

#[test]
fn the_legacy_stack_admits_and_renews_restore_object() {
    with_stack(LEGACY, restore_admits_once_and_renews_in_place);
}

fn batch(keys: &[&str]) -> aws_sdk_s3::types::builders::DeleteBuilder {
    Delete::builder().set_objects(Some(
        keys.iter()
            .map(|key| ObjectIdentifier::builder().key(*key).build().expect("object key"))
            .collect(),
    ))
}

fn delete_objects<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let status = StatusCapture::default();
        let result = client
            .delete_objects()
            .bucket(MATRIX_BUCKET)
            .delete(batch(&["matrix-absent"]).build().unwrap())
            .customize()
            .interceptor(status.clone())
            .send()
            .await;
        let Ok(output) = result else {
            return outcome(result);
        };
        let response = status.take_response().expect("receive DeleteObjects response");
        assert_eq!(response.status, 200);
        if let [error] = output.errors() {
            assert_eq!(error.key(), Some("matrix-absent"));
            assert!(output.deleted().is_empty());
            return Err(super::Refusal {
                code: error.code().map(str::to_owned),
                status: Some(response.status),
                request_ids: response.request_ids,
            });
        }
        assert!(output.errors().is_empty(), "unexpected response: {output:?}");
        assert_eq!(output.deleted().len(), 1);
        assert_eq!(output.deleted()[0].key(), Some("matrix-absent"));
        Ok(())
    })
}

const DELETE_BUCKET: &str = "e2-delete-object";
/// A bucket name RustFS creates and the AWS grammar refuses (`sthree-` is reserved).
const RESERVED_NAME_BUCKET: &str = "sthree-e2-delete";

async fn put_in_the_store(bucket: &str, key: &str) {
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .put_object(bucket, key, &mut PutObjReader::from_vec(b"e2".to_vec()), &ObjectOptions::default())
        .await
        .expect("put object in the store");
}

async fn is_in_the_store(bucket: &str, key: &str) -> bool {
    match get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .get_object_info(bucket, key, &ObjectOptions::default())
        .await
    {
        Ok(_) => true,
        Err(error) if is_err_object_not_found(&error) => false,
        Err(error) => panic!("object lookup failed: {error}"),
    }
}

/// DeleteObject refuses whoever it must refuse, with the legacy code and before touching the
/// object, then deletes for root and answers the delete-marker header. A key with a NUL byte is
/// `400 InvalidArgument` and a key over 1024 bytes `400 KeyTooLongError` on both stacks, the
/// legacy answers; `nul_key_message` is the wording of the first where the stack's is pinned.
async fn delete_object_refuses_then_deletes(server: RustFSServer, nul_key_message: Option<&'static str>) {
    create_bucket_in_the_store(DELETE_BUCKET).await;
    create_bucket_in_the_store(RESERVED_NAME_BUCKET).await;
    create_user_without_a_policy(NO_POLICY_USER).await;
    put_in_the_store(DELETE_BUCKET, "doomed").await;
    put_in_the_store(RESERVED_NAME_BUCKET, "doomed").await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let no_policy = s3_client(&endpoint, NO_POLICY_USER, USER_SECRET_KEY);
    let anonymous = anonymous_client(&endpoint).await;

    for (caller, client) in [("anonymous", &anonymous), ("no policy", &no_policy)] {
        let error = client
            .delete_object()
            .bucket(DELETE_BUCKET)
            .key("doomed")
            .send()
            .await
            .expect_err(caller);
        assert_eq!(refusal(&error), (Some("AccessDenied".to_owned()), Some(403)), "{caller}");
    }
    assert!(is_in_the_store(DELETE_BUCKET, "doomed").await, "a refused delete removed the object");

    // Exercise a missing bucket.
    let error = no_policy
        .delete_object()
        .bucket("e2-no-such-bucket")
        .key("doomed")
        .send()
        .await
        .expect_err("missing bucket");
    assert_eq!(refusal(&error), (Some("NoSuchBucket".to_owned()), Some(404)));

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("doomed")
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "maybe");
        })
        .send()
        .await
        .expect_err("invalid force-delete header");
    assert_eq!(refusal(&error), (Some("InvalidRequest".to_owned()), Some(400)));
    assert!(is_in_the_store(DELETE_BUCKET, "doomed").await, "a refused delete removed the object");

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("bad\0key")
        .send()
        .await
        .expect_err("NUL key");
    assert_eq!(refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
    if let Some(message) = nul_key_message {
        assert_eq!(error.message(), Some(message));
    }

    let error = root
        .delete_object()
        .bucket(DELETE_BUCKET)
        .key("k".repeat(1025))
        .send()
        .await
        .expect_err("a key over 1024 bytes");
    assert_eq!(refusal(&error), (Some("KeyTooLongError".to_owned()), Some(400)));

    let status = StatusCapture::default();
    root.delete_object()
        .bucket(DELETE_BUCKET)
        .key("absent")
        .customize()
        .interceptor(status.clone())
        .send()
        .await
        .expect("delete absent object");
    assert_eq!(status.take(), Some(204), "no-op delete status");

    for bucket in [DELETE_BUCKET, RESERVED_NAME_BUCKET] {
        let output = root
            .delete_object()
            .bucket(bucket)
            .key("doomed")
            .send()
            .await
            .unwrap_or_else(|error| panic!("root deletes from {bucket}: {error:?}"));
        assert_eq!(output.delete_marker(), Some(false), "{bucket}");
        assert!(!is_in_the_store(bucket, "doomed").await, "{bucket}: the object survived its delete");
    }

    if nul_key_message.is_some() {
        const LOCK_BUCKET: &str = "e2-delete-locked";
        root.create_bucket()
            .bucket(LOCK_BUCKET)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await
            .expect("create lock bucket");
        for bucket in [DELETE_BUCKET, RESERVED_NAME_BUCKET, LOCK_BUCKET, "e2-no-such-bucket"] {
            for version in [None, Some("invalid-version")] {
                let too_long = root
                    .delete_object()
                    .bucket(bucket)
                    .key("k".repeat(1025))
                    .set_version_id(version.map(str::to_owned))
                    .send()
                    .await
                    .expect_err("key too long");
                assert_eq!(refusal(&too_long), (Some("KeyTooLongError".to_owned()), Some(400)));
                assert_eq!(too_long.message(), Some("Your key is too long."));
                let error = root
                    .delete_object()
                    .bucket(bucket)
                    .key("bad\0key")
                    .set_version_id(version.map(str::to_owned))
                    .send()
                    .await
                    .expect_err("invalid key");
                if bucket == "e2-no-such-bucket" {
                    assert_eq!(refusal(&error), (Some("NoSuchBucket".to_owned()), Some(404)));
                    assert_eq!(error.message(), Some("The specified bucket does not exist"));
                } else {
                    assert_eq!(refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
                    assert_eq!(error.message(), Some("Object key contains invalid control characters: \"bad\\0key\""));
                }
            }
            let invalid_version = root
                .delete_object()
                .bucket(bucket)
                .key("normal")
                .version_id("invalid-version")
                .send()
                .await
                .expect_err("invalid version");
            let expected = if bucket == "e2-no-such-bucket" {
                ("NoSuchBucket", 404, "The specified bucket does not exist")
            } else {
                ("InvalidArgument", 400, "Invalid argument")
            };
            assert_eq!(refusal(&invalid_version), (Some(expected.0.to_owned()), Some(expected.1)));
            assert_eq!(invalid_version.message(), Some(expected.2));
        }
    }
    server.shutdown().await;
}

/// The gateway words its own refusal of a NUL key; the code and the status are what the stacks share.
#[test]
fn the_gateway_stack_refuses_then_deletes_an_object() {
    with_stack(GATEWAY, |server| delete_object_refuses_then_deletes(server, None));
}

/// The legacy edge preserves the key validation result and error ordering.
#[test]
fn the_legacy_stack_refuses_then_deletes_an_object() {
    with_stack(LEGACY, |server| {
        delete_object_refuses_then_deletes(server, Some("Object key contains invalid control characters: \"bad\\0key\""))
    });
}

const SELECT_BUCKET: &str = "e2-select-object";
const SELECT_FIXTURE_KEY: &str = "input.csv";
const SELECT_FIXTURE: &[u8] = b"value\nold\n";
const SELECT_REQUEST: &str = concat!(
    "<SelectObjectContentRequest><Expression>SELECT * FROM S3Object</Expression><ExpressionType>SQL</ExpressionType>",
    "<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>",
    "<OutputSerialization><CSV/></OutputSerialization></SelectObjectContentRequest>"
);
const SELECT_SSE_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const SELECT_SSE_KEY: &str = "x-amz-server-side-encryption-customer-key";
const SELECT_SSE_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

pub(super) async fn seed_select_matrix() {
    let store = get_global_app_context().expect("embedded server context").object_store();
    store
        .put_object(
            MATRIX_BUCKET,
            SELECT_FIXTURE_KEY,
            &mut PutObjReader::from_vec(SELECT_FIXTURE.to_vec()),
            &ObjectOptions::default(),
        )
        .await
        .expect("store the matrix CSV");
}

fn select_object<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        outcome(
            client
                .select_object_content()
                .bucket(MATRIX_BUCKET)
                .key(SELECT_FIXTURE_KEY)
                .expression("SELECT * FROM S3Object")
                .expression_type(ExpressionType::Sql)
                .input_serialization(
                    InputSerialization::builder()
                        .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
                        .build(),
                )
                .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
                .send()
                .await,
        )
    })
}

async fn select_wire(
    server: &RustFSServer,
    key: &str,
    xml: &str,
    headers: &[(&str, &str)],
    caller: (&str, &str),
) -> (http::response::Parts, Bytes) {
    let path = format!("/{SELECT_BUCKET}/{key}?select&select-type=2");
    let uri: http::Uri = format!("{}{path}", server.endpoint()).parse().expect("Select URI");
    let mut request = http::Request::post(&uri)
        .header(http::header::HOST, uri.authority().expect("server authority").as_str())
        .header(http::header::CONTENT_TYPE, "application/xml")
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(Full::new(Bytes::copy_from_slice(xml.as_bytes())))
        .expect("Select request");
    let mut request =
        rustfs_signer::try_sign_v4(request, xml.len() as i64, caller.0, caller.1, "", "us-east-1").expect("sign Select request");
    *request.uri_mut() = path.parse().expect("Select request path");
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let stream = tokio::net::TcpStream::connect(server.address()).await.expect("connect");
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("HTTP connection");
        let transport = tokio::spawn(connection);
        let (parts, body) = sender.send_request(request).await.expect("Select response").into_parts();
        let bytes = body.collect().await.expect("Select response reaches EOF").to_bytes();
        drop(sender);
        transport.abort();
        let _ = transport.await;
        (parts, bytes)
    })
    .await
    .expect("Select response deadline")
}

fn assert_select_event<'a>(mut body: &'a [u8], kind: &str, expected_payload: &[u8]) -> &'a [u8] {
    let len = u32::from_be_bytes(body[..4].try_into().expect("frame length")) as usize;
    let headers_len = u32::from_be_bytes(body[4..8].try_into().expect("header length")) as usize;
    assert!(16 <= len && len <= body.len() && headers_len <= len - 16);
    let (frame, rest) = body.split_at(len);
    body = &frame[12..12 + headers_len];
    let mut headers = Vec::new();
    while !body.is_empty() {
        let key_len = usize::from(body[0]);
        assert_eq!(body[1 + key_len], 7, "string header");
        let value_len = u16::from_be_bytes(body[2 + key_len..4 + key_len].try_into().expect("value length")) as usize;
        let end = 4 + key_len + value_len;
        headers.push((&body[1..1 + key_len], &body[4 + key_len..end]));
        body = &body[end..];
    }
    let header = |name: &[u8]| headers.iter().find_map(|(key, value)| (*key == name).then_some(*value));
    if kind == "OverMaxRecordSize" {
        assert_eq!(header(b":message-type"), Some(b"error".as_slice()));
        assert_eq!(header(b":error-code"), Some(kind.as_bytes()));
        assert!(header(b":event-type").is_none());
        assert!(header(b":error-message").is_some_and(|message| !message.is_empty()));
    } else {
        assert_eq!(header(b":message-type"), Some(b"event".as_slice()));
        assert_eq!(header(b":event-type"), Some(kind.as_bytes()));
    }
    assert_eq!(&frame[12 + headers_len..len - 4], expected_payload);
    rest
}

async fn select_parity(server: RustFSServer) {
    create_bucket_in_the_store(SELECT_BUCKET).await;
    create_user_without_a_policy(NO_POLICY_USER).await;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let key = base64_simd::STANDARD.encode_to_string([0_u8; 32]);
    let md5 = "cLyPS3KoaSFGi/joRB3OUQ==";
    let mut oversized = b"value\n".to_vec();
    oversized.extend(std::iter::repeat_n(b'a', (1 << 20) + 1));
    oversized.push(b'\n');
    for (name, body) in [
        (SELECT_FIXTURE_KEY, ByteStream::from_static(SELECT_FIXTURE)),
        ("large.csv", ByteStream::from(oversized)),
    ] {
        client
            .put_object()
            .bucket(SELECT_BUCKET)
            .key(name)
            .body(body)
            .send()
            .await
            .expect("put CSV");
    }
    client
        .put_object()
        .bucket(SELECT_BUCKET)
        .key("encrypted.csv")
        .body(ByteStream::from_static(SELECT_FIXTURE))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key)
        .sse_customer_key_md5(md5)
        .send()
        .await
        .expect("put encrypted CSV");

    let root = (server.access_key(), server.secret_key());
    let request_headers = [
        (SELECT_SSE_ALGORITHM, "AES256"),
        (SELECT_SSE_KEY, key.as_str()),
        (SELECT_SSE_MD5, md5),
    ];
    for (name, sse) in [(SELECT_FIXTURE_KEY, false), ("encrypted.csv", true)] {
        let headers = if sse { request_headers.as_slice() } else { &[] };
        let (parts, body) = select_wire(&server, name, SELECT_REQUEST, headers, root).await;
        assert_eq!(parts.status, 200);
        assert_eq!(
            parts
                .headers
                .get(SELECT_SSE_ALGORITHM)
                .and_then(|header| header.to_str().ok()),
            sse.then_some("AES256")
        );
        assert_eq!(
            parts.headers.get(SELECT_SSE_MD5).and_then(|header| header.to_str().ok()),
            sse.then_some(md5)
        );
        assert!(!parts.headers.contains_key(SELECT_SSE_KEY));
        let rest = assert_select_event(&body, "Records", b"old\n");
        let rest = assert_select_event(
            rest,
            "Stats",
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Stats><BytesProcessed>10</BytesProcessed><BytesReturned>4</BytesReturned><BytesScanned>10</BytesScanned></Stats>",
        );
        assert!(assert_select_event(rest, "End", b"").is_empty());
    }

    let (parts, body) = select_wire(&server, "large.csv", SELECT_REQUEST, &[], root).await;
    assert_eq!(parts.status, 200);
    assert!(assert_select_event(&body, "OverMaxRecordSize", b"").is_empty());

    let empty_expression = SELECT_REQUEST.replace("SELECT * FROM S3Object", "");
    let whitespace_expression = SELECT_REQUEST.replace("SELECT * FROM S3Object", "   ");
    let invalid_range = SELECT_REQUEST.replace(
        "</SelectObjectContentRequest>",
        "<ScanRange><Start>999999</Start></ScanRange></SelectObjectContentRequest>",
    );
    for (name, xml, caller, status, code) in [
        ("encrypted.csv", SELECT_REQUEST, root, 400, "InvalidRequest"),
        (SELECT_FIXTURE_KEY, empty_expression.as_str(), root, 400, "ParseSelectFailure"),
        (SELECT_FIXTURE_KEY, whitespace_expression.as_str(), root, 400, "ParseSelectFailure"),
        (SELECT_FIXTURE_KEY, invalid_range.as_str(), root, 400, "InvalidRequestParameter"),
        (SELECT_FIXTURE_KEY, SELECT_REQUEST, (NO_POLICY_USER, USER_SECRET_KEY), 403, "AccessDenied"),
    ] {
        let (parts, body) = select_wire(&server, name, xml, &[], caller).await;
        assert_eq!(parts.status, status, "{code}: {}", String::from_utf8_lossy(&body));
        let body = std::str::from_utf8(&body).expect("Select error XML");
        assert!(body.contains(&format!("<Code>{code}</Code>")), "{body}");
    }
    server.shutdown().await;
}

#[test]
fn gateway_select_preserves_the_event_sequence_and_denies_unauthorized_reads() {
    with_stack(GATEWAY, select_parity);
}

#[test]
fn legacy_select_preserves_the_event_sequence_and_denies_unauthorized_reads() {
    with_stack(LEGACY, select_parity);
}

async fn delete_objects_checks_each_key_and_preserves_quiet_errors(server: RustFSServer) {
    const BUCKET: &str = "e2-delete-batch-scoped";
    const USER: &str = "e2deletebatchscoped";
    create_bucket_in_the_store(BUCKET).await;
    create_user_without_a_policy(USER).await;
    let policy = rustfs_policy::policy::Policy::parse_config(format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::{BUCKET}/allowed*"}}]}}"#
    ).as_bytes()).expect("parse batch policy");
    let iam = get_global_app_context().expect("embedded server context").iam().handle();
    iam.set_policy(USER, policy).await.expect("store batch policy");
    iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
        .await
        .expect("attach batch policy");

    let client = s3_client(&server.endpoint(), USER, USER_SECRET_KEY);
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for quiet in [false, true] {
        put_in_the_store(BUCKET, "allowed").await;
        put_in_the_store(BUCKET, "allowed-etag").await;
        put_in_the_store(BUCKET, "denied").await;
        let head = root
            .head_object()
            .bucket(BUCKET)
            .key("allowed-etag")
            .send()
            .await
            .expect("read fixture ETag");
        let objects = [
            ObjectIdentifier::builder().key("denied").build().unwrap(),
            ObjectIdentifier::builder().key("allowed").e_tag("*").build().unwrap(),
            ObjectIdentifier::builder().key("allowed-absent").build().unwrap(),
            ObjectIdentifier::builder().key("allowed-absent").build().unwrap(),
            ObjectIdentifier::builder()
                .key("allowed-etag")
                .e_tag(head.e_tag().expect("stored ETag"))
                .build()
                .unwrap(),
        ];
        let output = client
            .delete_objects()
            .bucket(BUCKET)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects.into()))
                    .quiet(quiet)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .expect("mixed DeleteObjects returns per-key outcomes");
        assert!(is_in_the_store(BUCKET, "denied").await, "key-level denial must preserve the object");
        assert!(
            is_in_the_store(BUCKET, "allowed-etag").await,
            "concrete ETag without GetObject must preserve the object"
        );
        assert!(
            !is_in_the_store(BUCKET, "allowed").await,
            "the wildcard needs DeleteObject but not GetObject"
        );
        let deleted: Vec<_> = output.deleted().iter().map(|object| object.key().unwrap()).collect();
        assert_eq!(
            deleted,
            if quiet {
                vec![]
            } else {
                vec!["allowed", "allowed-absent", "allowed-absent"]
            }
        );
        let errors: Vec<_> = output
            .errors()
            .iter()
            .map(|error| (error.key().unwrap(), error.code().unwrap()))
            .collect();
        assert_eq!(errors, [("denied", "AccessDenied"), ("allowed-etag", "AccessDenied")]);
    }
}

#[test]
fn the_gateway_stack_deletes_only_authorized_batch_entries() {
    with_stack(GATEWAY, delete_objects_checks_each_key_and_preserves_quiet_errors);
}

#[test]
fn the_legacy_stack_deletes_only_authorized_batch_entries() {
    with_stack(LEGACY, delete_objects_checks_each_key_and_preserves_quiet_errors);
}

#[test]
fn the_gateway_stack_echoes_rejected_entries_without_executing_them() {
    with_stack(GATEWAY, |server| async move {
        const BUCKET: &str = "e2-delete-batch-refusals";
        create_bucket_in_the_store(BUCKET).await;
        for key in ["kept", "conditional", "deleted"] {
            put_in_the_store(BUCKET, key).await;
        }
        let oversized_key = format!("{}kept", "/".repeat(1025));
        let objects = [
            ObjectIdentifier::builder().key("").build().unwrap(),
            ObjectIdentifier::builder().key(&oversized_key).build().unwrap(),
            ObjectIdentifier::builder().key("conditional").e_tag("\"\"").build().unwrap(),
            ObjectIdentifier::builder().key("deleted").build().unwrap(),
        ];
        let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        let output = root
            .delete_objects()
            .bucket(BUCKET)
            .delete(Delete::builder().set_objects(Some(objects.into())).build().unwrap())
            .send()
            .await
            .expect("the independent entry still succeeds");

        assert_eq!(output.deleted().len(), 1);
        assert_eq!(output.deleted()[0].key(), Some("deleted"));
        let errors: Vec<_> = output.errors().iter().map(|error| (error.key(), error.code())).collect();
        assert_eq!(
            errors,
            [
                (Some(""), Some("InvalidArgument")),
                (Some(oversized_key.as_str()), Some("InvalidArgument")),
                (Some("conditional"), Some("InvalidArgument")),
            ],
        );
        assert!(is_in_the_store(BUCKET, "kept").await);
        assert!(is_in_the_store(BUCKET, "conditional").await);
        assert!(!is_in_the_store(BUCKET, "deleted").await);
    });
}
