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

//! Test-only assembly of verified POST preflight and shared member authorization.
//! The production gateway handler remains closed for archive extraction.

use super::*;
use crate::app::object::post_body::{
    PostBodyIntegrity, VerifiedPostArchivePolicy, stage_post_archive_for_test, with_archive_resources_for_test,
};
use crate::app::object::post_headers::{post_archive_checksum_headers, post_archive_content_md5};
use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput};
use crate::storage_api::server::http::access::access_owned::object_write::{
    PostObjectFormPolicy, PostObjectPolicyFields, PutObjectAuthorization, authorize_post_object, authorize_put_object,
};
use crate::storage_api::server::http::access::test_support::{
    BucketOperations as _, MakeBucketOptions, ObjectIO as _, ObjectOperations as _, StoragePutObjReader, is_err_object_not_found,
};
use bytes::Bytes;
use hmac::{Hmac, Mac as _};
use http::{HeaderMap, Request, StatusCode};
use http_body_util::{BodyExt as _, Full};
use rustfs_gateway::{Credentials, HandlerError, RegionSet, SigV4Authenticator, StaticCredentials, allow_when, dto};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt as _;

const ACCESS: &str = "archive-snapshot-iam-writer";
const SECRET: &str = "archive-snapshot-iam-secret";
const BUCKET: &str = "archive-snapshot-iam-bucket";
const BOUNDARY: &str = "archive-iam-diagnostic";

struct Probe {
    server: Arc<ServerContextSlot>,
    calls: AtomicUsize,
    authorized: AtomicUsize,
    preflighted: AtomicUsize,
    committed: AtomicUsize,
}

async fn authorize_form(
    envelope: &mut RequestEnvelope,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
    metadata: &[(String, String)],
) -> Result<(), HandlerError> {
    let form = PostObjectFormPolicy::new(PostObjectPolicyFields::default())
        .and_then(|form| form.with_metadata(metadata))
        .map_err(|error| handler_error(error.into()))?;
    let facts = PutObjectAuthorization {
        bucket,
        key,
        version_id: None,
        retain_until: None,
        specific_etag: false,
        legal_hold: false,
        retention: false,
        post: true,
    };
    authorize_put_object(envelope, server, facts, |envelope| {
        form.install_gateway(envelope);
        Ok(())
    })
    .await
    .map_err(|error| handler_error(error.into()))?;
    authorize_post_object(envelope, bucket, key, None)
        .await
        .map_err(|error| handler_error(error.into()))
}

impl Handler<dto::PostObject> for Probe {
    async fn call(&self, request: Req<dto::PostObject>) -> HandlerResult<dto::PostObject> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let policy = VerifiedPostArchivePolicy::from_request(&request)
            .ok_or_else(|| HandlerError::internal_error("verified archive policy is missing"))?;
        let mut envelope = access::admit(&self.server, request.context()).await.map_err(handler_error)?;
        envelope
            .bind_gateway_post_archive_identity()
            .ok_or_else(|| HandlerError::internal_error("archive request identity is missing"))?;
        let input = request.into_input();
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        authorize_form(&mut envelope, &self.server, &bucket, &key, &input.metadata).await?;
        self.authorized.fetch_add(1, Ordering::SeqCst);
        let generation = envelope
            .take_bucket_generation()
            .ok_or_else(|| HandlerError::internal_error("archive generation is missing"))?;
        envelope.release_table_publication_guards();
        let checksum_headers = post_archive_checksum_headers(&envelope, &input.fields)
            .map_err(handler_error)?
            .unwrap_or_else(|| envelope.headers().clone());
        let md5 = post_archive_content_md5(&envelope, &input.fields).map_err(handler_error)?;
        let integrity = PostBodyIntegrity::for_archive(
            checksum_headers,
            md5,
            crate::app::storage_api::object_usecase::options::get_content_sha256_with_query(
                envelope.headers(),
                envelope.uri().query(),
            ),
        );
        let archive = stage_post_archive_for_test(&envelope, &bucket, &key, input.body.into_body(), integrity, policy)
            .await
            .map_err(handler_error)?;
        self.preflighted.fetch_add(1, Ordering::SeqCst);
        authorize_form(&mut envelope, &self.server, &bucket, &key, &input.metadata).await?;
        if envelope.bucket_generation() != Some(&generation) {
            return Err(HandlerError::internal_error("archive bucket generation changed"));
        }
        let write = PutObjectWriteInput {
            bucket,
            key,
            post_archive: Some(archive),
            ..Default::default()
        };
        DefaultObjectUsecase::with_context(self.server.app_context())
            .execute_put_object_extract_owned(envelope, write)
            .await
            .map_err(handler_error)?;
        self.committed.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(dto::PostObjectOutput::default()))
    }
}

fn mac(key: &[u8], message: &str) -> Vec<u8> {
    let mut hmac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(key).expect("HMAC key");
    hmac.update(message.as_bytes());
    hmac.finalize().into_bytes().to_vec()
}

fn signed_form(archive: &[u8], bad_checksum: bool, tampered: bool) -> Vec<u8> {
    let format =
        time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z").expect("date format");
    let stamp = time::OffsetDateTime::now_utc().format(&format).expect("current date");
    let day = &stamp[..8];
    let credential = format!("{ACCESS}/{day}/us-east-1/s3/aws4_request");
    let sha = if bad_checksum {
        base64_simd::STANDARD.encode_to_string([0u8; 32])
    } else {
        base64_simd::STANDARD.encode_to_string(Sha256::digest(archive))
    };
    let md5 = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(archive));
    let policy = serde_json::json!({
        "expiration":"2099-01-02T00:00:00Z",
        "conditions":[
            {"bucket":BUCKET}, ["starts-with","$key","allowed/"], {"x-amz-algorithm":"AWS4-HMAC-SHA256"},
            {"x-amz-credential":credential}, {"x-amz-date":stamp},
            {"x-amz-checksum-sha256":sha}, {"content-md5":md5},
            {"x-amz-meta-snowball-auto-extract":"true"}
        ]
    });
    let encoded = base64_simd::STANDARD.encode_to_string(policy.to_string().as_bytes());
    let mut signing_key = format!("AWS4{SECRET}").into_bytes();
    for part in [day, "us-east-1", "s3", "aws4_request"] {
        signing_key = mac(&signing_key, part);
    }
    let signature = if tampered {
        "0".repeat(64)
    } else {
        hex_simd::encode_to_string(mac(&signing_key, &encoded), hex_simd::AsciiCase::Lower)
    };
    let mut body = Vec::new();
    for (name, value) in [
        ("bucket", BUCKET),
        ("key", "allowed/archive.tar"),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", credential.as_str()),
        ("x-amz-date", stamp.as_str()),
        ("x-amz-checksum-sha256", sha.as_str()),
        ("content-md5", md5.as_str()),
        ("x-amz-meta-snowball-auto-extract", "true"),
        ("policy", encoded.as_str()),
        ("x-amz-signature", signature.as_str()),
    ] {
        body.extend_from_slice(
            format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"archive.tar\"\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(archive);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn archive(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut builder = tokio_tar::Builder::new(Vec::new());
    for &(key, data) in members {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(u64::try_from(data.len()).expect("fixture size"));
        header.set_cksum();
        builder
            .append_data(&mut header, key, std::io::Cursor::new(data))
            .await
            .expect("append member");
    }
    builder.into_inner().await.expect("finish archive")
}

#[tokio::test]
#[serial_test::serial]
async fn signed_snapshot_preflight_and_member_iam_gate_real_storage() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
    let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
    assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: "archive-diagnostic-root".to_owned(),
        secret_key: "archive-diagnostic-root-secret".to_owned(),
        status: "on".to_owned(),
        ..Default::default()
    }));
    let server = ServerContextSlot::new();
    assert!(server.install(Arc::clone(&context)));
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create diagnostic bucket");
    let iam = context.iam().handle();
    iam.create_user(
        ACCESS,
        &rustfs_madmin::AddOrUpdateUserReq {
            secret_key: SECRET.to_owned(),
            policy: None,
            status: rustfs_madmin::AccountStatus::Enabled,
        },
    )
    .await
    .expect("create diagnostic user");
    let document = serde_json::json!({
        "Version":"2012-10-17",
        "Statement":[
            {"Effect":"Allow","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{BUCKET}/allowed/*")},
            {"Effect":"Deny","Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{BUCKET}/allowed/deny*")}
        ]
    });
    let policy = rustfs_policy::policy::Policy::parse_config(document.to_string().as_bytes()).expect("diagnostic IAM policy");
    iam.set_policy(ACCESS, policy).await.expect("store diagnostic policy");
    iam.policy_db_set(ACCESS, rustfs_iam::store::UserType::Reg, false, ACCESS)
        .await
        .expect("attach diagnostic policy");
    for key in ["allowed/protected", "allowed/deny-existing"] {
        let mut data = StoragePutObjReader::from_vec(b"original".to_vec());
        store
            .put_object(BUCKET, key, &mut data, &Default::default())
            .await
            .expect("seed protected object");
    }
    let probe = Arc::new(Probe {
        server,
        calls: AtomicUsize::new(0),
        authorized: AtomicUsize::new(0),
        preflighted: AtomicUsize::new(0),
        committed: AtomicUsize::new(0),
    });
    let credentials = StaticCredentials::new().with(Credentials::new(ACCESS, SECRET.as_bytes()).expect("credentials"));
    let service = ServiceBuilder::new()
        .rustfs_profile()
        .authenticator(
            SigV4Authenticator::new(Arc::new(credentials), RegionSet::new(["us-east-1"]).expect("region")).rustfs_profile(),
        )
        .authorizer(allow_when(|request| request.operation == "PostObject"))
        .register::<dto::PostObject, _>(Arc::clone(&probe))
        .build()
        .expect("diagnostic gateway");
    let payload = vec![42; 2 * 64 * 1024];
    Box::pin(async {
        for (case, members, bad_checksum, tampered) in [
            (
                "policy",
                vec![
                    ("allowed/protected", b"replacement".as_slice()),
                    ("outside/denied", payload.as_slice()),
                ],
                false,
                false,
            ),
            (
                "iam",
                vec![
                    ("allowed/deny-existing", payload.as_slice()),
                    ("allowed/after-deny", b"other".as_slice()),
                ],
                false,
                false,
            ),
            ("success", vec![("allowed/success", payload.as_slice())], false, false),
            ("checksum", vec![("allowed/protected", payload.as_slice())], true, false),
            ("signature", vec![("allowed/protected", payload.as_slice())], false, true),
        ] {
            let archive = archive(&members).await;
            let form = signed_form(&archive, bad_checksum, tampered);
            let calls = probe.calls.load(Ordering::SeqCst);
            let authorized = probe.authorized.load(Ordering::SeqCst);
            let preflighted = probe.preflighted.load(Ordering::SeqCst);
            let committed = probe.committed.load(Ordering::SeqCst);
            let request = Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}"))
                .header("host", "localhost")
                .header("content-type", format!("multipart/form-data; boundary={BOUNDARY}"))
                .header("content-length", form.len())
                .body(Full::new(Bytes::from(form)))
                .expect("signed diagnostic request");
            let response = tower::Service::call(&mut service.clone(), request)
                .await
                .expect("gateway adapter is infallible");
            let status = response.status();
            let body = response.into_body().collect().await.expect("response body").to_bytes();
            let text = String::from_utf8_lossy(&body);
            assert_eq!(
                probe.calls.load(Ordering::SeqCst),
                calls + usize::from(!tampered),
                "{case}: only a verified signature may reach the handler"
            );
            assert_eq!(
                probe.authorized.load(Ordering::SeqCst),
                authorized + usize::from(!tampered),
                "{case}: the outer authorization must be exercised: {status} {text}"
            );
            assert_eq!(
                probe.preflighted.load(Ordering::SeqCst),
                preflighted + usize::from(matches!(case, "iam" | "success")),
                "{case}: only the complete authorized key set may reach member IAM: {status} {text}"
            );
            assert_eq!(
                probe.committed.load(Ordering::SeqCst),
                committed + usize::from(case == "success"),
                "{case}: only a fully admitted archive may complete"
            );
            if case == "success" {
                assert_eq!(status, StatusCode::NO_CONTENT, "{case}: {text}");
                let mut reader = store
                    .get_object_reader(BUCKET, "allowed/success", None, HeaderMap::new(), &Default::default())
                    .await
                    .expect("successful member");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("read member");
                assert_eq!(actual, payload, "the stored member must come from the preflighted snapshot");
            } else {
                let expected = match case {
                    "policy" | "iam" => "AccessDenied",
                    "checksum" => "BadDigest",
                    _ => "SignatureDoesNotMatch",
                };
                assert!(!status.is_success(), "{case}: {text}");
                assert!(text.contains(&format!("<Code>{expected}</Code>")), "{case}: {text}");
                for key in ["allowed/protected", "allowed/deny-existing"] {
                    let mut reader = store
                        .get_object_reader(BUCKET, key, None, HeaderMap::new(), &Default::default())
                        .await
                        .expect("protected object");
                    let mut actual = Vec::new();
                    reader.stream.read_to_end(&mut actual).await.expect("read protected object");
                    assert_eq!(actual, b"original", "{case}: {key} must not be overwritten");
                }
                for key in ["outside/denied", "allowed/after-deny"] {
                    let error = match store.get_object_info(BUCKET, key, &Default::default()).await {
                        Ok(_) => panic!("{case}: {key} must not be published"),
                        Err(error) => error,
                    };
                    assert!(is_err_object_not_found(&error), "{case}: {key}: {error}");
                }
            }
        }
    })
    .await;

    // Drive the production POST handler with a task-local test budget; deployed
    // archive admission remains closed until its resource gate is verified.
    Box::pin(async {
        let backend = Arc::new(GatewayBackend::new(Arc::clone(&probe.server)));
        let credentials = StaticCredentials::new().with(Credentials::new(ACCESS, SECRET.as_bytes()).expect("credentials"));
        let production = ServiceBuilder::new()
            .rustfs_profile()
            .authenticator(
                SigV4Authenticator::new(Arc::new(credentials), RegionSet::new(["us-east-1"]).expect("region")).rustfs_profile(),
            )
            .authorizer(allow_when(|request| request.operation == "PostObject"))
            .register_post_object_after_authorization(backend)
            .build()
            .expect("production handler gateway");
        for (case, members, bad_checksum, tampered) in [
            (
                "policy",
                vec![
                    ("allowed/protected", b"replacement".as_slice()),
                    ("outside/live", b"x".as_slice()),
                ],
                false,
                false,
            ),
            ("iam", vec![("allowed/deny-existing", payload.as_slice())], false, false),
            ("success", vec![("allowed/live-success", payload.as_slice())], false, false),
            ("checksum", vec![("allowed/protected", payload.as_slice())], true, false),
            ("signature", vec![("allowed/protected", payload.as_slice())], false, true),
        ] {
            let archive = archive(&members).await;
            let form = signed_form(&archive, bad_checksum, tampered);
            let request = Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}"))
                .header("host", "localhost")
                .header("content-type", format!("multipart/form-data; boundary={BOUNDARY}"))
                .header("content-length", form.len())
                .body(Full::new(Bytes::from(form)))
                .expect("signed production request");
            let (status, body) = with_archive_resources_for_test(async {
                let response = tower::Service::call(&mut production.clone(), request)
                    .await
                    .expect("gateway adapter is infallible");
                let status = response.status();
                let body = response.into_body().collect().await.expect("response body").to_bytes();
                (status, body)
            })
            .await
            .expect("test admission resources");
            let text = String::from_utf8_lossy(&body);
            if case == "success" {
                assert_eq!(status, StatusCode::NO_CONTENT, "{case}: {text}");
                let mut reader = store
                    .get_object_reader(BUCKET, "allowed/live-success", None, HeaderMap::new(), &Default::default())
                    .await
                    .expect("production member");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("read production member");
                assert_eq!(actual, payload);
            } else {
                let expected = match case {
                    "policy" | "iam" => "AccessDenied",
                    "checksum" => "BadDigest",
                    _ => "SignatureDoesNotMatch",
                };
                assert!(!status.is_success(), "{case}: {text}");
                assert!(text.contains(&format!("<Code>{expected}</Code>")), "{case}: {text}");
                for key in ["allowed/protected", "allowed/deny-existing"] {
                    let mut reader = store
                        .get_object_reader(BUCKET, key, None, HeaderMap::new(), &Default::default())
                        .await
                        .expect("protected object");
                    let mut actual = Vec::new();
                    reader.stream.read_to_end(&mut actual).await.expect("read protected object");
                    assert_eq!(actual, b"original", "{case}: {key} must not be overwritten");
                }
                let missing = store
                    .get_object_info(BUCKET, "outside/live", &Default::default())
                    .await
                    .expect_err("outside member must not be written");
                assert!(is_err_object_not_found(&missing));
            }
        }
    })
    .await;
}
