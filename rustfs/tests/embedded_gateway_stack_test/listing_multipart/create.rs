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

//! CreateMultipartUpload wire replies and durable session facts.

use super::super::{GATEWAY, LEGACY, create_bucket_in_the_store, s3_client, with_stack};
use aws_sdk_s3::config::interceptors::AfterDeserializationInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::{BoxError, ProvideErrorMetadata};
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_storage_api::MultipartOperations as _;
use std::sync::{Arc, Mutex};

const BUCKET: &str = "create-multipart-wire";
const LOCK_BUCKET: &str = "create-multipart-locked";
const RESERVED_BUCKET: &str = "sthree-multipart";
const STORAGE_CLASS: &str = "x-amz-storage-class";
const LEGAL_HOLD: &str = "x-amz-object-lock-legal-hold";
const LOCK_MODE: &str = "x-amz-object-lock-mode";
const RETAIN_UNTIL: &str = "x-amz-object-lock-retain-until-date";
const DATE: &str = "2030-01-01T00:00:00.123456789Z";

struct Case {
    name: &'static str,
    bucket: &'static str,
    key: &'static str,
    headers: &'static [(&'static str, &'static str)],
}

const CASES: &[Case] = &[
    Case {
        name: "plain",
        bucket: BUCKET,
        key: "plain",
        headers: &[
            (STORAGE_CLASS, "STANDARD"),
            ("content-type", "application/x-multipart-fixture"),
            ("x-amz-meta-content-type", "user-content-type"),
            ("x-amz-tagging", "project=rustfs"),
            ("x-amz-checksum-algorithm", "CRC64NVME"),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ],
    },
    Case {
        name: "reserved",
        bucket: RESERVED_BUCKET,
        key: "a%252Fb%20%2B%26%3C%E9%9B%AA",
        headers: &[("x-amz-acl", "unknown-acl"), ("x-amz-request-payer", "unknown-payer")],
    },
    Case {
        name: "backslash",
        bucket: BUCKET,
        key: "a%5Cb",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "tab",
        bucket: BUCKET,
        key: "a%09%20b",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "double-slash",
        bucket: BUCKET,
        key: "dir//x",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "dot-dot",
        bucket: BUCKET,
        key: "dir/../x",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "internal-name",
        bucket: BUCKET,
        key: "xl.meta",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "control",
        bucket: BUCKET,
        key: "a%01b",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "empty",
        bucket: BUCKET,
        key: "",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "nul",
        bucket: BUCKET,
        key: "a%00b",
        headers: &[(STORAGE_CLASS, "unknown-class")],
    },
    Case {
        name: "unknown-sse",
        bucket: BUCKET,
        key: "sse",
        headers: &[("x-amz-server-side-encryption", "unknown-sse")],
    },
    Case {
        name: "unlocked",
        bucket: BUCKET,
        key: "lock",
        headers: &[(LEGAL_HOLD, "unknown-hold")],
    },
    Case {
        name: "lock-mode-only",
        bucket: LOCK_BUCKET,
        key: "missing-date",
        headers: &[(LOCK_MODE, "unknown-mode")],
    },
    Case {
        name: "lock-mode-with-date",
        bucket: LOCK_BUCKET,
        key: "invalid-mode",
        headers: &[(LOCK_MODE, "unknown-mode"), (RETAIN_UNTIL, DATE)],
    },
    Case {
        name: "hold-with-date",
        bucket: LOCK_BUCKET,
        key: "invalid-hold",
        headers: &[(LEGAL_HOLD, "unknown-hold"), (LOCK_MODE, "GOVERNANCE"), (RETAIN_UNTIL, DATE)],
    },
    Case {
        name: "class-before-lock",
        bucket: LOCK_BUCKET,
        key: "invalid-class",
        headers: &[(STORAGE_CLASS, "unknown-class"), (LEGAL_HOLD, "unknown-hold")],
    },
    Case {
        name: "utc-date",
        bucket: LOCK_BUCKET,
        key: "utc-date",
        headers: &[(LOCK_MODE, "GOVERNANCE"), (RETAIN_UNTIL, DATE)],
    },
    Case {
        name: "offset-date",
        bucket: LOCK_BUCKET,
        key: "offset-date",
        headers: &[
            (LOCK_MODE, "GOVERNANCE"),
            (RETAIN_UNTIL, "2030-01-01T08:00:00.123456789+08:00"),
        ],
    },
    Case {
        name: "invalid-checksum",
        bucket: BUCKET,
        key: "invalid-checksum",
        headers: &[("x-amz-checksum-algorithm", "SHA256"), ("x-amz-checksum-type", "FULL_OBJECT")],
    },
];

#[derive(Clone, Debug, Default)]
struct SuccessWire(Arc<Mutex<Option<(u16, String)>>>);

impl Intercept for SuccessWire {
    fn name(&self) -> &'static str {
        "create-multipart-response"
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            assert!(response.headers().get("x-amz-server-side-encryption-customer-key").is_none());
            let xml = std::str::from_utf8(response.body().bytes().expect("buffered CreateMultipartUpload XML"))?;
            *self.0.lock().expect("wire capture") = Some((status, xml.to_owned()));
        }
        Ok(())
    }
}

fn expected_refusal(name: &str, legacy: bool) -> Option<(u16, &'static str, &'static str)> {
    const STORAGE: (u16, &str, &str) = (400, "InvalidStorageClass", "The storage class you specified is not valid.");
    const LOCK_XML: (u16, &str, &str) = (
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    );
    Some(match name {
        "plain" | "reserved" | "utc-date" | "offset-date" => return None,
        "backslash" | "tab" | "class-before-lock" => STORAGE,
        "double-slash" | "dot-dot" => (400, "InvalidArgument", "Invalid object key"),
        "internal-name" => (400, "InvalidArgument", "Object key contains a reserved storage filename"),
        "control" => (400, "InvalidArgument", "Object key contains characters unsupported by XML listings"),
        "empty" => (501, "NotImplemented", "Unknown operation"),
        "nul" if !legacy => (400, "InvalidArgument", "the name contains a NUL byte"),
        "nul" => (400, "InvalidArgument", "Object key contains invalid control characters: \"a\\0b\""),
        "unknown-sse" if !legacy => (
            400,
            "InvalidArgument",
            "x-amz-server-side-encryption must be one of AES256, aws:fsx, aws:kms or aws:kms:dsse",
        ),
        "unknown-sse" => (
            400,
            "InvalidArgument",
            "The SSE algorithm specified is not supported. The valid values are AES256 or aws:kms.",
        ),
        "unlocked" => (400, "InvalidRequest", "Bucket is missing ObjectLockConfiguration"),
        "lock-mode-only" => (
            400,
            "InvalidRequest",
            "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
        ),
        "lock-mode-with-date" | "hold-with-date" => LOCK_XML,
        "invalid-checksum" => (400, "InvalidArgument", "Invalid checksum type"),
        other => panic!("no measured answer for {other}"),
    })
}

async fn create_multipart_wire(server: RustFSServer, legacy: bool) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for bucket in [BUCKET, RESERVED_BUCKET] {
        create_bucket_in_the_store(bucket).await;
    }
    client
        .create_bucket()
        .bucket(LOCK_BUCKET)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .expect("create lock bucket");
    let context = get_global_app_context().expect("embedded context");
    let store = context.object_store();
    let mut expected = Vec::new();
    let mut observed = Vec::new();
    for case in CASES {
        if !legacy && case.name == "empty" {
            // The empty path does not dispatch CreateMultipartUpload.
            continue;
        }
        let uploads = || store.list_multipart_uploads(case.bucket, "", None, None, None, 100);
        let before = uploads().await.expect("list before request").uploads.len();
        let wire = SuccessWire::default();
        let result = client
            .create_multipart_upload()
            .bucket(case.bucket)
            .key("fixture")
            .customize()
            .interceptor(wire.clone())
            .mutate_request(move |request| {
                let (prefix, _) = request.uri().rsplit_once("/fixture").expect("SDK fixture path");
                request
                    .set_uri(format!("{prefix}/{}?uploads", case.key))
                    .expect("fixture request target");
                for (name, value) in case.headers {
                    request.headers_mut().insert(*name, *value);
                }
            })
            .send()
            .await;
        let after = uploads().await.expect("list after request").uploads.len();
        expected.push((
            case.name,
            expected_refusal(case.name, legacy)
                .map(|(status, code, message)| (status, Some(code.to_owned()), Some(message.to_owned()))),
        ));
        let output = match result {
            Ok(output) => {
                observed.push((case.name, None));
                output
            }
            Err(error) => {
                assert_eq!(after, before, "{}", case.name);
                let response = error.raw_response().expect("wire response");
                observed.push((
                    case.name,
                    Some((
                        response.status().as_u16(),
                        error.code().map(str::to_owned),
                        error.message().map(str::to_owned),
                    )),
                ));
                for name in ["x-amz-request-id", "x-request-id"] {
                    assert!(
                        response.headers().get(name).is_some_and(|value| !value.is_empty()),
                        "{name}: {}",
                        case.name
                    );
                }
                continue;
            }
        };
        let (status, xml) = wire.0.lock().expect("wire capture").take().expect("success XML");
        assert_eq!(status, 200);
        assert!(xml.contains("<InitiateMultipartUploadResult") && xml.contains("</InitiateMultipartUploadResult>"));
        let key = output.key().expect("response key");
        let upload_id = output.upload_id().filter(|id| !id.is_empty()).expect("nonempty upload ID");
        assert_eq!(output.bucket(), Some(case.bucket));
        assert!(xml.contains(&format!("<UploadId>{upload_id}</UploadId>")));
        assert_eq!(after, before + 1, "{}", case.name);
        let info = store
            .get_multipart_info(case.bucket, key, upload_id, &Default::default())
            .await
            .expect("get durable upload metadata");
        let metadata = |name| info.user_defined.get(name).map(String::as_str);
        match case.name {
            "plain" => {
                assert_eq!(key, "plain");
                assert_eq!(metadata("content-type"), Some("application/x-multipart-fixture"));
                assert_eq!(metadata("x-amz-meta-content-type"), Some("user-content-type"));
                assert_eq!(metadata("x-amz-tagging"), Some("project=rustfs"));
                assert_eq!(metadata("x-rustfs-multipart-checksum"), Some("CRC64NVME"));
                assert_eq!(metadata("x-rustfs-multipart-checksum-type"), Some("FULL_OBJECT"));
                assert_eq!(output.checksum_algorithm().map(|value| value.as_str()), Some("CRC64NVME"));
                assert_eq!(output.checksum_type().map(|value| value.as_str()), Some("FULL_OBJECT"));
            }
            "reserved" => {
                assert_eq!(key, "a%2Fb +&<雪");
                assert_eq!(metadata("content-type"), Some("binary/octet-stream"));
                assert_eq!(output.checksum_algorithm(), None);
            }
            "utc-date" | "offset-date" => {
                use time::{OffsetDateTime, format_description::well_known::Rfc3339};
                assert_eq!(key, case.key);
                assert_eq!(metadata(LOCK_MODE), Some("GOVERNANCE"));
                let date = metadata(RETAIN_UNTIL).expect("durable retention date");
                let requested = case.headers.iter().find(|(name, _)| *name == RETAIN_UNTIL).unwrap().1;
                if legacy {
                    assert_eq!(date, requested);
                }
                let nanos = |value| OffsetDateTime::parse(value, &Rfc3339).unwrap().unix_timestamp_nanos();
                assert_eq!(nanos(date), nanos(requested));
            }
            other if expected_refusal(other, legacy).is_some() => {}
            other => panic!("no durable assertion for {other}"),
        }
    }
    assert_eq!(observed, expected);
    server.shutdown().await;
}

#[test]
fn create_multipart_baseline_legacy_wire() {
    with_stack(LEGACY, |server| create_multipart_wire(server, true));
}

#[test]
fn create_multipart_wire_gateway() {
    with_stack(GATEWAY, |server| create_multipart_wire(server, false));
}

async fn create_multipart_authorization(server: RustFSServer) {
    use super::super::{USER_SECRET_KEY, create_user_without_a_policy};
    use aws_sdk_s3::Client;
    use rustfs_iam::store::UserType;
    use rustfs_policy::policy::Policy;
    use serde_json::json;

    const PUT_ONLY: &str = "multipart-put-only";
    const FULL_ACCESS: &str = "multipart-full-access";
    const NO_POLICY: &str = "multipart-no-policy";
    const TARGET: &str = "target";
    const VERSION: &str = "0194e0f1-0000-7000-8000-000000000000";
    const REFUSED_VERSION: &str = "0194e0f1-0000-7000-8000-000000000001";
    const REPLICATION: &str = "x-rustfs-source-replication-request";

    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    root.create_bucket()
        .bucket(LOCK_BUCKET)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .expect("create the Object Lock bucket");
    create_bucket_in_the_store(BUCKET).await;

    let context = get_global_app_context().expect("embedded context");
    let store = context.object_store();
    let iam = context.iam().handle();
    for (user, actions) in [
        (PUT_ONLY, &["s3:PutObject"][..]),
        (
            FULL_ACCESS,
            &[
                "s3:PutObject",
                "s3:ReplicateObject",
                "s3:PutObjectLegalHold",
                "s3:PutObjectRetention",
            ][..],
        ),
    ] {
        create_user_without_a_policy(user).await;
        let resource = format!("arn:aws:s3:::{LOCK_BUCKET}/{TARGET}");
        let policy = json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":actions,"Resource":resource},
            {"Effect":"Deny","Action":["s3:PutObject"],"Resource":resource,
             "Condition":{"StringEquals":{"s3:VersionId":REFUSED_VERSION}}}
        ]});
        let policy = Policy::parse_config(policy.to_string().as_bytes()).expect("valid test policy");
        iam.set_policy(user, policy).await.expect("store the test policy");
        iam.policy_db_set(user, UserType::Reg, false, user)
            .await
            .expect("attach the test policy");
    }
    create_user_without_a_policy(NO_POLICY).await;
    let put_only = s3_client(&endpoint, PUT_ONLY, USER_SECRET_KEY);
    let full = s3_client(&endpoint, FULL_ACCESS, USER_SECRET_KEY);
    let no_policy = s3_client(&endpoint, NO_POLICY, USER_SECRET_KEY);

    struct Attempt<'a> {
        reason: &'static str,
        client: &'a Client,
        bucket: &'static str,
        key: &'static str,
        version: Option<&'static str>,
        headers: &'static [(&'static str, &'static str)],
        refusal: Option<(&'static str, u16)>,
    }

    let denied = Some(("AccessDenied", 403));
    for row in [
        Attempt {
            reason: "no PutObject permission",
            client: &no_policy,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[],
            refusal: denied,
        },
        Attempt {
            reason: "wrong bucket",
            client: &put_only,
            bucket: BUCKET,
            key: TARGET,
            version: None,
            headers: &[],
            refusal: denied,
        },
        Attempt {
            reason: "wrong key",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: "other",
            version: None,
            headers: &[],
            refusal: denied,
        },
        Attempt {
            reason: "no ReplicateObject permission",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[(REPLICATION, "true")],
            refusal: denied,
        },
        Attempt {
            reason: "no PutObjectLegalHold permission",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[(LEGAL_HOLD, "OFF")],
            refusal: denied,
        },
        Attempt {
            reason: "no PutObjectRetention permission",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[(LOCK_MODE, "GOVERNANCE"), (RETAIN_UNTIL, DATE)],
            refusal: denied,
        },
        Attempt {
            reason: "query version overrides header on deny",
            client: &full,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: Some(REFUSED_VERSION),
            headers: &[("x-rustfs-source-version-id", VERSION)],
            refusal: denied,
        },
        Attempt {
            reason: "MinIO header alias supplies the policy version",
            client: &full,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[("x-minio-source-version-id", REFUSED_VERSION)],
            refusal: denied,
        },
        Attempt {
            reason: "valid scoped PutObject",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: None,
            headers: &[],
            refusal: None,
        },
        Attempt {
            reason: "replication, legal hold and retention allowed",
            client: &full,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: Some(VERSION),
            headers: &[
                ("x-minio-source-version-id", REFUSED_VERSION),
                (LEGAL_HOLD, "OFF"),
                (LOCK_MODE, "GOVERNANCE"),
                (RETAIN_UNTIL, DATE),
            ],
            refusal: None,
        },
        Attempt {
            reason: "null query version overrides the header",
            client: &full,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: Some("null"),
            headers: &[("x-rustfs-source-version-id", REFUSED_VERSION)],
            refusal: None,
        },
        Attempt {
            reason: "a query version still needs replication authorization",
            client: &put_only,
            bucket: LOCK_BUCKET,
            key: TARGET,
            version: Some(VERSION),
            headers: &[],
            refusal: Some(("InvalidArgument", 400)),
        },
    ] {
        let uploads = || store.list_multipart_uploads(row.bucket, "", None, None, None, 100);
        let before = uploads().await.expect("list before authorization").uploads.len();
        let result = row
            .client
            .create_multipart_upload()
            .bucket(row.bucket)
            .key(row.key)
            .customize()
            .mutate_request(move |request| {
                if let Some(version) = row.version {
                    let uri = format!("{}&versionId={version}", request.uri());
                    request.set_uri(uri).expect("CreateMultipartUpload with versionId");
                }
                for (name, value) in row.headers {
                    request.headers_mut().insert(*name, *value);
                }
            })
            .send()
            .await;
        let after = uploads().await.expect("list after authorization").uploads.len();
        if let Some((code, status)) = row.refusal {
            let error = result.expect_err(row.reason);
            assert_eq!(super::super::refusal(&error), (Some(code.to_owned()), Some(status)), "{}", row.reason);
            assert_eq!(before, after, "{}", row.reason);
        } else {
            let output = result.expect(row.reason);
            assert_eq!(output.bucket(), Some(row.bucket), "{}", row.reason);
            assert_eq!(output.key(), Some(row.key), "{}", row.reason);
            assert_eq!(before + 1, after, "{}", row.reason);
            if row.version == Some(VERSION) {
                use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
                use rustfs_storage_api::CompletePart;

                let upload_id = output.upload_id().expect("authorized upload ID");
                let options = ObjectOptions {
                    versioned: true,
                    ..Default::default()
                };
                let mut body = PutObjReader::from_vec(b"part of the requested version".to_vec());
                let part = store
                    .put_object_part(row.bucket, row.key, upload_id, 1, &mut body, &options)
                    .await
                    .expect("stage a part for the selected version");
                let completed = store
                    .clone()
                    .complete_multipart_upload(
                        row.bucket,
                        row.key,
                        upload_id,
                        vec![CompletePart {
                            part_num: 1,
                            etag: part.etag,
                            ..Default::default()
                        }],
                        &options,
                    )
                    .await
                    .expect("publish the version persisted by CreateMultipartUpload");
                assert_eq!(completed.version_id.expect("versioned object").to_string(), VERSION);
            }
        }
    }
    server.shutdown().await;
}

#[test]
fn create_multipart_authorization_legacy() {
    with_stack(LEGACY, create_multipart_authorization);
}

#[test]
fn create_multipart_authorization_gateway() {
    with_stack(GATEWAY, create_multipart_authorization);
}

async fn create_multipart_encryption(server: RustFSServer, legacy: bool) {
    use rustfs_ecstore::api::bucket::{metadata::BUCKET_ETAG_CONFIG, metadata_sys};
    use rustfs_utils::http::{SUFFIX_ETAG_ALGORITHM, SUFFIX_PLAINTEXT_CHECKSUM, get_consistent_str};

    let bucket = "create-multipart-encryption";
    create_bucket_in_the_store(bucket).await;
    metadata_sys::update(bucket, BUCKET_ETAG_CONFIG, br#"{"algorithm":"xxh3"}"#.to_vec())
        .await
        .expect("configure bucket XXH3 ETags");
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let context = get_global_app_context().expect("embedded context");
    let store = context.object_store();
    let kms = context.kms().handle();
    kms.reconfigure(rustfs_kms::config::KmsConfig::static_kms(
        "multipart-test-key".to_owned(),
        base64_simd::STANDARD.encode_to_string([1_u8; 32]),
    ))
    .await
    .expect("configure test KMS");

    let create = |key: &'static str| {
        client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme)
            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
    };
    let count = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|row| row.op == rustfs_s3_ops::S3Operation::CreateMultipartUpload.as_str())
            .map_or(0, |row| row.total)
    };
    let customer_key = base64_simd::STANDARD.encode_to_string([0_u8; 32]);
    let customer_md5 = "cLyPS3KoaSFGi/joRB3OUQ==";
    let ssec = |md5: &'static str| {
        create("sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(customer_key.clone())
            .sse_customer_key_md5(md5)
    };
    let uploads = || store.list_multipart_uploads(bucket, "", None, None, None, 100);
    let supplied_md5 = "1B2M2Y8AsgTpgAmY7PhCfg==";
    if legacy && !cfg!(feature = "rio-v2") {
        let before = count();
        let output = ssec(supplied_md5).send().await.expect("initiate with the supplied digest");
        let upload_id = output.upload_id().expect("upload ID for the supplied digest");
        let stored = uploads().await.expect("list the initiated upload").uploads;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].upload_id, upload_id);
        assert_eq!(stored[0].object, "sse-c");
        assert_eq!(output.sse_customer_key_md5(), Some(supplied_md5));
        assert_eq!(count(), before + 1);
        store
            .abort_multipart_upload(bucket, "sse-c", upload_id, &Default::default())
            .await
            .expect("remove the supplied-digest fixture");
    } else if !legacy {
        let before = count();
        let response = ssec(supplied_md5).send().await;
        assert!(uploads().await.expect("list after a refused digest").uploads.is_empty());
        let error = response.expect_err("the gateway refuses a nonmatching customer-key digest");
        assert_eq!(super::super::refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
        assert_eq!(
            error.message(),
            Some("the customer-provided encryption key and its MD5 must be base64 of 32 and 16 bytes and must agree")
        );
        assert_eq!(count(), before);
    }

    let before = count();
    let response = ssec(customer_md5)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await;
    assert!(
        uploads()
            .await
            .expect("list after conflicting encryption headers")
            .uploads
            .is_empty()
    );
    let error = response.expect_err("an upload selects one encryption channel");
    assert_eq!(super::super::refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
    let message = if legacy {
        "The SSE-C and managed server-side encryption headers cannot be used together."
    } else {
        "a request may name a server-managed encryption algorithm or a customer-provided key, not both"
    };
    assert_eq!(error.message(), Some(message));
    assert_eq!(count(), before + u64::from(legacy));

    let before = count();
    let customer = ssec(customer_md5)
        .customize()
        .interceptor(SuccessWire::default())
        .send()
        .await
        .expect("create an SSE-C upload");
    assert_eq!(count(), before + 1);
    assert_eq!(customer.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(customer.sse_customer_key_md5(), Some(customer_md5));
    let before = count();
    let managed = create("sse-kms")
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .ssekms_key_id("multipart-test-key")
        .send()
        .await
        .expect("create an SSE-KMS upload");
    assert_eq!(count(), before + 1);
    assert_eq!(managed.server_side_encryption().map(|value| value.as_str()), Some("aws:kms"));
    assert_eq!(managed.ssekms_key_id(), Some("multipart-test-key"));

    for output in [customer, managed] {
        assert_eq!(output.checksum_algorithm().map(|value| value.as_str()), Some("CRC64NVME"));
        assert_eq!(output.checksum_type().map(|value| value.as_str()), Some("FULL_OBJECT"));
        let info = store
            .get_multipart_info(
                bucket,
                output.key().expect("encrypted upload key"),
                output.upload_id().expect("encrypted upload ID"),
                &Default::default(),
            )
            .await
            .expect("read the durable encrypted upload");
        assert_eq!(get_consistent_str(&info.user_defined, SUFFIX_PLAINTEXT_CHECKSUM), Some("true"));
        assert_eq!(get_consistent_str(&info.user_defined, SUFFIX_ETAG_ALGORITHM), Some("xxh3"));
    }

    kms.stop().await.expect("stop test KMS");
    server.shutdown().await;
}

#[test]
fn create_multipart_encryption_legacy() {
    with_stack(LEGACY, |server| create_multipart_encryption(server, true));
}

#[test]
fn create_multipart_encryption_gateway() {
    with_stack(GATEWAY, |server| create_multipart_encryption(server, false));
}
