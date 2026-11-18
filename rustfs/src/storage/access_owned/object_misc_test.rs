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

//! Both request forms through the one decision: the same answer and the same state handed on
//! (the caller's `ReqInfo` and the guard), allowed and refused.

use super::decide_delete_object;
use crate::app::RequestEnvelope;
use crate::storage::access::{
    AuthorizationRequest, BucketGenerationGuard, PendingDeleteBucketGenerationGuard, ReqInfo, S3Error, S3ErrorCode, S3Request,
    S3Result,
};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use rustfs_iam::store::{Store as _, UserType};
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, SUFFIX_SOURCE_VERSION_ID, insert_header};
use serial_test::serial;
use std::sync::Arc;

const BUCKET: &str = "e2-delete-parity";
const QUERY_VERSION: &str = "11111111-1111-1111-1111-111111111111";
const SOURCE_VERSION: &str = "22222222-2222-2222-2222-222222222222";

struct Case {
    name: &'static str,
    headers: &'static [(&'static str, &'static str)],
    query_version: Option<&'static str>,
    caller: Option<ReqInfo>,
    generation: fn() -> S3Result<BucketGenerationGuard>,
    expected: Result<Handed, S3ErrorCode>,
}

/// What an allowed decision hands on: the version it authorized and whether the guard is ready.
#[derive(Clone, Debug, PartialEq)]
struct Handed {
    version_id: Option<String>,
    guard_ready: bool,
}

fn ready() -> S3Result<BucketGenerationGuard> {
    Ok(BucketGenerationGuard {
        bucket: BUCKET.to_owned(),
        incarnation_id: uuid::Uuid::nil(),
    })
}

fn missing_bucket() -> S3Result<BucketGenerationGuard> {
    Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "gone"))
}

fn unavailable() -> S3Result<BucketGenerationGuard> {
    Err(S3Error::with_message(S3ErrorCode::ServiceUnavailable, "metadata unavailable"))
}

fn owner() -> Option<ReqInfo> {
    Some(ReqInfo {
        cred: Some(rustfs_credentials::Credentials::default()),
        is_owner: true,
        ..Default::default()
    })
}

fn stranger() -> Option<ReqInfo> {
    Some(ReqInfo {
        cred: Some(rustfs_credentials::Credentials {
            access_key: "e2-delete-parity-stranger".to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn anonymous() -> Option<ReqInfo> {
    Some(ReqInfo::default())
}

const REPLICA: (&str, &str) = (AMZ_BUCKET_REPLICATION_STATUS, "REPLICA");

fn cases() -> Vec<Case> {
    let handed = |version: Option<&str>, guard_ready| {
        Ok(Handed {
            version_id: version.map(str::to_owned),
            guard_ready,
        })
    };
    vec![
        Case {
            name: "a missing bucket is named before policy, even to an anonymous caller",
            headers: &[],
            query_version: Some(QUERY_VERSION),
            caller: anonymous(),
            generation: missing_bucket,
            expected: Err(S3ErrorCode::NoSuchBucket),
        },
        Case {
            name: "anonymous",
            headers: &[],
            query_version: Some(QUERY_VERSION),
            caller: anonymous(),
            generation: ready,
            expected: Err(S3ErrorCode::AccessDenied),
        },
        Case {
            name: "a caller without a policy",
            headers: &[],
            query_version: Some(QUERY_VERSION),
            caller: stranger(),
            generation: ready,
            expected: Err(S3ErrorCode::AccessDenied),
        },
        Case {
            name: "no caller installed",
            headers: &[],
            query_version: Some(QUERY_VERSION),
            caller: None,
            generation: ready,
            expected: Err(S3ErrorCode::InternalError),
        },
        Case {
            name: "an invalid force-delete value",
            headers: &[("x-rustfs-force-delete", "maybe")],
            query_version: Some(QUERY_VERSION),
            caller: owner(),
            generation: ready,
            expected: Err(S3ErrorCode::InvalidRequest),
        },
        Case {
            name: "a replica delete naming no version is authorized for its source version",
            headers: &[REPLICA],
            query_version: None,
            caller: owner(),
            generation: ready,
            expected: handed(Some(SOURCE_VERSION), true),
        },
        Case {
            name: "any other delete naming no version ignores the source-version header",
            headers: &[],
            query_version: None,
            caller: owner(),
            generation: ready,
            expected: handed(None, true),
        },
        Case {
            name: "an unavailable generation is handed on as a failed guard",
            headers: &[],
            query_version: Some(QUERY_VERSION),
            caller: owner(),
            generation: unavailable,
            expected: handed(Some(QUERY_VERSION), false),
        },
    ]
}

fn request(case: &Case, server_ctx: &Arc<ServerContextSlot>) -> S3Request<()> {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, SOURCE_VERSION);
    for (name, value) in case.headers {
        headers.insert(*name, HeaderValue::from_static(value));
    }
    let mut extensions = Extensions::new();
    if let Some(caller) = &case.caller {
        extensions.insert(caller.clone());
    }
    extensions.insert(Arc::clone(server_ctx));
    S3Request {
        input: (),
        method: Method::DELETE,
        uri: Uri::from_static("/e2-delete-parity/key"),
        headers,
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn decide<R: AuthorizationRequest + ?Sized>(req: &mut R, bucket: &str, case: &Case) -> Result<Handed, S3ErrorCode> {
    let guard = decide_delete_object(req, bucket, "key", case.query_version, (case.generation)())
        .await
        .map_err(|error| error.code().clone())?;
    let info = req.req_info().expect("an allowed decision keeps the caller");
    assert_eq!((info.bucket.as_deref(), info.object.as_deref()), (Some(bucket), Some("key")));
    Ok(Handed {
        version_id: info.version_id.clone(),
        guard_ready: matches!(guard, PendingDeleteBucketGenerationGuard::Ready(_)),
    })
}

/// Runs every case in both request forms; one run names every case a defect breaks.
async fn assert_cases(cases: Vec<Case>, bucket: &str, server_ctx: &Arc<ServerContextSlot>) {
    let mut wrong = Vec::new();
    for case in cases {
        let mut legacy = request(&case, server_ctx);
        let mut envelope = RequestEnvelope::from_s3s(&legacy);
        for (form, answer) in [
            ("legacy request", decide(&mut legacy, bucket, &case).await),
            ("envelope", decide(&mut envelope, bucket, &case).await),
        ] {
            if answer != case.expected {
                wrong.push(format!("{} ({form}): {answer:?}", case.name));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn both_request_forms_decide_and_hand_on_the_same() {
    crate::app::gating_test_env::run_large_stack_test("e2-delete-object-parity", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        assert_cases(cases(), BUCKET, &server_ctx).await;
    });
}

const FORCE: (&str, &str) = ("x-rustfs-force-delete", "true");
const BYPASS: (&str, &str) = ("x-amz-bypass-governance-retention", "true");

/// A caller allowed `s3:DeleteObject` but not the action a header adds is refused that action;
/// the caller allowed both is served.
#[tokio::test]
#[serial]
async fn the_actions_a_header_adds_are_decided_for_the_caller() {
    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    ObjectStore::new(Arc::clone(&store))
        .save_iam_config(
            serde_json::json!({"version": 1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("seed IAM format");
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&store)).await.expect("initialize IAM");
    let mut callers = Vec::new();
    for (user, actions) in [
        ("e2-delete-only", r#""s3:DeleteObject""#),
        ("e2-delete-force", r#""s3:DeleteObject","s3:ForceDeleteObject""#),
        ("e2-delete-bypass", r#""s3:DeleteObject","s3:BypassGovernanceRetention""#),
    ] {
        iam.create_user(
            user,
            &AddOrUpdateUserReq {
                secret_key: format!("{user}-secret-key"),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create IAM user");
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":[{actions}],"Resource":"arn:aws:s3:::{BUCKET}/*"}}]}}"#
        );
        iam.set_policy(user, Policy::parse_config(policy.as_bytes()).expect("parse policy"))
            .await
            .expect("store policy");
        iam.policy_db_set(user, UserType::Reg, false, user)
            .await
            .expect("attach policy");
        let credentials = iam.get_user(user).await.expect("load IAM user").credentials;
        callers.push(ReqInfo {
            cred: Some(credentials),
            ..Default::default()
        });
    }
    let server_ctx = ServerContextSlot::new();
    assert!(server_ctx.install(Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms)))));
    let [delete_only, delete_force, delete_bypass] = <[ReqInfo; 3]>::try_from(callers).expect("three callers");
    let served = Ok(Handed {
        version_id: None,
        guard_ready: true,
    });
    type Headers = &'static [(&'static str, &'static str)];
    let case = |name: &'static str, headers: Headers, caller: &ReqInfo, expected: Result<Handed, S3ErrorCode>| Case {
        name,
        headers,
        query_version: None,
        caller: Some(caller.clone()),
        generation: ready,
        expected,
    };
    let cases = vec![
        case(
            "force delete without s3:ForceDeleteObject",
            &[FORCE],
            &delete_only,
            Err(S3ErrorCode::AccessDenied),
        ),
        case("force delete with s3:ForceDeleteObject", &[FORCE], &delete_force, served.clone()),
        case(
            "bypass without s3:BypassGovernanceRetention",
            &[BYPASS],
            &delete_only,
            Err(S3ErrorCode::AccessDenied),
        ),
        case("bypass with s3:BypassGovernanceRetention", &[BYPASS], &delete_bypass, served.clone()),
        case("a plain delete needs neither", &[], &delete_only, served),
    ];
    assert_cases(cases, BUCKET, &server_ctx).await;
}

/// A recursive force delete needs an authenticated caller, even where a bucket policy grants an
/// anonymous one both actions; a replica request is exempt.
#[test]
fn a_recursive_force_delete_by_an_anonymous_caller_needs_a_replica_request() {
    crate::app::gating_test_env::run_large_stack_test("e2-delete-object-anonymous-force", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("e2-delete-public-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the public bucket");
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ForceDeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket).await.expect("bucket metadata")).clone();
        metadata.policy_config = Some(serde_json::from_str(&policy).expect("parse bucket policy"));
        metadata.policy_config_json = policy.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("publish bucket policy");
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(store, Arc::new(UnreadyIam), Arc::new(TestKms)))));
        let served = Ok(Handed {
            version_id: None,
            guard_ready: true,
        });
        type Headers = &'static [(&'static str, &'static str)];
        let case = |name: &'static str, headers: Headers, expected: Result<Handed, S3ErrorCode>| Case {
            name,
            headers,
            query_version: None,
            caller: anonymous(),
            generation: ready,
            expected,
        };
        // A replica delete is decided for its source version (`request` always sends one).
        let replica = Ok(Handed {
            version_id: Some(SOURCE_VERSION.to_owned()),
            guard_ready: true,
        });
        let cases = vec![
            case("an anonymous recursive force delete", &[FORCE], Err(S3ErrorCode::AccessDenied)),
            case("an anonymous replica recursive force delete", &[FORCE, REPLICA], replica),
            case("an anonymous plain delete the bucket policy grants", &[], served),
        ];
        assert_cases(cases, &bucket, &server_ctx).await;
    });
}

struct ReadyIam(Arc<IamSys<ObjectStore>>);

impl IamInterface for ReadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        Arc::clone(&self.0)
    }

    fn is_ready(&self) -> bool {
        true
    }
}

struct UnreadyIam;

impl IamInterface for UnreadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        panic!("a bucket-policy decision does not resolve IAM")
    }

    fn is_ready(&self) -> bool {
        false
    }
}

struct TestKms;

impl KmsInterface for TestKms {
    fn handle(&self) -> Arc<KmsServiceManager> {
        Arc::new(KmsServiceManager::new())
    }
}
