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

use super::authorize_delete_object;
use crate::app::RequestEnvelope;
use crate::storage::access::{
    AuthorizationRequest, BucketGenerationGuard, PendingDeleteBucketGenerationGuard, ReqInfo, S3Error, S3ErrorCode, S3Request,
    S3Result,
};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, SUFFIX_SOURCE_VERSION_ID, insert_header};
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
#[derive(Debug, PartialEq)]
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

async fn decide<R: AuthorizationRequest + ?Sized>(req: &mut R, case: &Case) -> Result<Handed, S3ErrorCode> {
    let guard = authorize_delete_object(req, BUCKET, "key", case.query_version, (case.generation)())
        .await
        .map_err(|error| error.code().clone())?;
    let info = req.req_info().expect("an allowed decision keeps the caller");
    assert_eq!((info.bucket.as_deref(), info.object.as_deref()), (Some(BUCKET), Some("key")));
    Ok(Handed {
        version_id: info.version_id.clone(),
        guard_ready: matches!(guard, PendingDeleteBucketGenerationGuard::Ready(_)),
    })
}

#[test]
fn both_request_forms_decide_and_hand_on_the_same() {
    crate::app::gating_test_env::run_large_stack_test("e2-delete-object-parity", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        // Every case runs, so one run names every case a defect breaks.
        let mut wrong = Vec::new();
        for case in cases() {
            let mut legacy = request(&case, &server_ctx);
            let mut envelope = RequestEnvelope::from_s3s(&legacy);
            for (form, answer) in [
                ("legacy request", decide(&mut legacy, &case).await),
                ("envelope", decide(&mut envelope, &case).await),
            ] {
                if answer != case.expected {
                    wrong.push(format!("{} ({form}): {answer:?}", case.name));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    });
}
