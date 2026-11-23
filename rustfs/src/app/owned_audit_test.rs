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

//! The legacy audit entries and notification events in the golden fixture were
//! recorded from the completion probe at b55931ccc. The owned path must emit
//! the same fields, including their absence. Each negative row changes a
//! single input and checks which fields changed.
//!
//! The fixture normalizes wall-clock time and records whether timing was present.
//!
//! The completion probe is a process-global map keyed by bucket, so every test
//! here runs under one `serial` key.

use crate::app::storage_api::context::OperationHelper;
use crate::app::storage_api::object_usecase::helper::MutationCompletionProbe;
use crate::app::storage_api::owned_parity::{ReqInfo, RequestContext};
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::shared_types::RemoteAddr;
use crate::storage::storage_api::helper_consumer::StorageObjectInfo;
use http::{Extensions, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use rustfs_audit::{AuditEntry, ObjectVersion};
use rustfs_credentials::Credentials;
use rustfs_notify::EventArgs;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::EventName;
use rustfs_trusted_proxies::ClientInfo;
use serde_json::{Value, json};
use serial_test::serial;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Instant;

const TOKEN: &str = "h2-session-token-must-not-leak";
const SECRET: &str = "h2-secret-key-must-not-leak";
const VERSION: &str = "0b5ae7d4-6c43-4c43-9b1e-8b4ad3e5c0f1";

type Request = fn(&mut HeaderMap, &mut Extensions);
type Steps = fn(OperationHelper) -> OperationHelper;
/// A case name, the owned-side input change, and the owned values it must produce.
type ViewRow = (&'static str, Request, Vec<(&'static str, Value)>);

#[derive(Clone, Copy)]
enum Answer {
    Ok(StatusCode, &'static [(&'static str, &'static str)]),
    NoSuchKey(&'static str),
    /// A code without a status of its own, with a message.
    Custom,
    /// The same code without a message.
    BareCustom,
    /// An internal error carrying a non-S3 cause and a message.
    Internal,
    /// A code with neither message nor cause.
    Bare,
}

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    uri: &'static str,
    op: S3Operation,
    event: EventName,
    credentials: bool,
    request: Request,
    steps: Steps,
    answer: Answer,
}

fn req_info(bucket: &str, object: &str, access_key: &str) -> ReqInfo {
    ReqInfo {
        cred: (!access_key.is_empty()).then(|| Credentials {
            access_key: access_key.to_owned(),
            secret_key: SECRET.to_owned(),
            session_token: TOKEN.to_owned(),
            parent_user: "h2-parent".to_owned(),
            ..Default::default()
        }),
        bucket: Some(bucket.to_owned()),
        object: Some(object.to_owned()),
        ..Default::default()
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        x_amz_request_id: request_id.to_owned(),
        trace_id: Some(format!("trace-{request_id}")),
        span_id: Some(format!("span-{request_id}")),
        start_time: Instant::now(),
    }
}

fn common(headers: &mut HeaderMap, extensions: &mut Extensions, bucket: &str, object: &str) {
    headers.insert("host", HeaderValue::from_static("h2.example.test:9000"));
    headers.insert("user-agent", HeaderValue::from_static("h2-parity"));
    extensions.insert(req_info(bucket, object, "h2-user"));
    extensions.insert(context(bucket));
}

fn object_info(bucket: &str, delete_marker: bool) -> StorageObjectInfo {
    StorageObjectInfo {
        bucket: bucket.to_owned(),
        name: "key".to_owned(),
        size: 42,
        etag: Some("etag-h2".to_owned()),
        version_id: VERSION.parse().ok(),
        delete_marker,
        ..Default::default()
    }
}

fn no_steps(helper: OperationHelper) -> OperationHelper {
    helper
}

const CASES: &[Case] = &[
    Case {
        name: "success",
        uri: "/h2-success/key",
        op: S3Operation::PutObjectTagging,
        event: EventName::ObjectTaggingPut,
        credentials: false,
        request: |h, e| common(h, e, "h2-success", "key"),
        steps: |helper| helper.object(object_info("h2-success", false)),
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "s3 error",
        uri: "/h2-s3-error/key",
        op: S3Operation::DeleteObjectTagging,
        event: EventName::ObjectTaggingDelete,
        credentials: false,
        request: |h, e| common(h, e, "h2-s3-error", "key"),
        steps: no_steps,
        answer: Answer::NoSuchKey("no such key in h2"),
    },
    Case {
        name: "custom code without status",
        uri: "/h2-custom/key",
        op: S3Operation::DeleteObjectTagging,
        event: EventName::ObjectTaggingDelete,
        credentials: false,
        request: |h, e| common(h, e, "h2-custom", "key"),
        steps: no_steps,
        answer: Answer::Custom,
    },
    Case {
        name: "non-s3 cause",
        uri: "/h2-internal/key",
        op: S3Operation::PutObjectAcl,
        event: EventName::ObjectAclPut,
        credentials: false,
        request: |h, e| common(h, e, "h2-internal", "key"),
        steps: no_steps,
        answer: Answer::Internal,
    },
    Case {
        name: "versioned object",
        uri: "/h2-versioned/key?versionId=0b5ae7d4-6c43-4c43-9b1e-8b4ad3e5c0f1",
        op: S3Operation::PutObjectRetention,
        event: EventName::ObjectCreatedPutRetention,
        credentials: false,
        request: |h, e| {
            common(h, e, "h2-versioned", "key");
            if let Some(info) = e.get_mut::<ReqInfo>() {
                info.version_id = Some(VERSION.to_owned());
            }
        },
        steps: |helper| helper.object(object_info("h2-versioned", false)).version_id(VERSION),
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "delete marker",
        uri: "/h2-delete-marker/key",
        op: S3Operation::DeleteObject,
        event: EventName::ObjectRemovedDelete,
        credentials: false,
        request: |h, e| common(h, e, "h2-delete-marker", "key"),
        steps: |helper| {
            helper
                .event_name(EventName::ObjectRemovedDeleteMarkerCreated)
                .object(object_info("h2-delete-marker", true))
                .version_id(VERSION)
        },
        answer: Answer::Ok(StatusCode::NO_CONTENT, &[]),
    },
    Case {
        name: "multipart completion",
        uri: "/h2-multipart/key?uploadId=h2-upload",
        op: S3Operation::CompleteMultipartUpload,
        event: EventName::ObjectCreatedCompleteMultipartUpload,
        credentials: false,
        request: |h, e| common(h, e, "h2-multipart", "key"),
        steps: |helper| helper.object(object_info("h2-multipart", false)).version_id(VERSION),
        answer: Answer::Ok(StatusCode::OK, &[("x-amz-checksum-xxhash64", "h2-checksum")]),
    },
    Case {
        name: "bulk delete objects",
        uri: "/h2-bulk?delete",
        op: S3Operation::DeleteObjects,
        event: EventName::ObjectRemovedDelete,
        credentials: false,
        request: |h, e| {
            common(h, e, "h2-bulk", "");
        },
        steps: |helper| {
            helper
                .suppress_event()
                .audit_objects(vec![ObjectVersion::new("gone".to_owned(), None)])
        },
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "anonymous caller",
        uri: "/h2-anonymous/path/key",
        op: S3Operation::PutObject,
        event: EventName::ObjectCreatedPut,
        credentials: false,
        request: |h, e| {
            h.insert("host", HeaderValue::from_static("h2.example.test"));
            e.insert(context("h2-anonymous"));
        },
        steps: no_steps,
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "forwarded client and peer",
        uri: "/h2-forwarded/key",
        op: S3Operation::PutObject,
        event: EventName::ObjectCreatedPut,
        credentials: false,
        request: |h, e| {
            common(h, e, "h2-forwarded", "key");
            h.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.7"));
            e.insert(ClientInfo::direct(SocketAddr::from(([203, 0, 113, 7], 0))));
            e.insert(Some(RemoteAddr(SocketAddr::from(([10, 0, 0, 2], 5555)))));
        },
        steps: no_steps,
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "peer only",
        uri: "/h2-peer/key",
        op: S3Operation::PutObject,
        event: EventName::ObjectCreatedPut,
        credentials: false,
        request: |h, e| {
            common(h, e, "h2-peer", "key");
            e.insert(Some(RemoteAddr(SocketAddr::from(([10, 0, 0, 3], 6666)))));
        },
        steps: no_steps,
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
    Case {
        name: "session token",
        uri: "/h2-token/key?X-Amz-Security-Token=h2-session-token-must-not-leak",
        op: S3Operation::PutObject,
        event: EventName::ObjectCreatedPut,
        credentials: true,
        request: |h, e| {
            common(h, e, "h2-token", "key");
            h.insert("x-amz-security-token", HeaderValue::from_static(TOKEN));
            h.insert(
                "authorization",
                HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=h2-user, Signature=h2-session-token-must-not-leak"),
            );
        },
        steps: no_steps,
        answer: Answer::Ok(StatusCode::OK, &[]),
    },
];

fn case(name: &str) -> Case {
    *CASES.iter().find(|case| case.name == name).expect("case exists")
}

fn response_headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    pairs
        .iter()
        .map(|(name, value)| (http::HeaderName::from_static(name), HeaderValue::from_static(value)))
        .collect()
}

fn owned_result(answer: Answer) -> rustfs_s3_types::S3Result<()> {
    use rustfs_s3_types::{S3Error, S3ErrorCode};
    match answer {
        Answer::Ok(..) => Ok(()),
        Answer::NoSuchKey(message) => Err(S3Error::with_message(S3ErrorCode::NoSuchKey, message)),
        Answer::Custom => Err(S3Error::with_message(S3ErrorCode::Custom("H2Custom".into()), "h2 custom refusal")),
        Answer::BareCustom => Err(S3Error::new(S3ErrorCode::Custom("H2Custom".into()))),
        Answer::Internal => {
            let mut err = S3Error::internal_error(std::io::Error::other("disk fault"));
            err.set_message("h2 storage fault");
            Err(err)
        }
        Answer::Bare => Err(S3Error::new(S3ErrorCode::InternalError)),
    }
}

fn parts(case: &Case, extra: Option<Request>) -> (Uri, HeaderMap, Extensions) {
    let (mut headers, mut extensions) = (HeaderMap::new(), Extensions::new());
    (case.request)(&mut headers, &mut extensions);
    if let Some(extra) = extra {
        extra(&mut headers, &mut extensions);
    }
    (Uri::from_static(case.uri), headers, extensions)
}

fn bucket(case: &Case) -> &'static str {
    case.uri.trim_start_matches('/').split(['/', '?']).next().unwrap_or_default()
}

/// What the probe saw, flattened to `field -> value`, plus every rendering of it
/// for the secret checks.
struct Captured {
    fields: BTreeMap<String, Value>,
    rendered: String,
}

fn flatten(prefix: &str, value: Value, out: &mut BTreeMap<String, Value>) {
    match value {
        // Recurse into the entry, its API details, the event and its object; every
        // other value (header maps, tags, object lists) is compared whole.
        Value::Object(map) if matches!(prefix, "audit" | "audit.api" | "event" | "event.object") => {
            for (key, value) in map {
                flatten(&format!("{prefix}.{key}"), value, out);
            }
        }
        value => {
            out.insert(prefix.to_owned(), value);
        }
    }
}

fn event_json(event: &EventArgs) -> Value {
    json!({
        "event_name": event.event_name.as_str(),
        "bucket_name": event.bucket_name,
        "object": {
            "bucket": event.object.bucket,
            "name": event.object.name,
            "size": event.object.size,
            "etag": event.object.etag,
            "version_id": event.object.version_id,
            "content_type": event.object.content_type,
        },
        "req_params": event.req_params,
        "resp_elements": event.resp_elements,
        "version_id": event.version_id,
        "host": event.host,
        "port": event.port,
        "user_agent": event.user_agent,
    })
}

fn capture(probe: &MutationCompletionProbe) -> Captured {
    let audits: Vec<AuditEntry> = std::mem::take(&mut *probe.audits.lock().expect("audits"));
    let events: Vec<EventArgs> = std::mem::take(&mut *probe.events.lock().expect("events"));
    let mut fields = BTreeMap::from([
        ("audit.count".to_owned(), json!(audits.len())),
        ("event.count".to_owned(), json!(events.len())),
    ]);
    let mut rendered = String::new();
    for mut entry in audits {
        rendered.push_str(&format!("{entry:?}"));
        rendered.push_str(&serde_json::to_string(&entry).expect("entry serializes"));
        // Elapsed time differs between two runs; that it is recorded is compared.
        entry.time = jiff::Timestamp::UNIX_EPOCH;
        let timed = entry.api.time_to_response.take().is_some() && entry.api.time_to_response_in_ns.take().is_some();
        fields.insert("audit.timed".to_owned(), json!(timed));
        flatten("audit", serde_json::to_value(&entry).expect("entry serializes"), &mut fields);
    }
    for event in events {
        rendered.push_str(&format!("{event:?}"));
        flatten("event", event_json(&event), &mut fields);
    }
    Captured { fields, rendered }
}

fn run_owned(case: &Case, extra: Option<Request>) -> Captured {
    let probe = MutationCompletionProbe::install(bucket(case));
    let (uri, headers, extensions) = parts(case, extra);
    let head = RequestHead {
        method: Method::PUT,
        uri,
        headers,
        credentials: case.credentials.then(|| {
            RequestCredentials::new("h2-user".to_owned(), SECRET.to_owned(), Some(TOKEN.to_owned())).expect("non-empty secret")
        }),
        region: None,
        service: None,
    };
    let envelope = RequestEnvelope::from_parts(head, extensions);
    let helper = (case.steps)(OperationHelper::from_view(&envelope.audit_view(), case.event, case.op));
    let (status, headers) = match case.answer {
        Answer::Ok(status, headers) => (status, response_headers(headers)),
        _ => (StatusCode::OK, HeaderMap::new()),
    };
    drop(helper.complete_owned(&owned_result(case.answer), status, &headers));
    capture(&probe)
}

fn legacy_fields(name: &str) -> BTreeMap<String, Value> {
    #[derive(serde::Deserialize)]
    struct Golden {
        common: BTreeMap<String, Value>,
        cases: BTreeMap<String, BTreeMap<String, Value>>,
    }

    let mut golden: Golden = serde_json::from_str(include_str!("owned_audit_golden.json")).expect("legacy audit fixture");
    golden.common.extend(golden.cases.remove(name).expect("recorded audit case"));
    golden.common
}

fn differing(baseline: &BTreeMap<String, Value>, actual: &BTreeMap<String, Value>) -> Vec<String> {
    let keys: std::collections::BTreeSet<_> = baseline.keys().chain(actual.keys()).collect();
    keys.into_iter()
        .filter(|key| baseline.get(*key) != actual.get(*key))
        .cloned()
        .collect()
}

#[test]
#[serial(owned_audit_probe)]
fn owned_helper_matches_the_recorded_audit_entry_and_event() {
    for case in CASES {
        let owned = run_owned(case, None);
        assert_eq!(owned.fields, legacy_fields(case.name), "{}", case.name);
        assert_eq!(owned.fields["audit.count"], json!(1), "{}", case.name);
        assert_eq!(owned.fields["audit.timed"], json!(true), "{}", case.name);
        let expect_event = matches!(case.answer, Answer::Ok(..)) && case.name != "bulk delete objects";
        assert_eq!(owned.fields["event.count"], json!(usize::from(expect_event)), "{}", case.name);
        assert!(!owned.rendered.contains(TOKEN), "{}: session token leaked", case.name);
        assert!(!owned.rendered.contains(SECRET), "{}: secret leaked", case.name);
    }
}

/// Pin the operation-specific fields exercised by these rows.
#[test]
#[serial(owned_audit_probe)]
fn parity_rows_carry_the_fields_they_exercise() {
    let field = |name: &str, key: &str| run_owned(&case(name), None).fields.get(key).cloned();
    assert_eq!(field("success", "audit.accessKey"), Some(json!("h2-user")));
    assert_eq!(field("success", "audit.requestID"), Some(json!("h2-success")));
    assert_eq!(
        field("success", "audit.tags"),
        Some(json!({"traceId": "trace-h2-success", "spanId": "span-h2-success"}))
    );
    assert_eq!(field("s3 error", "audit.error"), Some(json!("no such key in h2")));
    assert_eq!(field("s3 error", "audit.api.status_code"), Some(json!(404)));
    assert_eq!(field("success", "event.object.size"), Some(json!(42)));
    assert_eq!(field("custom code without status", "audit.api.status_code"), Some(json!(400)));
    assert_eq!(field("custom code without status", "audit.error"), Some(json!("h2 custom refusal")));
    assert_eq!(field("non-s3 cause", "audit.api.status_code"), Some(json!(500)));
    assert_eq!(field("non-s3 cause", "audit.error"), Some(json!("h2 storage fault")));
    assert_eq!(field("versioned object", "event.version_id"), Some(json!(VERSION)));
    assert_eq!(field("delete marker", "audit.api.status_code"), Some(json!(204)));
    assert_eq!(
        field("delete marker", "event.event_name"),
        Some(json!("s3:ObjectRemoved:DeleteMarkerCreated"))
    );
    assert_eq!(field("delete marker", "audit.event"), Some(json!("s3:ObjectRemoved:DeleteMarkerCreated")));
    assert_eq!(
        field("multipart completion", "audit.responseHeader"),
        Some(json!({"x-amz-checksum-xxhash64": "h2-checksum"}))
    );
    assert_eq!(field("bulk delete objects", "audit.api.objects"), Some(json!([{"objectName": "gone"}])));
    assert_eq!(field("anonymous caller", "audit.accessKey"), None);
    assert_eq!(field("anonymous caller", "audit.api.object"), Some(json!("path/key")));
    assert_eq!(field("forwarded client and peer", "audit.remotehost"), Some(json!("203.0.113.7")));
    assert_eq!(field("peer only", "audit.remotehost"), Some(json!("10.0.0.3")));
    assert_eq!(
        field("session token", "audit.requestQuery").and_then(|query| query.get("x-amz-security-token").cloned()),
        Some(json!("***redacted***"))
    );
}

fn with_cred(e: &mut Extensions, change: fn(&mut Credentials)) {
    if let Some(cred) = e.get_mut::<ReqInfo>().and_then(|info| info.cred.as_mut()) {
        change(cred);
    }
}

/// Each row changes one input that `audit_view` hands to the helper, on the
/// owned side only. The owned entry must carry the changed value (so a view that
/// dropped the input goes red) and differ from legacy in exactly those fields.
#[test]
#[serial(owned_audit_probe)]
fn an_input_of_the_audit_view_reaches_the_owned_entry() {
    let req_params =
        |agent: &str, principal: &str| json!({"host": "h2.example.test:9000", "user-agent": agent, "principalId": principal});
    let rows: [ViewRow; 8] = [
        (
            "success",
            |_, e| with_cred(e, |cred| cred.access_key = "other-user".to_owned()),
            vec![
                ("audit.accessKey", json!("other-user")),
                ("event.req_params", req_params("h2-parity", "other-user")),
            ],
        ),
        (
            "success",
            |_, e| with_cred(e, |cred| cred.parent_user = "other-parent".to_owned()),
            vec![("audit.parentUser", json!("other-parent"))],
        ),
        (
            "success",
            |_, e| {
                with_cred(e, |cred| {
                    cred.claims = Some(std::collections::HashMap::from([
                        ("sub".to_owned(), json!("h2-subject")),
                        ("iss".to_owned(), json!("rustfs-oidc")),
                        ("oidc_provider".to_owned(), json!("h2-provider")),
                        ("email".to_owned(), json!("private@example.test")),
                    ]))
                })
            },
            vec![(
                "audit.requestClaims",
                json!({"sub": "h2-subject", "iss": "rustfs-oidc", "oidc_provider": "h2-provider"}),
            )],
        ),
        (
            "forwarded client and peer",
            |_, e| {
                e.insert(ClientInfo::direct(SocketAddr::from(([198, 51, 100, 9], 0))));
            },
            vec![("audit.remotehost", json!("198.51.100.9"))],
        ),
        (
            "peer only",
            |_, e| {
                e.insert(Some(RemoteAddr(SocketAddr::from(([10, 0, 0, 4], 6666)))));
            },
            vec![("audit.remotehost", json!("10.0.0.4"))],
        ),
        (
            "success",
            |_, e| {
                if let Some(info) = e.get_mut::<ReqInfo>() {
                    info.object = Some("other-key".to_owned());
                }
            },
            vec![("audit.api.object", json!("other-key"))],
        ),
        (
            "success",
            |h, _| {
                h.insert("user-agent", HeaderValue::from_static("other-agent"));
            },
            vec![
                ("audit.requestQuery", json!({"host": "h2.example.test:9000", "user-agent": "other-agent"})),
                ("audit.userAgent", json!("other-agent")),
                ("event.req_params", req_params("other-agent", "h2-user")),
                ("event.user_agent", json!("other-agent")),
            ],
        ),
        (
            "success",
            |_, e| {
                e.insert(context("h2-other-request"));
            },
            vec![
                ("audit.requestID", json!("h2-other-request")),
                (
                    "audit.tags",
                    json!({"traceId": "trace-h2-other-request", "spanId": "span-h2-other-request"}),
                ),
                ("event.resp_elements", json!({"x-amz-request-id": "h2-other-request"})),
            ],
        ),
    ];
    for (name, extra, expected) in rows {
        let case = case(name);
        let legacy = legacy_fields(case.name);
        let owned = run_owned(&case, Some(extra));
        let keys: Vec<String> = expected.iter().map(|(key, _)| (*key).to_owned()).collect();
        assert_eq!(differing(&legacy, &owned.fields), keys, "{name}");
        for (key, value) in expected {
            assert_eq!(owned.fields.get(key), Some(&value), "{name}: {key}");
        }
    }
}

/// The owned wrapper records what the legacy `audit_only_operation` records,
/// hands the handler's result back unchanged, and builds no notification event.
#[tokio::test]
#[serial(owned_audit_probe)]
async fn owned_audit_only_operation_matches_the_recorded_entries() {
    let bucket = "h2-audit-only";
    let request = |h: &mut HeaderMap, e: &mut Extensions| common(h, e, bucket, "");
    let rows = [
        ("audit-only 200", StatusCode::OK, None),
        ("audit-only 204", StatusCode::NO_CONTENT, None),
        ("audit-only refused", StatusCode::OK, Some("refused")),
    ];
    for (name, status, refusal) in rows {
        let probe = MutationCompletionProbe::install(bucket);
        let answer = refusal.map_or(Answer::Ok(status, &[]), Answer::NoSuchKey);
        let (mut headers, mut extensions) = (HeaderMap::new(), Extensions::new());
        request(&mut headers, &mut extensions);
        headers.insert("host", HeaderValue::from_static("h2.example.test"));
        let head = RequestHead {
            method: Method::DELETE,
            uri: Uri::from_static("/h2-audit-only?encryption"),
            headers,
            credentials: None,
            region: None,
            service: None,
        };
        let envelope = RequestEnvelope::from_parts(head, extensions);
        let owned_result =
            RequestEnvelope::audit_only_operation(envelope, S3Operation::DeleteBucketEncryption, status, |req| async move {
                assert_eq!(req.uri().path(), "/h2-audit-only", "the handler receives the request");
                owned_result(answer)
            })
            .await;
        let owned = capture(&probe);

        assert_eq!(owned.fields, legacy_fields(name), "{name}");
        assert_eq!(owned.fields["audit.count"], json!(1));
        assert_eq!(owned.fields["event.count"], json!(0), "audit-only builds no event");
        assert_eq!(owned.fields["audit.api.status_code"], json!(refusal.map_or(status.as_u16(), |_| 404)));
        assert_eq!(owned_result.is_ok(), refusal.is_none());
        assert_eq!(owned_result.err().and_then(|err| err.message().map(str::to_owned)).as_deref(), refusal);
    }
}

#[tokio::test]
#[serial(owned_audit_probe)]
async fn owned_operation_attaches_the_sse_kms_summary_to_the_audit_entry() {
    use crate::app::object::{DecryptionRequest, SseKmsPrincipal, SseKmsPrincipalExt as _, sse_decryption};
    use crate::server::{refresh_audit_module_enabled, refresh_notify_module_enabled};

    temp_env::async_with_vars(
        [
            (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
            (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
        ],
        async {
            refresh_notify_module_enabled();
            refresh_audit_module_enabled();

            let bucket = "h2-kms-audit";
            let probe = MutationCompletionProbe::install(bucket);
            let mut headers = HeaderMap::new();
            let mut extensions = Extensions::new();
            common(&mut headers, &mut extensions, bucket, "ledger.csv");
            extensions.insert(context("kms-tagged-request"));
            let envelope = RequestEnvelope::from_parts(
                RequestHead {
                    method: Method::GET,
                    uri: Uri::from_static("/h2-kms-audit/ledger.csv"),
                    headers,
                    credentials: None,
                    region: None,
                    service: None,
                },
                extensions,
            );
            let helper = OperationHelper::from_view(&envelope.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject);

            let wrapped_key = base64_simd::STANDARD.encode_to_string([7u8; 48]);
            let metadata = std::collections::HashMap::from([
                ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
                ("x-rustfs-encryption-key-id".to_string(), "finance-key".to_string()),
                ("x-rustfs-encryption-key".to_string(), wrapped_key.clone()),
                ("x-rustfs-encryption-algorithm".to_string(), "aws:kms".to_string()),
            ]);
            let principal = SseKmsPrincipal::from_request(&envelope).expect("authenticated request has a principal");
            sse_decryption(DecryptionRequest {
                bucket: "finance",
                key: "ledger.csv",
                metadata: &metadata,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: Some(&principal),
            })
            .await
            .expect_err("metadata without an IV cannot be unwrapped");

            drop(helper.complete_owned(&Ok(()), StatusCode::OK, &HeaderMap::new()));
            probe.wait_for(1, 0).await;
            let audits = probe.audits.lock().expect("audit entries");
            assert_eq!(audits.len(), 1);
            let tags = audits[0]
                .tags
                .as_ref()
                .expect("a request that used KMS must carry its summary");
            assert_eq!(tags.get("sseType").and_then(Value::as_str), Some("SSE-KMS"));
            assert_eq!(tags.get("kmsKeyId").and_then(Value::as_str), Some("finance-key"));
            assert_eq!(tags.get("kmsOutcome").and_then(Value::as_str), Some("failure"));
            assert!(tags.contains_key("kmsErrorClass"), "a failed KMS interaction must be classified");
            assert!(
                !serde_json::to_string(tags)
                    .expect("audit tags serialize")
                    .contains(&wrapped_key)
            );
        },
    )
    .await;
}

/// The one field the two edges record differently: for an error without a
/// message the owned entry carries the code name, while the legacy entry carries
/// whatever default message s3s stored for the code (its prose for a known code,
/// nothing for a custom one).
#[test]
#[serial(owned_audit_probe)]
fn an_error_without_a_message_is_audited_under_its_code_name() {
    for (name, answer, owned_error) in [
        ("bare error", Answer::Bare, "InternalError"),
        ("bare custom error", Answer::BareCustom, "H2Custom"),
    ] {
        let case = Case {
            answer,
            ..case("non-s3 cause")
        };
        let legacy = legacy_fields(name);
        let owned = run_owned(&case, None);
        assert_eq!(differing(&legacy, &owned.fields), vec!["audit.error".to_owned()], "{owned_error}");
        assert_eq!(owned.fields.get("audit.error"), Some(&json!(owned_error)));
        let legacy_error = legacy.get("audit.error");
        match answer {
            Answer::Bare => assert!(
                legacy_error
                    .and_then(Value::as_str)
                    .is_some_and(|message| !message.is_empty() && message != owned_error)
            ),
            _ => assert_eq!(legacy_error, None),
        }
    }
}

fn s3_op_total(op: S3Operation) -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|snapshot| snapshot.op == op.as_str())
        .map_or(0, |snapshot| snapshot.total)
}

/// The owned gateway counts at the edge. Constructing and completing its helper
/// must leave the count unchanged.
#[test]
#[serial(owned_audit_probe)]
fn the_owned_helper_leaves_the_operation_count_unchanged() {
    let op = S3Operation::GetObjectRetention;
    let event = EventName::ObjectAccessedGetRetention;
    let head = RequestHead {
        method: Method::GET,
        uri: Uri::from_static("/count-bucket/count-object?retention"),
        headers: HeaderMap::new(),
        credentials: None,
        region: None,
        service: None,
    };
    let envelope = RequestEnvelope::from_parts(head, Extensions::new());

    let before = s3_op_total(op);
    drop(OperationHelper::from_view(&envelope.audit_view(), event, op).complete_owned(
        &Ok(()),
        StatusCode::OK,
        &HeaderMap::new(),
    ));
    assert_eq!(s3_op_total(op), before, "the owned helper counts nothing");
}

#[test]
#[serial_test::serial]
fn gateway_still_counts_get_object_when_audit_and_notify_are_disabled() {
    crate::app::gating_test_env::run_large_stack_test("get-counter-disabled-audit", || async {
        use crate::server::{refresh_audit_module_enabled, refresh_notify_module_enabled};
        use std::sync::Arc;
        use temp_env::async_with_vars;

        async_with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            async {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();
                let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
                let context = Arc::new(crate::app::gating_test_env::app_context_from_current_environment(&ambient));
                let slot = crate::runtime_sources::ServerContextSlot::new();
                assert!(slot.install(Arc::clone(&context)));
                let gateway = crate::server::TestGateway::over(&context, slot);
                let req = RequestEnvelope::from_parts(
                    RequestHead {
                        method: Method::GET,
                        uri: Uri::from_static("/missing-counter-bucket/object"),
                        headers: HeaderMap::new(),
                        credentials: None,
                        region: None,
                        service: None,
                    },
                    Extensions::new(),
                );
                assert!(matches!(
                    OperationHelper::from_view(&req.audit_view(), EventName::ObjectAccessedGet, S3Operation::GetObject),
                    OperationHelper::Disabled,
                ));
                let recorder = metrics_util::debugging::DebuggingRecorder::new();
                let snapshotter = recorder.snapshotter();
                let _guard = metrics::set_default_local_recorder(&recorder);
                let response = gateway
                    .signed(Method::GET, "/missing-counter-bucket/object", vec![], bytes::Bytes::new())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
                assert!(
                    snapshotter.snapshot().into_vec().into_iter().any(|(key, _, _, value)| {
                        key.key().name() == "rustfs_s3_operations_total"
                            && key
                                .key()
                                .labels()
                                .any(|l| l.key() == "op" && l.value() == S3Operation::GetObject.as_str())
                            && matches!(value, metrics_util::debugging::DebugValue::Counter(1))
                    }),
                    "an admitted GET must count when audit and notify are disabled"
                );
            },
        )
        .await;
    });
}
