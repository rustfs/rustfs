// Copyright 2026 RustFS Team
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

use super::*;
use http::{Request as HttpRequest, StatusCode};
use rustfs_gateway::{ConnectionIntent, Limits, RouterBuilder, TargetKind, WireRequest};
use rustfs_gateway_core::{HostClass, RouteRequestParts};
use std::collections::BTreeSet;

const TAG_PATH: &str = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fm3-vectors%2Findex%2Findex.one";

#[test]
fn gateway_dialect_declares_all_21_operations() {
    let dialect = dialect().expect("all 21 service declarations must assemble");
    assert_eq!(dialect.operations().len(), 18);
    assert_eq!(dialect.claimed_operations().len(), 3);
    assert_eq!(dialect.operation_names().collect::<BTreeSet<_>>(), NAMES.into_iter().collect());
    assert!(NAMES.into_iter().all(serves));
    assert!(!serves("GetObject"));
    assert!(!serves("s3vectors:UnknownAction"));
}

#[test]
fn gateway_operation_floors_allow_only_sigv4_header_and_query() {
    use rustfs_gateway::sig::SecurityFloor;

    let admissions = [
        SecurityFloor::default(),
        SecurityFloor::default().delegate_anonymous_to_authorizer_after_listing_in_the_posture_report(),
    ];
    for floor in &FLOORS {
        assert!(floor.requires_sigv4_under_every_policy(), "{}", floor.name());
        assert_eq!(floor.service(), SigService::S3Vectors);
        assert!(floor.allowed_schemes().allows_header());
        assert!(floor.refuses_sigv2_under_every_policy());
        for admission in &admissions {
            assert!(admission.admits_presigned(floor));
            assert!(!admission.admits_anonymous(floor));
        }
        assert!(!floor.allowed_schemes().allows_post_policy());
        assert!(floor.query_bearer_claim().is_none());
    }
}

#[test]
fn gateway_router_selects_exact_methods_and_raw_paths() {
    let dialect = dialect().expect("service dialect");
    let router = RouterBuilder::for_service(SERVICE)
        .dialect(&dialect)
        .build()
        .expect("route registry");
    for (index, operation) in OPERATIONS.into_iter().enumerate() {
        let (method, path) = match operation {
            Operation::TagResource => (Method::POST, TAG_PATH.to_owned()),
            Operation::ListTagsForResource => (Method::GET, TAG_PATH.to_owned()),
            Operation::UntagResource => (Method::DELETE, TAG_PATH.to_owned()),
            _ => (Method::POST, format!("/{}", operation.name())),
        };
        let request = HttpRequest::builder()
            .method(method.clone())
            .uri(&path)
            .header("host", "127.0.0.1")
            .body(Bytes::new())
            .expect("route request");
        let wire = WireRequest::accept(request, &Limits::default()).expect("accepted request");
        let route = RouteRequestParts {
            method: &method,
            path: &path,
            target: TargetKind::Service,
            host_class: HostClass::Standard,
            arn_form: None,
            query: wire.query(),
            headers: wire.headers(),
            host_named_bucket: false,
        };
        assert_eq!(router.resolve(&route).expect("modeled operation must resolve").op_name, NAMES[index]);
    }
}

#[test]
fn gateway_router_has_no_s3_fallback_or_action_aliases() {
    let dialect = dialect().expect("service dialect");
    let router = RouterBuilder::for_service(SERVICE)
        .dialect(&dialect)
        .build()
        .expect("route registry");
    for (method, path) in [
        (Method::GET, "/GetVectors"),
        (Method::POST, "/getvectors"),
        (Method::POST, "/GetVectors/"),
        (Method::POST, "/UnknownAction"),
        (Method::GET, "/"),
        (Method::GET, "/bucket/key"),
        (Method::PUT, TAG_PATH),
    ] {
        let request = HttpRequest::builder()
            .method(method.clone())
            .uri(path)
            .header("host", "127.0.0.1")
            .body(Bytes::new())
            .expect("negative route request");
        let wire = WireRequest::accept(request, &Limits::default()).expect("accepted request");
        let route = RouteRequestParts {
            method: &method,
            path,
            target: TargetKind::Service,
            host_class: HostClass::Standard,
            arn_form: None,
            query: wire.query(),
            headers: wire.headers(),
            host_named_bucket: false,
        };
        assert!(router.resolve(&route).is_none(), "unexpected route: {method} {path}");
    }
}

#[test]
fn protocol_input_retains_repeated_tag_keys() {
    let route = Route::recognize(&Method::DELETE, TAG_PATH).expect("UntagResource route");
    let parsed = route
        .parse(Some("tagKeys=a%20b&tagKeys=a%2Fb&tagKeys=a%20b"), b"", "us-east-1")
        .expect("repeated tagKeys are modeled list elements");
    let Request::UntagResource(input) = parsed else { panic!("wrong operation") };
    assert_eq!(input.tag_keys, ["a b", "a/b", "a b"]);
    assert_eq!(input.resource_arn, "arn:aws:s3vectors:us-east-1::bucket/m3-vectors/index/index.one");
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransportMarker(u64);

#[tokio::test]
async fn rest_json_error_keeps_connection_intent_and_every_extension() {
    use http_body_util::BodyExt;
    let mut response = HttpResponse::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/xml")
        .header("content-length", "999")
        .header("x-amz-request-id", "m3-gateway-error")
        .body(Body::from("old XML".as_bytes().to_vec()))
        .expect("gateway error response");
    response.extensions_mut().insert(ConnectionIntent::Close);
    response.extensions_mut().insert(TransportMarker(7));
    replace_error_response(&mut response, ProtocolError::new(ErrorCode::AccessDenied, "Access denied"));
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.extensions().get::<ConnectionIntent>(), Some(&ConnectionIntent::Close));
    assert_eq!(response.extensions().get::<TransportMarker>(), Some(&TransportMarker(7)));
    assert_eq!(response.headers()["x-amz-request-id"], "m3-gateway-error");
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-amzn-errortype"], "AccessDeniedException");
    let declared_length = response.headers()["content-length"]
        .to_str()
        .expect("length string")
        .parse::<usize>()
        .expect("length");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("complete JSON response")
        .to_bytes();
    assert_eq!(declared_length, body.len());
    let json: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON error");
    assert_eq!(json["__type"], "AccessDeniedException");
    assert_eq!(json["message"], "Access denied");
}

#[test]
fn codec_preserves_modeled_quota_status_and_json() {
    let wire = WireRequest::accept(
        HttpRequest::builder()
            .method(Method::POST)
            .uri("/CreateIndex")
            .header("host", "127.0.0.1")
            .body(Bytes::new())
            .expect("wire"),
        &Limits::default(),
    )
    .expect("accepted");
    let meta = MetaView::of(&wire, TargetKind::Service).expect("service request");
    let response = ProtocolError::new(ErrorCode::ServiceQuotaExceeded, "Quota exceeded").into_response();
    let encoded = Action::<0>::encode(response, &meta, 402).expect("modeled error encoder");
    assert_eq!(encoded.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(encoded.headers["x-amzn-errortype"], "ServiceQuotaExceededException");
    let ResponseBody::Complete(body) = encoded.body else { panic!("complete JSON body required") };
    let json: serde_json::Value = serde_json::from_slice(&body).expect("quota JSON");
    assert_eq!(json["__type"], "ServiceQuotaExceededException");
}

// These tests exercise the real Gateway authenticator and the protocol Backend. The
// application fixture refuses authorization or execution, so it cannot report business success.
use http_body_util::BodyExt;
use rustfs_gateway::sig::{AmzDate, PayloadMode, SigLocation, SigV4Signer, SigningCredentials, SigningRequest, SigningScope};
use rustfs_gateway::{
    ClockSkewAck, Credentials, FixedClock, RegionSet, S3Service, SigV4Authenticator, StaticCredentials, allow_when,
};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

const TEST_EPOCH: i64 = 1_767_323_045;
const TEST_ACCESS_KEY: &str = "AKIDEXAMPLE";
const TEST_SECRET: &[u8] = b"gateway-sdk-probe-public-test-secret";
const BUCKET_BODY: &[u8] = br#"{"vectorBucketName":"protocol-bucket"}"#;

#[derive(Debug)]
struct AdmissionObservation {
    access_key: String,
    operation: String,
    location: SigLocation,
    raw_path: String,
    raw_query: String,
    request: Request,
}

#[derive(Default)]
struct AdmissionCalls {
    authorized: Mutex<Vec<AdmissionObservation>>,
    executed: AtomicUsize,
}

struct RefusingApplication {
    calls: Arc<AdmissionCalls>,
    allow_admission: bool,
}

impl VectorGatewayService for RefusingApplication {
    type Authorization = Request;

    fn authorize<'a>(
        &'a self,
        context: &'a RequestContextView,
        request: &'a Request,
    ) -> BoxFuture<'a, Result<Self::Authorization, ProtocolError>> {
        let principal = context.principal().expect("verified principal");
        self.calls
            .authorized
            .lock()
            .expect("observations")
            .push(AdmissionObservation {
                access_key: principal.identity().access_key_id().to_owned(),
                operation: context.operation().to_owned(),
                location: principal.scheme().location(),
                raw_path: context.raw_path().to_owned(),
                raw_query: context.raw_query().to_owned(),
                request: request.clone(),
            });
        Box::pin(async move {
            if self.allow_admission {
                Ok(request.clone())
            } else {
                Err(ProtocolError::new(ErrorCode::AccessDenied, "Access denied"))
            }
        })
    }

    fn execute<'a>(
        &'a self,
        authorization: Self::Authorization,
        request: Request,
    ) -> BoxFuture<'a, Result<Response, ProtocolError>> {
        assert_eq!(authorization, request, "execute receives the exact authorized DTO");
        self.calls.executed.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ProtocolError::new(ErrorCode::ServiceUnavailable, "Vector service is unavailable")) })
    }
}

fn admission_service(allow_admission: bool) -> (S3Service, Arc<AdmissionCalls>) {
    admission_service_for_region(allow_admission, "us-east-1")
}

fn admission_service_for_region(allow_admission: bool, backend_region: &str) -> (S3Service, Arc<AdmissionCalls>) {
    admission_service_with_builder(allow_admission, backend_region, ServiceBuilder::for_service(SigService::S3Vectors))
}

fn admission_service_with_builder(
    allow_admission: bool,
    backend_region: &str,
    builder: ServiceBuilder,
) -> (S3Service, Arc<AdmissionCalls>) {
    let credentials =
        Arc::new(StaticCredentials::new().with(Credentials::new(TEST_ACCESS_KEY, TEST_SECRET).expect("test identity")));
    let authenticator = SigV4Authenticator::new(credentials, RegionSet::new(["us-east-1"]).expect("test region"))
        .verify_paths_double_encoded_under(&["/tags"]);
    let builder = builder
        .authenticator(authenticator)
        .authorizer(allow_when(|request| serves(request.operation)))
        .clock_with_skew_ack(
            FixedClock::at_unix_seconds(TEST_EPOCH),
            ClockSkewAck::i_understand_a_skewed_clock_can_disable_signature_expiry(),
        );
    let calls = Arc::new(AdmissionCalls::default());
    let backend = Arc::new(Backend::new(
        RefusingApplication {
            calls: Arc::clone(&calls),
            allow_admission,
        },
        backend_region.to_owned(),
    ));
    let service = register(builder, backend)
        .expect("Vector dialect")
        .build()
        .expect("Vector service");
    (service, calls)
}

fn signed_request(
    location: SigLocation,
    method: Method,
    path: &str,
    query: &str,
    body: Bytes,
    scope: (&str, SigService),
    secret: &[u8],
) -> HttpRequest<http_body_util::Full<Bytes>> {
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::HOST, http::HeaderValue::from_static("gateway-sdk-probe.invalid"));
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    let probe = HttpRequest::builder()
        .uri(path)
        .header(http::header::HOST, "gateway-sdk-probe.invalid")
        .body(Bytes::new())
        .expect("probe");
    let accepted = WireRequest::accept(probe, &Limits::default()).expect("accepted host");
    let stamp = AmzDate::parse("20260102T030405Z").expect("fixed date");
    let scope = SigningScope::new(stamp.day(), scope.0, scope.1).expect("scope");
    let mut signer = SigV4Signer::new(SigningCredentials::new(TEST_ACCESS_KEY, secret).expect("credentials"), scope);
    let signing = SigningRequest::new(
        &method,
        path,
        query,
        &headers,
        accepted.host().raw_for_signing(),
        match location {
            SigLocation::Query => PayloadMode::Unsigned,
            _ => PayloadMode::ExactSha256(Sha256::digest(&body).into()),
        },
        stamp,
    );
    let signed = match location {
        SigLocation::Header => signer.sign_headers(&signing),
        SigLocation::Query => signer.presign(&signing, 300),
        _ => panic!("test supports header and query signatures"),
    }
    .expect("signed request");
    let mut request = HttpRequest::builder().method(method).uri(signed.target());
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    request.body(http_body_util::Full::new(body)).expect("request")
}

async fn assert_protocol_error(response: HttpResponse<Body>, code: ErrorCode) {
    assert_eq!(response.status(), code.status_code());
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-amzn-errortype"], code.name());
    let body = response.into_body().collect().await.expect("error body").to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error JSON");
    assert_eq!(body["__type"], code.name());
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn signed_header_and_presign_reach_authorization_with_verified_identity() {
    for location in [SigLocation::Header, SigLocation::Query] {
        let (service, calls) = admission_service(false);
        let request = signed_request(
            location,
            Method::POST,
            "/GetVectorBucket",
            "",
            Bytes::from_static(BUCKET_BODY),
            ("us-east-1", SigService::S3Vectors),
            TEST_SECRET,
        );
        let raw_query = request.uri().query().unwrap_or_default().to_owned();
        assert_protocol_error(service.call(request).await, ErrorCode::AccessDenied).await;
        let observations = calls.authorized.lock().expect("observations");
        assert_eq!(observations.len(), 1);
        let observed = &observations[0];
        assert_eq!(observed.access_key, TEST_ACCESS_KEY);
        assert_eq!(observed.operation, "s3vectors:GetVectorBucket");
        assert_eq!(observed.location, location);
        assert_eq!(observed.raw_path, "/GetVectorBucket");
        assert_eq!(observed.raw_query, raw_query);
        assert!(
            matches!(&observed.request, Request::GetVectorBucket(input) if input.vector_bucket_name.as_deref() == Some("protocol-bucket"))
        );
        assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn authorized_execution_error_remains_a_modeled_failure() {
    let (service, calls) = admission_service(true);
    let request = signed_request(
        SigLocation::Header,
        Method::POST,
        "/GetVectorBucket",
        "",
        Bytes::from_static(BUCKET_BODY),
        ("us-east-1", SigService::S3Vectors),
        TEST_SECRET,
    );
    assert_protocol_error(service.call(request).await, ErrorCode::ServiceUnavailable).await;
    assert_eq!(calls.authorized.lock().expect("observations").len(), 1);
    assert_eq!(calls.executed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_scope_and_signature_never_reach_application_authorization() {
    let (service, calls) = admission_service(true);
    for location in [SigLocation::Header, SigLocation::Query] {
        for (region, signing_service, secret) in [
            ("us-east-1", SigService::S3, TEST_SECRET),
            ("eu-west-1", SigService::S3Vectors, TEST_SECRET),
            ("us-east-1", SigService::S3Vectors, b"incorrect-public-test-secret".as_slice()),
        ] {
            let request = signed_request(
                location,
                Method::POST,
                "/GetVectorBucket",
                "",
                Bytes::from_static(BUCKET_BODY),
                (region, signing_service),
                secret,
            );
            let response = service.call(request).await;
            assert!(!response.status().is_success());
            assert!(response.extensions().get::<rustfs_gateway::S3Error>().is_some());
        }
    }
    assert!(calls.authorized.lock().expect("observations").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn verified_scope_must_also_match_backend_region() {
    let (service, calls) = admission_service_for_region(true, "eu-west-1");
    let request = signed_request(
        SigLocation::Header,
        Method::POST,
        "/GetVectorBucket",
        "",
        Bytes::from_static(BUCKET_BODY),
        ("us-east-1", SigService::S3Vectors),
        TEST_SECRET,
    );
    assert_protocol_error(service.call(request).await, ErrorCode::AccessDenied).await;
    assert!(calls.authorized.lock().expect("observations").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tampered_signed_components_never_reach_application_authorization() {
    let (service, calls) = admission_service(true);
    for tamper in ["body", "path", "query", "unsigned-amz-header"] {
        let mut request = signed_request(
            SigLocation::Header,
            Method::POST,
            "/GetVectorBucket",
            "marker=original",
            Bytes::from_static(BUCKET_BODY),
            ("us-east-1", SigService::S3Vectors),
            TEST_SECRET,
        );
        match tamper {
            "body" => {
                *request.body_mut() = http_body_util::Full::new(Bytes::from_static(br#"{"vectorBucketName":"different-bucket"}"#))
            }
            "path" => *request.uri_mut() = "/DeleteVectorBucket?marker=original".parse().expect("URI"),
            "query" => *request.uri_mut() = "/GetVectorBucket?marker=changed".parse().expect("URI"),
            "unsigned-amz-header" => {
                request
                    .headers_mut()
                    .insert("x-amz-meta-injected", http::HeaderValue::from_static("unexpected"));
            }
            _ => unreachable!(),
        }
        let response = service.call(request).await;
        assert!(!response.status().is_success(), "{tamper}");
        assert!(response.extensions().get::<rustfs_gateway::S3Error>().is_some(), "{tamper}");
    }
    assert!(calls.authorized.lock().expect("observations").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn authenticated_invalid_json_is_rejected_before_application_authorization() {
    let (service, calls) = admission_service(true);
    let request = signed_request(
        SigLocation::Header,
        Method::POST,
        "/GetVectorBucket",
        "",
        Bytes::from_static(b"{"),
        ("us-east-1", SigService::S3Vectors),
        TEST_SECRET,
    );
    assert_protocol_error(service.call(request).await, ErrorCode::Validation).await;
    assert!(calls.authorized.lock().expect("observations").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

struct UnknownLengthBody {
    bytes: Bytes,
}

impl http_body::Body for UnknownLengthBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.bytes.is_empty() {
            return std::task::Poll::Ready(None);
        }
        let size = self.bytes.len().min(64 * 1024);
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(self.bytes.split_to(size)))))
    }
}

#[tokio::test]
async fn gateway_body_limit_counts_streamed_bytes_without_content_length() {
    use crate::vector::protocol::gateway_error_bridge::typed_error_filter;

    for length in [
        super::super::MAX_REQUEST_BODY_BYTES - 1,
        super::super::MAX_REQUEST_BODY_BYTES,
        super::super::MAX_REQUEST_BODY_BYTES + 1,
    ] {
        let builder = ServiceBuilder::for_service(SigService::S3Vectors).stage_filter(typed_error_filter(|_, error| {
            assert_eq!(error.code(), Some(&rustfs_gateway::ErrorCode::ENTITY_TOO_LARGE));
            Some(ProtocolError::validation("request", "Request body exceeds 20 MiB"))
        }));
        let (service, calls) = admission_service_with_builder(false, "us-east-1", builder);
        let mut bytes = BUCKET_BODY.to_vec();
        bytes.resize(length, b' ');
        let bytes = Bytes::from(bytes);
        let request = signed_request(
            SigLocation::Header,
            Method::POST,
            "/GetVectorBucket",
            "",
            bytes.clone(),
            ("us-east-1", SigService::S3Vectors),
            TEST_SECRET,
        )
        .map(|_| UnknownLengthBody { bytes });
        assert!(!request.headers().contains_key(http::header::CONTENT_LENGTH));
        let response = service.call(request).await;
        let expected_calls = usize::from(length <= super::super::MAX_REQUEST_BODY_BYTES);
        if expected_calls == 1 {
            assert_protocol_error(response, ErrorCode::AccessDenied).await;
        } else {
            assert!(response.extensions().get::<rustfs_gateway::S3Error>().is_some());
            assert_protocol_error(response, ErrorCode::Validation).await;
        }
        assert_eq!(calls.authorized.lock().expect("observations").len(), expected_calls);
        assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn sdk_tags_keep_signed_resource_and_repeated_query_until_authorization() {
    // Copied from the fixed botocore 1.42.97 fixture, signed at TEST_EPOCH. The
    // independent SDK signature covers generic SigV4 double path encoding.
    const PATH: &str = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fprotocol-bucket%2Findex%2Findex.one";
    const QUERY: &str = "tagKeys=a%20b&tagKeys=a%2Fb&tagKeys=a%20b";
    const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=host;x-amz-date, Signature=80779a6d5f1fc9f7aca162fd32c728ed1fb7c1c43df68efa585c3a030a2463fa";
    let (service, calls) = admission_service(false);
    let request = HttpRequest::builder()
        .method(Method::DELETE)
        .uri(format!("{PATH}?{QUERY}"))
        .header("host", "gateway-sdk-probe.invalid")
        .header("x-amz-date", "20260102T030405Z")
        .header("authorization", AUTHORIZATION)
        .header("content-length", "0")
        .body(http_body_util::Full::new(Bytes::new()))
        .expect("SDK fixture request");
    assert_protocol_error(service.call(request).await, ErrorCode::AccessDenied).await;
    let observations = calls.authorized.lock().expect("observations");
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].access_key, TEST_ACCESS_KEY);
    assert_eq!(observations[0].raw_path, PATH);
    assert_eq!(observations[0].raw_query, QUERY);
    let Request::UntagResource(input) = &observations[0].request else {
        panic!("UntagResource DTO expected");
    };
    assert_eq!(input.resource_arn, "arn:aws:s3vectors:us-east-1::bucket/protocol-bucket/index/index.one");
    assert_eq!(input.tag_keys, ["a b", "a/b", "a b"]);
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct TagKeysExecution {
    condition_tag_keys: Mutex<Vec<Vec<String>>>,
    executed_tag_keys: Mutex<Vec<Vec<String>>>,
}

struct TagKeysConditionApplication {
    fixture: crate::vector::access_core::test_support::ResourceFixture,
    policy: rustfs_policy::policy::Policy,
    recorded: Arc<TagKeysExecution>,
}

impl VectorGatewayService for TagKeysConditionApplication {
    type Authorization = Request;

    fn authorize<'a>(
        &'a self,
        context: &'a RequestContextView,
        request: &'a Request,
    ) -> BoxFuture<'a, Result<Self::Authorization, ProtocolError>> {
        let principal = context.principal().expect("verified principal");
        let target = self.fixture.target(request.operation());
        let conditions =
            crate::vector::access_core::vector_conditions(std::collections::HashMap::new(), request, &target, "us-east-1")
                .expect("valid resource target");
        let tag_keys = conditions.get("TagKeys").cloned().unwrap_or_default();
        self.recorded.condition_tag_keys.lock().expect("condition log").push(tag_keys);
        let (bucket, object) = crate::vector::access_core::test_support::target_names(&target);
        let bucket = bucket.to_owned();
        let object = object.to_owned();
        let account = principal.identity().access_key_id().to_owned();
        let action = rustfs_policy::policy::action::Action::S3VectorsAction(crate::vector::access_core::primary_action(
            request.operation(),
        ));
        Box::pin(async move {
            let empty_claims = std::collections::HashMap::new();
            let args = rustfs_policy::policy::Args {
                account: &account,
                groups: &None,
                action,
                bucket: &bucket,
                object: &object,
                conditions: &conditions,
                is_owner: false,
                claims: &empty_claims,
                deny_only: false,
            };
            if self.policy.is_allowed(&args).await {
                Ok(request.clone())
            } else {
                Err(ProtocolError::new(ErrorCode::AccessDenied, "Access denied"))
            }
        })
    }

    fn execute<'a>(
        &'a self,
        authorization: Self::Authorization,
        request: Request,
    ) -> BoxFuture<'a, Result<Response, ProtocolError>> {
        assert_eq!(authorization, request, "execute receives exact authorized UntagResource DTO");
        let Request::UntagResource(input) = request else {
            panic!("expected UntagResource request");
        };
        self.recorded
            .executed_tag_keys
            .lock()
            .expect("execution log")
            .push(input.tag_keys);
        Box::pin(async { Ok(Response::UntagResource(super::super::dto::UntagResourceOutput {})) })
    }
}

fn tag_keys_policy_service(allowed_tag_keys: &[&str]) -> (S3Service, Arc<TagKeysExecution>) {
    let policy_bytes = serde_json::to_vec(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3vectors:UntagResource"],
            "Resource": ["arn:aws:s3vectors:us-east-1::bucket/s6-vector-bucket/index/s6-index"],
            "Condition": {
                "ForAllValues:StringEquals": { "aws:TagKeys": allowed_tag_keys },
                "Null": { "aws:TagKeys": "false" }
            }
        }]
    }))
    .expect("policy JSON");
    let policy: rustfs_policy::policy::Policy = serde_json::from_slice(&policy_bytes).expect("valid aws:TagKeys policy");
    let credentials =
        Arc::new(StaticCredentials::new().with(Credentials::new(TEST_ACCESS_KEY, TEST_SECRET).expect("test identity")));
    let authenticator = SigV4Authenticator::new(credentials, RegionSet::new(["us-east-1"]).expect("test region"))
        .verify_paths_double_encoded_under(&["/tags"]);
    let builder = ServiceBuilder::for_service(SigService::S3Vectors)
        .authenticator(authenticator)
        .authorizer(allow_when(|request| serves(request.operation)))
        .clock_with_skew_ack(
            FixedClock::at_unix_seconds(TEST_EPOCH),
            ClockSkewAck::i_understand_a_skewed_clock_can_disable_signature_expiry(),
        );
    let recorded = Arc::new(TagKeysExecution::default());
    let backend = Arc::new(Backend::new(
        TagKeysConditionApplication {
            fixture: crate::vector::access_core::test_support::ResourceFixture::new(),
            policy,
            recorded: Arc::clone(&recorded),
        },
        "us-east-1".to_owned(),
    ));
    let service = register(builder, backend)
        .expect("Vector dialect")
        .build()
        .expect("Vector service");
    (service, recorded)
}

fn signed_double_encoded_tags_delete(
    location: SigLocation,
    wire_path: &str,
    query: &str,
) -> (HttpRequest<http_body_util::Full<Bytes>>, String) {
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::HOST, http::HeaderValue::from_static("gateway-sdk-probe.invalid"));
    let probe = HttpRequest::builder()
        .uri(wire_path)
        .header(http::header::HOST, "gateway-sdk-probe.invalid")
        .body(Bytes::new())
        .expect("probe");
    let accepted = WireRequest::accept(probe, &Limits::default()).expect("accepted host");
    let stamp = AmzDate::parse("20260102T030405Z").expect("fixed date");
    let scope = SigningScope::new(stamp.day(), "us-east-1", SigService::S3Vectors).expect("scope");
    let mut signer = SigV4Signer::new(SigningCredentials::new(TEST_ACCESS_KEY, TEST_SECRET).expect("credentials"), scope);
    let double_encoded_path = wire_path.replace('%', "%25");
    let signing = SigningRequest::new(
        &Method::DELETE,
        &double_encoded_path,
        query,
        &headers,
        accepted.host().raw_for_signing(),
        match location {
            SigLocation::Query => PayloadMode::Unsigned,
            _ => PayloadMode::Empty,
        },
        stamp,
    );
    let signed = match location {
        SigLocation::Header => signer.sign_headers(&signing),
        SigLocation::Query => signer.presign(&signing, 300),
        _ => panic!("test supports header and query signatures"),
    }
    .expect("signed request");
    let canonical = signed.canonical_request().to_owned();
    let target = if signed.query().is_empty() {
        wire_path.to_owned()
    } else {
        format!("{wire_path}?{}", signed.query())
    };
    let mut request = HttpRequest::builder().method(Method::DELETE).uri(target);
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    (
        request
            .body(http_body_util::Full::new(Bytes::new()))
            .expect("signed DELETE request"),
        canonical,
    )
}

#[tokio::test]
async fn untag_resource_sigv4_query_plus_aligns_signature_route_conditions_and_handler() {
    const WIRE_PATH: &str = "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fs6-vector-bucket%2Findex%2Fs6-index";
    const QUERY: &str = "tagKeys=a+b&tagKeys=a%2Bb&tagKeys=a%20b";

    for location in [SigLocation::Header, SigLocation::Query] {
        let (service, recorded) = tag_keys_policy_service(&["a+b", "a b"]);
        let (request, canonical) = signed_double_encoded_tags_delete(location, WIRE_PATH, QUERY);
        assert!(
            canonical.contains("tagKeys=a%20b&tagKeys=a%2Bb&tagKeys=a%2Bb"),
            "SigV4 canonical query must treat '+' and '%2B' identically and keep '%20' distinct: {canonical}"
        );
        let response = service.call(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(
            response
                .into_body()
                .collect()
                .await
                .expect("empty body")
                .to_bytes()
                .is_empty()
        );
        assert_eq!(
            *recorded.condition_tag_keys.lock().expect("condition log"),
            vec![vec!["a+b".to_owned(), "a+b".to_owned(), "a b".to_owned()]]
        );
        assert_eq!(
            *recorded.executed_tag_keys.lock().expect("execution log"),
            vec![vec!["a+b".to_owned(), "a+b".to_owned(), "a b".to_owned()]]
        );
    }

    // Regression guard: a policy permitting only "a b" must deny `tagKeys=a+b`.
    let (space_only_service, space_only_recorded) = tag_keys_policy_service(&["a b"]);
    let (plus_request, _) = signed_double_encoded_tags_delete(SigLocation::Header, WIRE_PATH, "tagKeys=a+b");
    assert_protocol_error(space_only_service.call(plus_request).await, ErrorCode::AccessDenied).await;
    assert_eq!(
        *space_only_recorded.condition_tag_keys.lock().expect("condition log"),
        vec![vec!["a+b".to_owned()]]
    );
    assert!(
        space_only_recorded
            .executed_tag_keys
            .lock()
            .expect("execution log")
            .is_empty()
    );

    // Wire-spelling parity: a signature over `tagKeys=a+b` is rejected if rewritten to `a%20b`,
    // and accepted with identical condition/handler semantics if rewritten to `a%2Bb`.
    let (plus_only_service, plus_only_recorded) = tag_keys_policy_service(&["a+b"]);
    let (mut tampered_to_space, _) = signed_double_encoded_tags_delete(SigLocation::Header, WIRE_PATH, "tagKeys=a+b");
    *tampered_to_space.uri_mut() = format!("{WIRE_PATH}?tagKeys=a%20b").parse().expect("URI");
    let refused = plus_only_service.call(tampered_to_space).await;
    assert!(!refused.status().is_success());
    assert!(refused.extensions().get::<rustfs_gateway::S3Error>().is_some());
    assert!(
        plus_only_recorded
            .condition_tag_keys
            .lock()
            .expect("condition log")
            .is_empty()
    );

    let (mut respelled_percent_plus, _) = signed_double_encoded_tags_delete(SigLocation::Header, WIRE_PATH, "tagKeys=a+b");
    *respelled_percent_plus.uri_mut() = format!("{WIRE_PATH}?tagKeys=a%2Bb").parse().expect("URI");
    let accepted = plus_only_service.call(respelled_percent_plus).await;
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(
        *plus_only_recorded.condition_tag_keys.lock().expect("condition log"),
        vec![vec!["a+b".to_owned()]]
    );
    assert_eq!(
        *plus_only_recorded.executed_tag_keys.lock().expect("execution log"),
        vec![vec!["a+b".to_owned()]]
    );
}

#[tokio::test]
async fn signed_unknown_route_and_wrong_method_do_not_enter_application() {
    let (service, calls) = admission_service(true);
    for (method, path) in [(Method::POST, "/UnknownAction"), (Method::GET, "/GetVectorBucket")] {
        let request = signed_request(
            SigLocation::Header,
            method,
            path,
            "",
            Bytes::from_static(BUCKET_BODY),
            ("us-east-1", SigService::S3Vectors),
            TEST_SECRET,
        );
        let response = service.call(request).await;
        let error = response
            .extensions()
            .get::<rustfs_gateway::S3Error>()
            .expect("typed route refusal");
        assert_eq!(error.code(), Some(&rustfs_gateway::ErrorCode::NOT_IMPLEMENTED));
        assert!(!response.status().is_success());
    }
    assert!(calls.authorized.lock().expect("observations").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

// Captured with botocore 1.42.97 and service model 053554bf4606d7affaad0d40e9f3367b7a7ff771.
// The signature covers every original body byte and every signed header below.
struct SdkFixture {
    operation: Operation,
    method: &'static str,
    target: &'static str,
    body: &'static str,
    headers: &'static [(&'static str, &'static str)],
}

static SDK_FIXTURES: &[SdkFixture] = &[
    SdkFixture {
        operation: Operation::CreateIndex,
        method: "POST",
        target: "/CreateIndex",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"dataType\": \"float32\", \"dimension\": 3, \"distanceMetric\": \"cosine\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=735bd36ae359a9123ef78dde7822ada845b73ffad4b670bd200c3252ae719fd0",
            ),
            ("content-length", "132"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::CreateVectorBucket,
        method: "POST",
        target: "/CreateVectorBucket",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"tags\": {\"a b\": \"sdk-probe\"}}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fd1d94a7df4530eb1c53770debcbce1f639edb7a06372674e7b446ef6ab030f4",
            ),
            ("content-length", "69"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::DeleteIndex,
        method: "POST",
        target: "/DeleteIndex",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=dd0f4fe18843afb1ba7189c2b9f9b7a343c716cca2bed85ea5a7473146a93a05",
            ),
            ("content-length", "65"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::DeleteVectorBucket,
        method: "POST",
        target: "/DeleteVectorBucket",
        body: "{\"vectorBucketName\": \"protocol-bucket\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=debace7f0f2688de080b6f4e100fe7662cc92839ab5f0f3a9be61f42dbe03484",
            ),
            ("content-length", "39"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::DeleteVectorBucketPolicy,
        method: "POST",
        target: "/DeleteVectorBucketPolicy",
        body: "{\"vectorBucketName\": \"protocol-bucket\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=1f8d88512521ca2b8a54b4913ff00734692322ae781291431d752160b3ccd601",
            ),
            ("content-length", "39"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::DeleteVectors,
        method: "POST",
        target: "/DeleteVectors",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"keys\": [\"key/one\"]}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=e61c2e5bb79a1866b6c93b4126e1aae83ce218daeb9989a61ed0710694cf10f2",
            ),
            ("content-length", "86"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::GetIndex,
        method: "POST",
        target: "/GetIndex",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=bbedbf85901335fc96dfb7747cc8b0865df16274603c19d86988485f7029d18a",
            ),
            ("content-length", "65"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::GetVectorBucket,
        method: "POST",
        target: "/GetVectorBucket",
        body: "{\"vectorBucketName\": \"protocol-bucket\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=2f0846ef48e957a1e258373c817e4a5e84b3424a7836145f81a461aa6b282bf1",
            ),
            ("content-length", "39"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::GetVectorBucketPolicy,
        method: "POST",
        target: "/GetVectorBucketPolicy",
        body: "{\"vectorBucketName\": \"protocol-bucket\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=0e8f71f0d199ed2447e827c907dc3fc39d01ff04f5956df633343bfdd9953137",
            ),
            ("content-length", "39"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::GetVectors,
        method: "POST",
        target: "/GetVectors",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"keys\": [\"key/one\"], \"returnData\": true, \"returnMetadata\": true}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=8f7a67e7d504d5acea2c7b3af975d6368236e9fb3d5f6ed481aa9ca60435a526",
            ),
            ("content-length", "130"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::ListIndexes,
        method: "POST",
        target: "/ListIndexes",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"maxResults\": 2}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=cf07ace48c052c378bf763eb5448295b8a3999dd2e50fd8867fe5cc02dfbab6e",
            ),
            ("content-length", "56"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::ListTagsForResource,
        method: "GET",
        target: "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fprotocol-bucket%2Findex%2Findex.one",
        body: "",
        headers: &[
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=host;x-amz-date, Signature=176b7ae0324f45fcc4b1a2cdc933c4224a59c6c21ef08ca2ca1966d6feb13fd3",
            ),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::ListVectorBuckets,
        method: "POST",
        target: "/ListVectorBuckets",
        body: "{\"maxResults\": 2}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=499b2b478d38e2acc6d796e216889d3773c59989bc52d355c34663304b8558ec",
            ),
            ("content-length", "17"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::ListVectors,
        method: "POST",
        target: "/ListVectors",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"maxResults\": 2, \"returnData\": true}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=bdeec795cdc13d4a74bd947314b1fbf3ba3ede1bb399abd8fc7ce9a87a95328d",
            ),
            ("content-length", "102"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::PutVectorBucketDefaultIndexMode,
        method: "POST",
        target: "/PutVectorBucketDefaultIndexMode",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"defaultIndexMode\": \"ENHANCED\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=90e9359d0717dc5f63d547a696e47b19f0d0771cd5394625d22a751c9d924fe8",
            ),
            ("content-length", "71"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::PutVectorBucketPolicy,
        method: "POST",
        target: "/PutVectorBucketPolicy",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"policy\": \"{\\\"Version\\\":\\\"2012-10-17\\\",\\\"Statement\\\":[]}\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=3846a4cac80c98c013f936431018ca61b982b98e9b432851fb8938e0113a6dad",
            ),
            ("content-length", "98"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::PutVectors,
        method: "POST",
        target: "/PutVectors",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"vectors\": [{\"key\": \"key/one\", \"data\": {\"float32\": [0.25, 0.5, 0.75]}, \"metadata\": {\"label\": \"Unicode \\u6d4b\\u8bd5 / quoted \\\"value\\\"\"}}]}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=cf6c34048aa13c7ddd79aec67eba6fb0ad800eecb63cfa41dc410d22c40b9124",
            ),
            ("content-length", "204"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::QueryVectors,
        method: "POST",
        target: "/QueryVectors",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"topK\": 2, \"queryVector\": {\"float32\": [0.25, 0.5, 0.75]}, \"returnDistance\": true}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=e8994a44a5a0bb6dd324cf254079907815000aac311f11c72abd65bec2b025ca",
            ),
            ("content-length", "147"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::TagResource,
        method: "POST",
        target: "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fprotocol-bucket%2Findex%2Findex.one",
        body: "{\"tags\": {\"a b\": \"one\", \"a/b\": \"two\"}}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=0fb83f0a83b9559f333714590e763450c42d196b51af2e348d6c543f5f5a7252",
            ),
            ("content-length", "38"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::UntagResource,
        method: "DELETE",
        target: "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fprotocol-bucket%2Findex%2Findex.one?tagKeys=a%20b&tagKeys=a%2Fb&tagKeys=a%20b",
        body: "",
        headers: &[
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=host;x-amz-date, Signature=80779a6d5f1fc9f7aca162fd32c728ed1fb7c1c43df68efa585c3a030a2463fa",
            ),
            ("content-length", "0"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
    SdkFixture {
        operation: Operation::UpdateIndexMode,
        method: "POST",
        target: "/UpdateIndexMode",
        body: "{\"vectorBucketName\": \"protocol-bucket\", \"indexName\": \"index.one\", \"indexMode\": \"ENHANCED\"}",
        headers: &[
            ("content-type", "application/json"),
            ("x-amz-date", "20260102T030405Z"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=9b054f4c1689adcd8b6f2efcf78a4c75c01d07c17a38d6c604143f36c1a869f8",
            ),
            ("content-length", "90"),
            ("host", "gateway-sdk-probe.invalid"),
        ],
    },
];

fn sdk_request(fixture: &SdkFixture) -> HttpRequest<http_body_util::Full<Bytes>> {
    let mut request = HttpRequest::builder().method(fixture.method).uri(fixture.target);
    for (name, value) in fixture.headers {
        request = request.header(*name, *value);
    }
    request
        .body(http_body_util::Full::new(Bytes::copy_from_slice(fixture.body.as_bytes())))
        .expect("captured SDK request")
}

#[tokio::test]
async fn all_sdk_operations_reach_exact_handlers_over_a_real_socket() {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (service, calls) = admission_service(true);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("loopback listener");
    let address = listener.local_addr().expect("listener address");
    let received = Arc::new(AtomicUsize::new(0));
    let server_received = Arc::clone(&received);
    let server = tokio::spawn(async move {
        for _ in SDK_FIXTURES {
            let (stream, _) = tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                .await
                .expect("SDK connection deadline")
                .expect("SDK connection");
            let service = service.clone();
            let received = Arc::clone(&server_received);
            let handler = service_fn(move |request| {
                let service = service.clone();
                received.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, std::convert::Infallible>(service.call(request).await) }
            });
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), handler)
                .await
                .expect("HTTP connection completes");
        }
    });

    for (index, fixture) in SDK_FIXTURES.iter().enumerate() {
        let mut stream = tokio::net::TcpStream::connect(address).await.expect("loopback connection");
        let mut wire = format!("{} {} HTTP/1.1\r\n", fixture.method, fixture.target);
        for (name, value) in fixture.headers {
            wire.push_str(&format!("{name}: {value}\r\n"));
        }
        wire.push_str("connection: close\r\n\r\n");
        stream.write_all(wire.as_bytes()).await.expect("request headers");
        stream.write_all(fixture.body.as_bytes()).await.expect("original signed body");
        let mut response = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(10), stream.read_to_end(&mut response))
            .await
            .expect("HTTP response deadline")
            .expect("HTTP response");
        let split = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("HTTP headers")
            + 4;
        let headers = std::str::from_utf8(&response[..split]).expect("response headers");
        assert!(headers.starts_with("HTTP/1.1 503 "), "{}: {headers}", fixture.operation.name());
        assert!(headers.to_ascii_lowercase().contains("content-type: application/json\r\n"));
        let body: serde_json::Value = serde_json::from_slice(&response[split..]).expect("REST JSON response");
        assert_eq!(body["__type"], "ServiceUnavailableException");
        assert_eq!(received.load(Ordering::SeqCst), index + 1);
        assert_eq!(calls.executed.load(Ordering::SeqCst), index + 1);
        let observations = calls.authorized.lock().expect("handler observations");
        assert_eq!(observations.len(), index + 1);
        let observed = &observations[index];
        assert_eq!(observed.operation, format!("s3vectors:{}", fixture.operation.name()));
        assert_eq!(observed.request.operation(), fixture.operation);
        assert_eq!(observed.access_key, TEST_ACCESS_KEY);
        assert_eq!(observed.raw_path, fixture.target.split('?').next().unwrap());
        assert_eq!(observed.raw_query, fixture.target.split_once('?').map_or("", |(_, query)| query));
    }
    server.await.expect("server task completes");
    assert_eq!(received.load(Ordering::SeqCst), 21);
    assert_eq!(calls.executed.load(Ordering::SeqCst), 21);
}

struct OrdinaryS3Handler(AtomicUsize);

impl Handler<rustfs_gateway::dto::GetObject> for OrdinaryS3Handler {
    async fn call(&self, request: Req<rustfs_gateway::dto::GetObject>) -> HandlerResult<rustfs_gateway::dto::GetObject> {
        assert_eq!(request.context().operation(), "GetObject");
        assert_eq!(request.context().raw_path(), "/tags/object");
        assert!(request.context().bucket().is_some());
        assert!(request.context().key().is_some());
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(rustfs_gateway::dto::GetObjectOutput::default()))
    }
}

#[tokio::test]
async fn ordinary_s3_get_object_stays_isolated_from_vectors_routes_and_signatures() {
    let backend = Arc::new(OrdinaryS3Handler(AtomicUsize::new(0)));
    let credentials =
        Arc::new(StaticCredentials::new().with(Credentials::new(TEST_ACCESS_KEY, TEST_SECRET).expect("test identity")));
    let service = ServiceBuilder::new()
        .authenticator(SigV4Authenticator::new(credentials, RegionSet::new(["us-east-1"]).unwrap()))
        .authorizer(allow_when(|request| request.operation == "GetObject"))
        .clock_with_skew_ack(
            FixedClock::at_unix_seconds(TEST_EPOCH),
            ClockSkewAck::i_understand_a_skewed_clock_can_disable_signature_expiry(),
        )
        .register::<rustfs_gateway::dto::GetObject, _>(Arc::clone(&backend))
        .build()
        .expect("ordinary S3 service");
    let response = service.call(s3_control_request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);

    let tags = SDK_FIXTURES
        .iter()
        .find(|fixture| fixture.operation == Operation::ListTagsForResource)
        .expect("SDK tags fixture");
    let response = service.call(sdk_request(tags)).await;
    assert!(!response.status().is_success());
    assert!(response.extensions().get::<rustfs_gateway::S3Error>().is_some());
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);

    let (vectors, calls) = admission_service(true);
    let response = vectors.call(s3_control_request()).await;
    assert!(!response.status().is_success());
    assert!(response.extensions().get::<rustfs_gateway::S3Error>().is_some());
    assert!(calls.authorized.lock().expect("Vector calls").is_empty());
    assert_eq!(calls.executed.load(Ordering::SeqCst), 0);
}

fn s3_control_request() -> HttpRequest<http_body_util::Full<Bytes>> {
    HttpRequest::builder()
        .method("GET")
        .uri("/tags/object")
        .header("x-amz-checksum-mode", "ENABLED")
        .header("x-amz-date", "20260102T030405Z")
        .header("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-checksum-mode;x-amz-content-sha256;x-amz-date, Signature=99b0aed6970064ec744c8bca12525d9d69d03856540d49f4090227636877a8a0")
        .header("host", "gateway-sdk-probe.invalid")
        .body(http_body_util::Full::new(Bytes::new()))
        .expect("SDK ordinary S3 request")
}
