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

//! Gateway routing and adapters for the independently configured S3 Vectors service.

use super::{ErrorCode, Operation, ProtocolError, Request, Response, Route};
use bytes::Bytes;
use http::{Method, Response as HttpResponse};
use rustfs_gateway::sig::{SigFamily, SigLocation};
use rustfs_gateway::{
    AuthRequirement, Body, BoxFuture, CodecError, DerivedResourceError, EncodedResponse, Handler, HandlerDeadlineClass,
    HandlerResult, MetaView, NoDerived, Operation as GatewayOperation, OperationCodec, OperationFloor, OperationSpec, Predicate,
    Req, RequestBody, RequestBodyMode, RequestContextView, ResourceShape, Resp, ResponseBody, ServiceBuilder, SigService,
};
use rustfs_gateway_core::{ClaimedRoute, ClaimedRow, Dialect, DialectError, DialectOverlay, DialectRoute, OverlayRow, PathClaim};
use std::sync::Arc;

// The protocol constant is a fixed 20 MiB limit and fits in u64.
pub(crate) const MAX_BODY_BYTES: u64 = super::MAX_REQUEST_BODY_BYTES as u64;
const SERVICE: SigService = SigService::S3Vectors;
const EVIDENCE: &[&str] = &[
    "https://github.com/boto/botocore/blob/053554bf4606d7affaad0d40e9f3367b7a7ff771/botocore/data/s3vectors/2025-07-15/service-2.json",
];

/// The application must implement both methods using its real IAM and Vector Service.
/// Authorization must include current policies, resource policy, STS and additional actions.
/// The authorization value binds the verified caller and this exact request's resource facts.
pub(crate) trait VectorGatewayService: Send + Sync + 'static {
    type Authorization: Send;
    fn authorize<'a>(
        &'a self,
        context: &'a RequestContextView,
        request: &'a Request,
    ) -> BoxFuture<'a, Result<Self::Authorization, ProtocolError>>;
    fn execute<'a>(
        &'a self,
        authorization: Self::Authorization,
        request: Request,
    ) -> BoxFuture<'a, Result<Response, ProtocolError>>;
}

pub(crate) struct Backend<S> {
    service: S,
    region: String,
}

impl<S> Backend<S> {
    pub(crate) fn new(service: S, region: String) -> Self {
        Self { service, region }
    }
}

struct Action<const N: usize>;

const fn spec(name: &'static str) -> OperationSpec {
    OperationSpec::builder(name, 200, None)
        .handler_deadline_class(HandlerDeadlineClass::Standard)
        .auth(AuthRequirement::new(name, ResourceShape::Service))
        .build()
}

const NAMES: [&str; 21] = [
    "s3vectors:CreateIndex",
    "s3vectors:CreateVectorBucket",
    "s3vectors:DeleteIndex",
    "s3vectors:DeleteVectorBucket",
    "s3vectors:DeleteVectorBucketPolicy",
    "s3vectors:DeleteVectors",
    "s3vectors:GetIndex",
    "s3vectors:GetVectorBucket",
    "s3vectors:GetVectorBucketPolicy",
    "s3vectors:GetVectors",
    "s3vectors:ListIndexes",
    "s3vectors:ListTagsForResource",
    "s3vectors:ListVectorBuckets",
    "s3vectors:ListVectors",
    "s3vectors:PutVectorBucketDefaultIndexMode",
    "s3vectors:PutVectorBucketPolicy",
    "s3vectors:PutVectors",
    "s3vectors:QueryVectors",
    "s3vectors:TagResource",
    "s3vectors:UntagResource",
    "s3vectors:UpdateIndexMode",
];

/// The independent service's early gate only admits operations this backend handles.
/// Application IAM authorization still runs for every admitted request.
pub(crate) fn serves(operation: &str) -> bool {
    NAMES.contains(&operation)
}

const OPERATIONS: [Operation; 21] = Operation::ALL;
static SPECS: [OperationSpec; 21] = [
    spec(NAMES[0]),
    spec(NAMES[1]),
    spec(NAMES[2]),
    spec(NAMES[3]),
    spec(NAMES[4]),
    spec(NAMES[5]),
    spec(NAMES[6]),
    spec(NAMES[7]),
    spec(NAMES[8]),
    spec(NAMES[9]),
    spec(NAMES[10]),
    spec(NAMES[11]),
    spec(NAMES[12]),
    spec(NAMES[13]),
    spec(NAMES[14]),
    spec(NAMES[15]),
    spec(NAMES[16]),
    spec(NAMES[17]),
    spec(NAMES[18]),
    spec(NAMES[19]),
    spec(NAMES[20]),
];
static FLOORS: [OperationFloor; 21] = [
    OperationFloor::builtin_presigned(NAMES[0], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[1], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[2], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[3], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[4], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[5], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[6], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[7], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[8], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[9], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[10], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[11], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[12], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[13], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[14], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[15], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[16], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[17], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[18], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[19], SERVICE).require_sigv4_under_every_policy(),
    OperationFloor::builtin_presigned(NAMES[20], SERVICE).require_sigv4_under_every_policy(),
];

impl<const N: usize> GatewayOperation for Action<N> {
    const NAME: &'static str = NAMES[N];
    type Input = Bytes;
    type Output = HttpResponse<Vec<u8>>;
    type DerivedResources = NoDerived;
    fn derive_resources(_: &Bytes) -> Result<NoDerived, DerivedResourceError> {
        Ok(NoDerived)
    }
    fn seal_derived_input(_: &mut Bytes) {}
    fn spec() -> &'static OperationSpec {
        &SPECS[N]
    }
    fn floor() -> &'static OperationFloor {
        &FLOORS[N]
    }
}

impl<const N: usize> OperationCodec for Action<N> {
    const REQUEST_BODY: RequestBodyMode = RequestBodyMode::Full;
    fn decode(_: &MetaView<'_>, body: RequestBody) -> Result<Bytes, CodecError> {
        body.into_buffered()
    }
    fn encode(output: Self::Output, _: &MetaView<'_>, _: u16) -> Result<EncodedResponse, CodecError> {
        let (parts, body) = output.into_parts();
        Ok(EncodedResponse {
            status: parts.status,
            headers: parts.headers,
            body: ResponseBody::Complete(body),
        })
    }
}

impl<S: VectorGatewayService, const N: usize> Handler<Action<N>> for Backend<S> {
    async fn call(&self, request: Req<Action<N>>) -> HandlerResult<Action<N>> {
        let answer = self.dispatch::<N>(&request).await;
        let response = match answer.and_then(Response::into_response) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
        let status = response.status().as_u16();
        Ok(Resp::with_status(response, status))
    }
}

impl<S: VectorGatewayService> Backend<S> {
    async fn dispatch<const N: usize>(&self, request: &Req<Action<N>>) -> Result<Response, ProtocolError> {
        let context = request.context();
        let principal = context.principal().ok_or_else(access_denied)?;
        let scope = principal.verified_scope().ok_or_else(access_denied)?;
        if principal.scheme().family() != SigFamily::V4
            || !matches!(principal.scheme().location(), SigLocation::Header | SigLocation::Query)
            || principal.scheme().service() != SERVICE
            || scope.service() != "s3vectors"
            || scope.region() != self.region
        {
            return Err(access_denied());
        }
        let route = Route::recognize(context.method(), context.raw_path())
            .filter(|route| route.operation() == OPERATIONS[N])
            .ok_or_else(|| ProtocolError::validation("request", "Unknown operation route"))?;
        let input = route.parse(Some(context.raw_query()), request.input(), &self.region)?;
        let authorization = self.service.authorize(context, &input).await?;
        self.service.execute(authorization, input).await
    }
}

fn access_denied() -> ProtocolError {
    ProtocolError::new(ErrorCode::AccessDenied, "Access denied")
}

/// Replace a classified error in place, retaining every transport extension and trace header.
/// The caller owns classification; this function never infers an error type from HTTP status.
pub(crate) fn replace_error_response(response: &mut HttpResponse<Body>, error: ProtocolError) {
    let (parts, body) = error.into_response().into_parts();
    *response.status_mut() = parts.status;
    let body_len = body.len();
    *response.body_mut() = Body::from(body);
    for name in [
        http::header::CONTENT_LENGTH,
        http::header::CONTENT_ENCODING,
        http::header::TRANSFER_ENCODING,
        http::header::TRAILER,
    ] {
        response.headers_mut().remove(name);
    }
    response.headers_mut().extend(parts.headers);
    if let Ok(length) = http::HeaderValue::from_str(&body_len.to_string()) {
        response.headers_mut().insert(http::header::CONTENT_LENGTH, length);
    }
}

const SELECTOR_0: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/CreateIndex")];
const SELECTOR_1: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/CreateVectorBucket")];
const SELECTOR_2: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/DeleteIndex")];
const SELECTOR_3: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/DeleteVectorBucket")];
const SELECTOR_4: &[Predicate] = &[
    Predicate::Method(Method::POST),
    Predicate::PathLiteral("/DeleteVectorBucketPolicy"),
];
const SELECTOR_5: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/DeleteVectors")];
const SELECTOR_6: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/GetIndex")];
const SELECTOR_7: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/GetVectorBucket")];
const SELECTOR_8: &[Predicate] = &[
    Predicate::Method(Method::POST),
    Predicate::PathLiteral("/GetVectorBucketPolicy"),
];
const SELECTOR_9: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/GetVectors")];
const SELECTOR_10: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/ListIndexes")];
const ROWS_11: &[ClaimedRow] = &[ClaimedRow {
    template: "/tags/{+resource_arn}",
    selector: &[Predicate::Method(Method::GET)],
}];
const SELECTOR_12: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/ListVectorBuckets")];
const SELECTOR_13: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/ListVectors")];
const SELECTOR_14: &[Predicate] = &[
    Predicate::Method(Method::POST),
    Predicate::PathLiteral("/PutVectorBucketDefaultIndexMode"),
];
const SELECTOR_15: &[Predicate] = &[
    Predicate::Method(Method::POST),
    Predicate::PathLiteral("/PutVectorBucketPolicy"),
];
const SELECTOR_16: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/PutVectors")];
const SELECTOR_17: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/QueryVectors")];
const ROWS_18: &[ClaimedRow] = &[ClaimedRow {
    template: "/tags/{+resource_arn}",
    selector: &[Predicate::Method(Method::POST)],
}];
const ROWS_19: &[ClaimedRow] = &[ClaimedRow {
    template: "/tags/{+resource_arn}",
    selector: &[Predicate::Method(Method::DELETE)],
}];
const SELECTOR_20: &[Predicate] = &[Predicate::Method(Method::POST), Predicate::PathLiteral("/UpdateIndexMode")];

static OVERLAY: DialectOverlay = DialectOverlay {
    name: "rustfs-s3vectors",
    vendor: "s3vectors",
    claims: &[PathClaim {
        prefix: "/tags",
        reason: "The dedicated S3 Vectors service owns tag resource routes.",
        evidence: EVIDENCE,
    }],
    operations: &[
        OverlayRow {
            name: NAMES[0],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/CreateIndex")"#,
            action: NAMES[0],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[1],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/CreateVectorBucket")"#,
            action: NAMES[1],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[2],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/DeleteIndex")"#,
            action: NAMES[2],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[3],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/DeleteVectorBucket")"#,
            action: NAMES[3],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[4],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/DeleteVectorBucketPolicy")"#,
            action: NAMES[4],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[5],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/DeleteVectors")"#,
            action: NAMES[5],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[6],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/GetIndex")"#,
            action: NAMES[6],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[7],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/GetVectorBucket")"#,
            action: NAMES[7],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[8],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/GetVectorBucketPolicy")"#,
            action: NAMES[8],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[9],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/GetVectors")"#,
            action: NAMES[9],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[10],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/ListIndexes")"#,
            action: NAMES[10],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[11],
            precedence: 50,
            selector: r#"PathTemplate("/tags/{+resource_arn}") ∧ Method(GET)"#,
            action: NAMES[11],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[12],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/ListVectorBuckets")"#,
            action: NAMES[12],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[13],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/ListVectors")"#,
            action: NAMES[13],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[14],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/PutVectorBucketDefaultIndexMode")"#,
            action: NAMES[14],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[15],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/PutVectorBucketPolicy")"#,
            action: NAMES[15],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[16],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/PutVectors")"#,
            action: NAMES[16],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[17],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/QueryVectors")"#,
            action: NAMES[17],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[18],
            precedence: 50,
            selector: r#"PathTemplate("/tags/{+resource_arn}") ∧ Method(POST)"#,
            action: NAMES[18],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[19],
            precedence: 50,
            selector: r#"PathTemplate("/tags/{+resource_arn}") ∧ Method(DELETE)"#,
            action: NAMES[19],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
        OverlayRow {
            name: NAMES[20],
            precedence: 50,
            selector: r#"Method(POST) ∧ PathLiteral("/UpdateIndexMode")"#,
            action: NAMES[20],
            resource: ResourceShape::Service,
            success_status: 200,
            anonymous: false,
            evidence: EVIDENCE,
        },
    ],
};

pub(crate) fn dialect() -> Result<Dialect, Vec<DialectError>> {
    Dialect::assemble_for_service(&OVERLAY, SERVICE)
        .declare::<Action<0>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_0,
            path_shape: "/CreateIndex",
            shadows: &[],
        })
        .declare::<Action<1>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_1,
            path_shape: "/CreateVectorBucket",
            shadows: &[],
        })
        .declare::<Action<2>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_2,
            path_shape: "/DeleteIndex",
            shadows: &[],
        })
        .declare::<Action<3>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_3,
            path_shape: "/DeleteVectorBucket",
            shadows: &[],
        })
        .declare::<Action<4>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_4,
            path_shape: "/DeleteVectorBucketPolicy",
            shadows: &[],
        })
        .declare::<Action<5>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_5,
            path_shape: "/DeleteVectors",
            shadows: &[],
        })
        .declare::<Action<6>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_6,
            path_shape: "/GetIndex",
            shadows: &[],
        })
        .declare::<Action<7>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_7,
            path_shape: "/GetVectorBucket",
            shadows: &[],
        })
        .declare::<Action<8>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_8,
            path_shape: "/GetVectorBucketPolicy",
            shadows: &[],
        })
        .declare::<Action<9>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_9,
            path_shape: "/GetVectors",
            shadows: &[],
        })
        .declare::<Action<10>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_10,
            path_shape: "/ListIndexes",
            shadows: &[],
        })
        .declare_claimed::<Action<11>>(ClaimedRoute {
            precedence: 50,
            rows: ROWS_11,
            shadows: &[],
            bucket_param: None,
        })
        .declare::<Action<12>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_12,
            path_shape: "/ListVectorBuckets",
            shadows: &[],
        })
        .declare::<Action<13>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_13,
            path_shape: "/ListVectors",
            shadows: &[],
        })
        .declare::<Action<14>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_14,
            path_shape: "/PutVectorBucketDefaultIndexMode",
            shadows: &[],
        })
        .declare::<Action<15>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_15,
            path_shape: "/PutVectorBucketPolicy",
            shadows: &[],
        })
        .declare::<Action<16>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_16,
            path_shape: "/PutVectors",
            shadows: &[],
        })
        .declare::<Action<17>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_17,
            path_shape: "/QueryVectors",
            shadows: &[],
        })
        .declare_claimed::<Action<18>>(ClaimedRoute {
            precedence: 50,
            rows: ROWS_18,
            shadows: &[],
            bucket_param: None,
        })
        .declare_claimed::<Action<19>>(ClaimedRoute {
            precedence: 50,
            rows: ROWS_19,
            shadows: &[],
            bucket_param: None,
        })
        .declare::<Action<20>>(DialectRoute {
            precedence: 50,
            selector: SELECTOR_20,
            path_shape: "/UpdateIndexMode",
            shadows: &[],
        })
        .build()
}

pub(crate) fn register<S: VectorGatewayService>(
    builder: ServiceBuilder,
    backend: Arc<Backend<S>>,
) -> Result<ServiceBuilder, Vec<DialectError>> {
    let builder = builder.max_buffered_body_bytes(MAX_BODY_BYTES).dialect(&dialect()?);
    Ok(builder
        .register::<Action<0>, _>(Arc::clone(&backend))
        .register::<Action<1>, _>(Arc::clone(&backend))
        .register::<Action<2>, _>(Arc::clone(&backend))
        .register::<Action<3>, _>(Arc::clone(&backend))
        .register::<Action<4>, _>(Arc::clone(&backend))
        .register::<Action<5>, _>(Arc::clone(&backend))
        .register::<Action<6>, _>(Arc::clone(&backend))
        .register::<Action<7>, _>(Arc::clone(&backend))
        .register::<Action<8>, _>(Arc::clone(&backend))
        .register::<Action<9>, _>(Arc::clone(&backend))
        .register::<Action<10>, _>(Arc::clone(&backend))
        .register::<Action<11>, _>(Arc::clone(&backend))
        .register::<Action<12>, _>(Arc::clone(&backend))
        .register::<Action<13>, _>(Arc::clone(&backend))
        .register::<Action<14>, _>(Arc::clone(&backend))
        .register::<Action<15>, _>(Arc::clone(&backend))
        .register::<Action<16>, _>(Arc::clone(&backend))
        .register::<Action<17>, _>(Arc::clone(&backend))
        .register::<Action<18>, _>(Arc::clone(&backend))
        .register::<Action<19>, _>(Arc::clone(&backend))
        .register::<Action<20>, _>(Arc::clone(&backend)))
}

#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
