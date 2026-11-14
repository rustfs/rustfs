// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{
    AccessMarkers, Action, AuthorizationRequest, AuthzPrincipal, AuthzTarget, HeaderMap, ReqInfo, S3Action, VerifiedSignature,
    build_args, build_conditions, get_request_auth_type_with_query, request_authz_facts,
};
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::facts::AuthTypeCondition;
use http::{Extensions, Method};
use rustfs_credentials::Credentials;
use rustfs_policy::policy::Policy;

fn accepted_query_caller(query: &str) -> (RequestEnvelope, Credentials) {
    let credentials = Credentials {
        access_key: "alice".to_owned(),
        ..Credentials::default()
    };
    let uri: http::Uri = format!("/rustfs/admin/v4/account/permissions?{query}")
        .parse()
        .expect("authenticated caller URI");
    let headers = HeaderMap::new();
    let signature = VerifiedSignature::from_auth_type(get_request_auth_type_with_query(&headers, uri.query()));
    let mut extensions = Extensions::new();
    AccessMarkers::new("AccountPermissions", signature).install(&mut extensions);
    let envelope = RequestEnvelope::from_parts(
        RequestHead {
            method: Method::POST,
            uri,
            headers,
            credentials: Some(RequestCredentials::without_secret_key(credentials.access_key.clone(), None)),
            region: None,
            service: None,
        },
        extensions,
    );
    (envelope, credentials)
}

#[tokio::test]
async fn permission_probe_preserves_query_signing_family_without_copying_capabilities() {
    let policy: Policy = serde_json::from_str(
        r#"{
        "Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::alice-data/*",
            "Condition":{"StringEquals":{"s3:signatureversion":"AWS4-HMAC-SHA256"}}
        }]
    }"#,
    )
    .expect("signature-family policy");
    for (query, signing_version, allowed) in [
        (
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=alice%2F20261114%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=accepted&X-Amz-SignedHeaders=host",
            "AWS4-HMAC-SHA256",
            true,
        ),
        ("AWSAccessKeyId=alice&Signature=accepted&Expires=2000000000", "AWS2", false),
    ] {
        let (envelope, credentials) = accepted_query_caller(query);
        let mut probe = envelope.fork_for_permission_probe(
            Method::GET,
            "/alice-data/object".parse().expect("operation URI"),
            ReqInfo {
                cred: Some(credentials.clone()),
                bucket: Some("alice-data".to_owned()),
                object: Some("object".to_owned()),
                ..ReqInfo::default()
            },
        );
        assert!(probe.uri().query().is_none(), "the query signature must not be copied");
        assert!(probe.verified_sigv4().is_none(), "the new operation is not signature-verified");
        assert!(probe.verified_presigned().is_none(), "the caller's URL capability must not be copied");
        let principal = AuthzPrincipal::new(&credentials, "root");
        for placement in [None, Some(AuthTypeCondition::RestHeader)] {
            if let Some(placement) = placement {
                probe.set_permission_probe_auth_type(placement);
            }
            let facts = request_authz_facts(
                &probe,
                AuthzTarget {
                    action: Action::S3Action(S3Action::GetObjectAction),
                    bucket: "alice-data",
                    object: "object",
                    version_id: None,
                    location_constraint: None,
                },
            );
            let conditions = build_conditions(&principal, &facts);
            let expected_placement = if placement.is_some() {
                "REST-HEADER"
            } else {
                "REST-QUERY-STRING"
            };
            assert_eq!(conditions.get("authType"), Some(&vec![expected_placement.to_owned()]));
            assert_eq!(conditions.get("signatureversion"), Some(&vec![signing_version.to_owned()]));
            assert_eq!(policy.is_allowed(&build_args(&principal, &facts, &conditions)).await, allowed);
        }
    }
}

#[test]
fn permission_probe_does_not_promote_an_unauthenticated_query_to_signing_facts() {
    let envelope = RequestEnvelope::from_parts(
        RequestHead {
            method: Method::POST,
            uri: "/rustfs/admin/v4/account/permissions?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=unverified"
                .parse()
                .expect("unverified request URI"),
            headers: HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        },
        Extensions::new(),
    );
    let probe =
        envelope.fork_for_permission_probe(Method::GET, "/alice-data/object".parse().expect("operation URI"), ReqInfo::default());
    let credentials = Credentials::default();
    let principal = AuthzPrincipal::new(&credentials, "root");
    let facts = request_authz_facts(
        &probe,
        AuthzTarget {
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "alice-data",
            object: "object",
            version_id: None,
            location_constraint: None,
        },
    );
    let conditions = build_conditions(&principal, &facts);
    assert!(probe.permission_probe_caller_authentication().is_none());
    assert!(!conditions.contains_key("signatureversion"));
    assert_eq!(conditions.get("authType"), Some(&vec!["Anonymous".to_owned()]));
}
