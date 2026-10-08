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

//! `auth::facts` against the legacy condition values.
//!
//! Every request in [`cases`] runs through the legacy derivation kept in
//! `auth::legacy_condition_oracle` and through [`build_conditions`]. The two
//! must agree key for key, value for value, and both must reproduce
//! `legacy_conditions.tsv`, which the legacy derivation wrote before
//! `auth::facts` existed. Run with `RUSTFS_AUTHZ_FACTS_BLESS=1` to rewrite it
//! from the legacy derivation.

use super::{
    AuthzFacts, AuthzPrincipal, AuthzTarget, SECRET_HEADERS, base_conditions, build_args, build_conditions,
    forward_secret_headers, list_query_pairs, merge_list_query_conditions,
};
use crate::auth::legacy_condition_oracle as oracle;
use crate::runtime_sources::current_action_credentials;
use http::{HeaderMap, HeaderName, HeaderValue};
use rustfs_credentials::{Credentials, IAM_POLICY_CLAIM_NAME_SA};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_trusted_proxies::{ClientInfo, ValidationMode};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

type Conditions = HashMap<String, Vec<String>>;

const GOLDEN: &str = include_str!("legacy_conditions.tsv");
const GOLDEN_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/auth/facts/legacy_conditions.tsv");
const BLESS_ENV: &str = "RUSTFS_AUTHZ_FACTS_BLESS";

const SIGV4_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=facts-user/20261009/us-east-1/s3/aws4_request, \
     SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
     Signature=f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff00010203040506070809fafbfcfdfeff";
const SESSION_TOKEN: &str = "facts-header-session-token-value";
const CUSTOMER_KEY: &str = "ZmFjdHMtY3VzdG9tZXIta2V5LXZhbHVlLTMyYnl0ZXM=";

#[derive(Clone, Copy, Debug)]
enum Who {
    Anonymous,
    User,
    Sts,
    ExpiredSts,
    ServiceAccount,
    Federated,
    GroupMember,
    ClaimShadowsHeaders,
}

fn claims(pairs: &[(&str, Value)]) -> Option<HashMap<String, Value>> {
    Some(
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect(),
    )
}

fn credentials(who: Who) -> Credentials {
    let now = OffsetDateTime::now_utc();
    let session = |access_key: &str, expiration: OffsetDateTime| Credentials {
        access_key: access_key.to_owned(),
        secret_key: format!("{access_key}-secret"),
        session_token: format!("{access_key}-session-token"),
        expiration: Some(expiration),
        status: "on".to_owned(),
        parent_user: "facts-sts-parent".to_owned(),
        claims: claims(&[
            ("parent", json!("facts-sts-parent")),
            ("sub", json!("facts-sts-subject")),
            ("exp", json!(4_102_444_800_u64)),
        ]),
        ..Credentials::default()
    };
    let user = |access_key: &str| Credentials {
        access_key: access_key.to_owned(),
        secret_key: format!("{access_key}-secret"),
        status: "on".to_owned(),
        ..Credentials::default()
    };
    match who {
        Who::Anonymous => Credentials::default(),
        Who::User => user("facts-user"),
        Who::Sts => session("facts-sts", now + Duration::hours(1)),
        Who::ExpiredSts => session("facts-expired-sts", now - Duration::hours(1)),
        Who::ServiceAccount => Credentials {
            parent_user: "facts-svc-parent".to_owned(),
            session_token: "facts-svc-session-token".to_owned(),
            claims: claims(&[
                (IAM_POLICY_CLAIM_NAME_SA, json!("inherited-policy")),
                ("parent", json!("facts-svc-parent")),
            ]),
            ..user("facts-svc")
        },
        Who::Federated => Credentials {
            parent_user: "facts-fed-parent".to_owned(),
            session_token: "facts-fed-session-token".to_owned(),
            expiration: Some(now + Duration::hours(1)),
            groups: Some(vec!["fed-credential-group".to_owned()]),
            claims: claims(&[
                ("ldapUsername", json!("Facts-Ldap-User")),
                ("Email", json!("dev@example.test")),
                ("groups", json!(["fed-g1", "fed-g2"])),
                ("Roles", json!("fed-r1, fed-r2")),
                ("aud", json!(7)),
            ]),
            ..user("facts-fed")
        },
        Who::GroupMember => Credentials {
            groups: Some(vec!["cred-g1".to_owned(), "cred-g2".to_owned()]),
            ..user("facts-member")
        },
        Who::ClaimShadowsHeaders => Credentials {
            claims: claims(&[
                ("authorization", json!("claim-authorization")),
                ("x-amz-grant-read", json!("claim-grant-read")),
            ]),
            ..user("facts-claims")
        },
    }
}

/// One request: what the legacy derivation and `AuthzFacts` are both given.
#[derive(Clone)]
struct Case {
    name: &'static str,
    who: Who,
    headers: Vec<(&'static str, Vec<u8>)>,
    query: Option<&'static str>,
    peer: Option<&'static str>,
    trusted: Option<(&'static str, Option<&'static str>)>,
    version_id: Option<&'static str>,
    region: Option<&'static str>,
    action: Action,
}

impl Case {
    fn new(name: &'static str, who: Who) -> Self {
        Self {
            name,
            who,
            headers: Vec::new(),
            query: None,
            peer: None,
            trusted: None,
            version_id: None,
            region: None,
            action: Action::S3Action(S3Action::GetObjectAction),
        }
    }

    fn header(self, name: &'static str, value: &str) -> Self {
        self.raw_header(name, value.as_bytes())
    }

    fn raw_header(mut self, name: &'static str, value: &[u8]) -> Self {
        self.headers.push((name, value.to_vec()));
        self
    }

    fn sigv4(self) -> Self {
        self.header("Authorization", SIGV4_AUTHORIZATION)
            .header("X-Amz-Date", "20261009T101500Z")
            .header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
    }

    fn query(mut self, query: &'static str) -> Self {
        self.query = Some(query);
        self
    }

    fn peer(mut self, peer: &'static str) -> Self {
        self.peer = Some(peer);
        self
    }

    fn trusted(mut self, client_ip: &'static str, forwarded_proto: Option<&'static str>) -> Self {
        self.trusted = Some((client_ip, forwarded_proto));
        self
    }

    fn version(mut self, version_id: &'static str) -> Self {
        self.version_id = Some(version_id);
        self
    }

    fn region(mut self, region: &'static str) -> Self {
        self.region = Some(region);
        self
    }

    fn action(mut self, action: S3Action) -> Self {
        self.action = Action::S3Action(action);
        self
    }

    fn header_map(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in &self.headers {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).expect("test header name"),
                HeaderValue::from_bytes(value).expect("test header value"),
            );
        }
        headers
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        self.peer.map(|peer| peer.parse().expect("test peer address"))
    }

    fn client_info(&self) -> Option<ClientInfo> {
        self.trusted.map(|(client_ip, forwarded_proto)| {
            ClientInfo::from_trusted_proxy(
                client_ip.parse().expect("test client address"),
                None,
                forwarded_proto.map(str::to_owned),
                "10.9.9.9".parse().expect("test proxy address"),
                1,
                ValidationMode::Lenient,
                Vec::new(),
            )
        })
    }

    fn target(&self) -> AuthzTarget<'_> {
        AuthzTarget {
            action: self.action,
            bucket: "facts-bucket",
            object: "facts/object.txt",
            version_id: self.version_id,
            location_constraint: self.region,
        }
    }

    fn facts(&self, now: OffsetDateTime) -> AuthzFacts {
        AuthzFacts::from_request_parts(
            &self.header_map(),
            self.query,
            self.remote_addr(),
            self.client_info().as_ref(),
            self.target(),
            now,
        )
    }

    /// The legacy derivation, including the listing merge `storage/access.rs` applied after it.
    fn legacy(&self, credentials: &Credentials) -> Conditions {
        let mut conditions = oracle::get_condition_values_with_query_and_client_info(
            &self.header_map(),
            credentials,
            self.version_id,
            self.region.map(|region| region.parse().expect("test region")),
            self.remote_addr(),
            self.query,
            self.client_info().as_ref(),
        );
        oracle::merge_list_bucket_query_conditions(self.action, self.query, &mut conditions);
        conditions
    }

    /// What the legacy edge now derives: the facts, plus the secret headers it still forwards.
    fn current(&self, credentials: &Credentials, now: OffsetDateTime) -> Conditions {
        let root = root_access_key();
        let principal = AuthzPrincipal::new(credentials, &root);
        let mut conditions = build_conditions(&principal, &self.facts(now));
        forward_secret_headers(&self.header_map(), &mut conditions);
        conditions
    }
}

/// The root access key the legacy derivation compared against.
fn root_access_key() -> String {
    current_action_credentials().map(|root| root.access_key).unwrap_or_default()
}

/// The time the legacy derivation read, recovered from its `CurrentTime`, after
/// checking that it was read during the call and agrees with `EpochTime`.
fn legacy_now(case: &str, legacy: &Conditions, before: OffsetDateTime, after: OffsetDateTime) -> OffsetDateTime {
    let current_time = &legacy["CurrentTime"];
    assert_eq!(current_time.len(), 1, "{case}: CurrentTime");
    let now = OffsetDateTime::parse(&current_time[0], &Rfc3339).expect("legacy CurrentTime is RFC 3339");
    assert!(before <= now && now <= after, "{case}: CurrentTime {now} outside [{before}, {after}]");
    assert_eq!(legacy["EpochTime"], vec![now.unix_timestamp().to_string()], "{case}: EpochTime");
    now
}

/// Every case's legacy and current condition values, the current ones derived
/// at the instant the legacy derivation read.
fn run_cases() -> Vec<(Case, Conditions, Conditions)> {
    cases()
        .into_iter()
        .map(|case| {
            let credentials = credentials(case.who);
            let before = OffsetDateTime::now_utc();
            let legacy = case.legacy(&credentials);
            let after = OffsetDateTime::now_utc();
            let now = legacy_now(case.name, &legacy, before, after);
            let current = case.current(&credentials, now);
            (case, legacy, current)
        })
        .collect()
}

fn cases() -> Vec<Case> {
    use S3Action::{ListBucketAction, ListBucketMultipartUploadsAction, ListBucketVersionsAction};
    use Who::{Anonymous, ClaimShadowsHeaders, ExpiredSts, Federated, GroupMember, ServiceAccount, Sts, User};
    vec![
        // How the request authenticated: `s3:authType` and `s3:signatureversion`.
        Case::new("anonymous_bare", Anonymous),
        Case::new("anonymous_with_peer_agent_referer", Anonymous)
            .peer("192.0.2.10:50000")
            .header("User-Agent", "aws-cli/2.17 md/facts")
            .header("Referer", "https://example.test/page"),
        Case::new("sigv4_header", User).sigv4().peer("192.0.2.11:50001"),
        Case::new("sigv2_header", User).header("Authorization", "AWS facts-user:ZmFjdHMtdjItc2lnbmF0dXJl"),
        Case::new("sigv4_presigned_query", User).query(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=facts-user%2F20261009%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20261009T101500Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=a0a1a2a3a4a5a6a7a8a9aaabacadaeaf",
        ),
        Case::new("sigv4_presigned_query_with_session_token", Sts).query(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=facts-sts%2F20261009%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20261009T101500Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host\
             &X-Amz-Security-Token=facts-presigned-session-token&X-Amz-Signature=b0b1b2b3b4b5b6b7b8b9babbbcbdbebf",
        ),
        Case::new("sigv2_presigned_query", User)
            .query("AWSAccessKeyId=facts-user&Expires=1791504000&Signature=ZmFjdHMtdjItcXVlcnk%3D"),
        Case::new("sigv4_credential_in_header", User).header("X-Amz-Credential", "facts-user/20261009/us-east-1/s3/aws4_request"),
        Case::new("empty_credential_query_is_anonymous", Anonymous).query("X-Amz-Credential=&prefix=ignored"),
        Case::new("streaming_signed", User)
            .header("Authorization", SIGV4_AUTHORIZATION)
            .header("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        Case::new("streaming_signed_trailer", User)
            .header("Authorization", SIGV4_AUTHORIZATION)
            .header("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
        Case::new("streaming_unsigned_trailer_without_signature", Anonymous)
            .header("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        Case::new("jwt_bearer", User).header("Authorization", "Bearer facts.jwt.bearer-token"),
        Case::new("post_policy_form", Anonymous).header("Content-Type", "multipart/form-data; boundary=facts"),
        Case::new("sigv4_with_form_content_type", User)
            .sigv4()
            .header("Content-Type", "multipart/form-data; boundary=facts"),
        Case::new("sts_action_header", Anonymous).header("Action", "AssumeRoleWithWebIdentity"),
        Case::new("unknown_authorization_scheme", User)
            .header("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=facts-user/20261009/s3/aws4_request"),
        // Who is asking: `aws:userid`, `aws:username`, `aws:principaltype` and claims.
        Case::new("sts_principal_with_token_header", Sts)
            .sigv4()
            .header("X-Amz-Security-Token", SESSION_TOKEN),
        Case::new("expired_sts_principal", ExpiredSts).sigv4(),
        Case::new("service_account_principal", ServiceAccount).sigv4(),
        Case::new("federated_claims", Federated).sigv4(),
        Case::new("credential_groups", GroupMember).sigv4(),
        Case::new("claims_shadow_secret_and_grant_headers", ClaimShadowsHeaders)
            .sigv4()
            .header("X-Amz-Grant-Read", "id=header-grantee"),
        Case::new("anonymous_with_session_token_header", Anonymous).header("X-Amz-Security-Token", SESSION_TOKEN),
        // Where the request came from: `aws:SourceIp` and `aws:SecureTransport`.
        Case::new("trusted_proxy_https", User)
            .sigv4()
            .peer("10.0.0.2:443")
            .trusted("203.0.113.9", Some("https")),
        Case::new("trusted_proxy_http", User)
            .sigv4()
            .peer("10.0.0.2:80")
            .trusted("203.0.113.10", Some("http")),
        Case::new("trusted_proxy_upper_case_https", User)
            .peer("10.0.0.2:443")
            .trusted("203.0.113.11", Some("HTTPS")),
        Case::new("trusted_proxy_without_scheme", User).trusted("203.0.113.12", None),
        Case::new("spoofed_forwarding_headers", User)
            .sigv4()
            .peer("192.0.2.20:51000")
            .header("X-Forwarded-For", "198.51.100.1, 198.51.100.2")
            .header("X-Real-IP", "198.51.100.3")
            .header("Forwarded", "for=198.51.100.4;proto=https")
            .header("X-Forwarded-Proto", "https"),
        Case::new("ipv6_peer", Anonymous).peer("[2001:db8::7]:8443"),
        Case::new("trusted_proxy_ipv6_client", User)
            .peer("10.0.0.3:443")
            .trusted("2001:db8::9", Some("https")),
        // What is asked about: `s3:versionid` and `s3:LocationConstraint`.
        Case::new("version_and_region", User)
            .sigv4()
            .version("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY")
            .region("eu-west-3"),
        Case::new("empty_version_id", Anonymous).version(""),
        Case::new("region_only", Anonymous).region("us-east-1"),
        // Request headers: reserved names, `s3:x-amz-*` keys and multi-value headers.
        Case::new("spoofed_identity_headers", User)
            .sigv4()
            .header("userid", "admin")
            .header("username", "admin")
            .header("principaltype", "Account")
            .header("signatureversion", "AWS2")
            .header("authtype", "REST-HEADER")
            .header("versionid", "spoofed-version")
            .header("groups", "admins")
            .header("roles", "consoleAdmin")
            .header("sub", "someone-else")
            .header("sourceip", "198.51.100.9")
            .header("securetransport", "true")
            .header("currenttime", "2000-01-01T00:00:00Z")
            .header("locationconstraint", "spoofed-region"),
        Case::new("object_lock_headers_and_aliases", User)
            .sigv4()
            .header("X-Amz-Object-Lock-Mode", "GOVERNANCE")
            .header("X-Amz-Object-Lock-Legal-Hold", "ON")
            .header("X-Amz-Object-Lock-Retain-Until-Date", "2030-01-01T00:00:00Z")
            .header("object-lock-mode", "COMPLIANCE")
            .header("object-lock-legal-hold", "OFF"),
        Case::new("duplicate_object_lock_and_grant_headers", User)
            .sigv4()
            .header("X-Amz-Object-Lock-Mode", "GOVERNANCE")
            .header("x-amz-object-lock-mode", "COMPLIANCE")
            .header("X-Amz-Grant-Read", "id=reader-one")
            .header("x-amz-grant-read", "id=reader-two")
            .header("X-Amz-Grant-Full-Control", "id=owner")
            .header("X-Amz-Grant-Write", "id=writer")
            .header("X-Amz-Grant-Read-Acp", "id=acl-reader")
            .header("X-Amz-Grant-Write-Acp", "id=acl-writer"),
        Case::new("signature_age_header", User)
            .sigv4()
            .header("X-Amz-Signature-Age", "300")
            .header("x-amz-signature-age", "900"),
        Case::new("condition_key_amz_headers", User)
            .sigv4()
            .header("X-Amz-Copy-Source", "/source-bucket/source%20key?versionId=v1")
            .header("X-Amz-Server-Side-Encryption", "aws:kms")
            .header("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
            .header("X-Amz-Acl", "bucket-owner-full-control")
            .header("X-Amz-Metadata-Directive", "REPLACE")
            .header("X-Amz-Storage-Class", "STANDARD_IA"),
        Case::new("duplicate_meta_headers_fold_case", User)
            .sigv4()
            .header("X-Amz-Meta-Color", "red")
            .header("x-amz-meta-color", "blue")
            .header("X-AMZ-META-SIZE", "3"),
        Case::new("tagging_header_is_not_a_key", User)
            .sigv4()
            .header("X-Amz-Tagging", "team=storage&tier=hot"),
        Case::new("customer_key_headers", User)
            .sigv4()
            .header("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
            .header("X-Amz-Server-Side-Encryption-Customer-Key", CUSTOMER_KEY)
            .header("X-Amz-Server-Side-Encryption-Customer-Key-Md5", "ZmFjdHMtbWQ1LXZhbHVlLQ==")
            .header("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key", CUSTOMER_KEY),
        Case::new("empty_and_non_utf8_header_values", Anonymous)
            .header("User-Agent", "")
            .raw_header("Referer", b"https://example.test/\xff")
            .raw_header("X-Amz-Meta-Binary", b"\xfe\xff"),
        Case::new("duplicate_user_agent", Anonymous)
            .header("User-Agent", "first-agent")
            .header("user-agent", "second-agent"),
        // Listing queries: `s3:prefix`, `s3:delimiter` and `s3:max-keys`.
        Case::new("list_bucket_query", User)
            .sigv4()
            .action(ListBucketAction)
            .query("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10&encoding-type=url"),
        Case::new("list_versions_repeated_and_case_variant_keys", User)
            .sigv4()
            .action(ListBucketVersionsAction)
            .query("prefix=a&prefix=b+c&Prefix=ignored&delimiter=&MAX-KEYS=5"),
        Case::new("list_uploads_presigned_with_session_token", Sts)
            .action(ListBucketMultipartUploadsAction)
            .query(
                "uploads=&prefix=incoming%2F&X-Amz-Algorithm=AWS4-HMAC-SHA256\
                 &X-Amz-Credential=facts-sts%2F20261009%2Fus-east-1%2Fs3%2Faws4_request\
                 &X-Amz-Security-Token=facts-presigned-session-token&X-Amz-Signature=c0c1c2c3c4c5c6c7c8c9cacbcccdcecf",
            ),
        Case::new("non_list_action_ignores_list_query", User)
            .sigv4()
            .query("prefix=secret%2F&delimiter=%2F&max-keys=1"),
        Case::new("list_bucket_without_query", Anonymous).action(ListBucketAction),
    ]
}

/// One line per key, keys sorted, values as JSON; the clock keys are replaced by
/// placeholders after [`legacy_now`] checked them.
fn render(case: &str, conditions: &Conditions) -> String {
    let sorted: BTreeMap<&String, &Vec<String>> = conditions.iter().collect();
    let mut out = format!("== {case}\n");
    for (key, values) in sorted {
        let rendered = match key.as_str() {
            "CurrentTime" => "[\"<current-time>\"]".to_owned(),
            "EpochTime" => "[\"<epoch-time>\"]".to_owned(),
            _ => serde_json::to_string(values).expect("condition values serialize"),
        };
        out.push_str(&format!("{key}\t{rendered}\n"));
    }
    out
}

fn render_all(runs: &[(Case, Conditions, Conditions)], pick: impl Fn(&(Case, Conditions, Conditions)) -> &Conditions) -> String {
    runs.iter().map(|run| render(run.0.name, pick(run))).collect()
}

/// Asserts that `key` agrees with the legacy derivation for every case, and that
/// the case table has it present in `min_present` cases and absent in `min_absent`.
fn assert_key_matches_legacy(key: &str, min_present: usize, min_absent: usize) {
    let (mut present, mut absent) = (0, 0);
    for (case, legacy, current) in run_cases() {
        assert_eq!(current.get(key), legacy.get(key), "{}: condition key {key}", case.name);
        if legacy.contains_key(key) {
            present += 1;
        } else {
            absent += 1;
        }
    }
    assert!(present >= min_present, "{key}: present in {present} cases, need {min_present}");
    assert!(absent >= min_absent, "{key}: absent in {absent} cases, need {min_absent}");
}

/// Asserts that every legacy value `key` takes in the case table is in `expected`
/// and every value in `expected` is taken, so the comparison above saw them all.
fn assert_legacy_values_seen(key: &str, expected: &[&str]) {
    let mut seen: Vec<String> = run_cases()
        .into_iter()
        .filter_map(|(_, legacy, _)| legacy.get(key).cloned())
        .flatten()
        .collect();
    seen.sort();
    seen.dedup();
    let mut expected: Vec<String> = expected.iter().map(|value| (*value).to_owned()).collect();
    expected.sort();
    assert_eq!(seen, expected, "{key}: values the case table exercises");
}

#[test]
fn case_table_covers_forty_requests_with_unique_names() {
    let cases = cases();
    assert!(cases.len() >= 40, "{} cases", cases.len());
    let mut names: Vec<&str> = cases.iter().map(|case| case.name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), cases.len(), "case names must be unique");
}

#[test]
fn legacy_oracle_reproduces_the_committed_golden() {
    let rendered = render_all(&run_cases(), |run| &run.1);
    if std::env::var_os(BLESS_ENV).is_some() {
        std::fs::write(GOLDEN_PATH, &rendered).expect("write the golden file");
    }
    assert_eq!(rendered, GOLDEN, "the legacy oracle no longer produces {GOLDEN_PATH}");
}

#[test]
fn facts_reproduce_the_committed_golden() {
    assert_eq!(render_all(&run_cases(), |run| &run.2), GOLDEN);
}

#[test]
fn every_condition_value_matches_legacy() {
    for (case, legacy, current) in run_cases() {
        assert_eq!(current, legacy, "{}", case.name);
    }
}

#[test]
fn facts_alone_leave_out_exactly_the_secret_headers() {
    let mut left_out = 0;
    for (case, legacy, _) in run_cases() {
        let credentials = credentials(case.who);
        let now = OffsetDateTime::parse(&legacy["CurrentTime"][0], &Rfc3339).expect("legacy CurrentTime");
        let root = root_access_key();
        let facts_only = build_conditions(&AuthzPrincipal::new(&credentials, &root), &case.facts(now));
        // The legacy value of a secret header's key came from the header unless a
        // claim of the same name took the key first; only the former is left out.
        let headers = case.header_map();
        let mut expected = legacy.clone();
        for name in SECRET_HEADERS {
            let sent: Vec<String> = headers
                .get_all(name)
                .iter()
                .map(|value| value.to_str().unwrap_or("").to_owned())
                .collect();
            if !sent.is_empty() && legacy.get(name) == Some(&sent) {
                expected.remove(name);
                left_out += 1;
            }
        }
        assert_eq!(facts_only, expected, "{}", case.name);
    }
    assert!(left_out >= SECRET_HEADERS.len(), "only {left_out} secret headers were left out");
}

#[test]
fn current_time_matches_legacy() {
    assert_key_matches_legacy("CurrentTime", cases().len(), 0);
}

#[test]
fn epoch_time_matches_legacy() {
    assert_key_matches_legacy("EpochTime", cases().len(), 0);
}

#[test]
fn secure_transport_matches_legacy() {
    assert_key_matches_legacy("SecureTransport", cases().len(), 0);
    assert_legacy_values_seen("SecureTransport", &["false", "true"]);
}

#[test]
fn source_ip_matches_legacy() {
    assert_key_matches_legacy("SourceIp", cases().len(), 0);
    assert_legacy_values_seen(
        "SourceIp",
        &[
            "",
            "192.0.2.10",
            "192.0.2.11",
            "192.0.2.20",
            "2001:db8::7",
            "2001:db8::9",
            "203.0.113.9",
            "203.0.113.10",
            "203.0.113.11",
            "203.0.113.12",
        ],
    );
}

#[test]
fn user_agent_matches_legacy() {
    assert_key_matches_legacy("UserAgent", 3, 1);
}

#[test]
fn referer_matches_legacy() {
    assert_key_matches_legacy("Referer", 2, 1);
}

#[test]
fn userid_matches_legacy() {
    assert_key_matches_legacy("userid", cases().len(), 0);
}

#[test]
fn username_matches_legacy() {
    assert_key_matches_legacy("username", cases().len(), 0);
}

#[test]
fn principal_type_matches_legacy() {
    assert_key_matches_legacy("principaltype", cases().len(), 0);
    assert_legacy_values_seen("principaltype", &["Anonymous", "AssumedRole", "User"]);
}

#[test]
fn version_id_matches_legacy() {
    assert_key_matches_legacy("versionid", 1, 1);
}

#[test]
fn signature_version_matches_legacy() {
    assert_key_matches_legacy("signatureversion", 2, 1);
    assert_legacy_values_seen("signatureversion", &["AWS2", "AWS4-HMAC-SHA256"]);
}

#[test]
fn auth_type_matches_legacy() {
    assert_key_matches_legacy("authType", 6, 1);
    assert_legacy_values_seen("authType", &["Anonymous", "JWT", "POST", "REST-HEADER", "REST-QUERY-STRING", "STS"]);
}

#[test]
fn location_constraint_matches_legacy() {
    assert_key_matches_legacy("LocationConstraint", 2, 1);
}

#[test]
fn signature_age_matches_legacy() {
    assert_key_matches_legacy("signatureAge", 1, 1);
}

#[test]
fn object_lock_keys_match_legacy() {
    for key in ["object-lock-mode", "object-lock-legal-hold", "object-lock-retain-until-date"] {
        assert_key_matches_legacy(key, 1, 1);
    }
}

#[test]
fn grant_keys_match_legacy() {
    for key in [
        "x-amz-grant-full-control",
        "x-amz-grant-read",
        "x-amz-grant-write",
        "x-amz-grant-read-acp",
        "x-amz-grant-write-acp",
    ] {
        assert_key_matches_legacy(key, 1, 1);
    }
}

#[test]
fn list_keys_match_legacy() {
    for key in ["prefix", "delimiter", "max-keys"] {
        assert_key_matches_legacy(key, 1, 1);
    }
}

#[test]
fn amz_header_keys_match_legacy() {
    for key in [
        "x-amz-copy-source",
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-content-sha256",
        "x-amz-acl",
        "x-amz-metadata-directive",
        "x-amz-storage-class",
        "x-amz-meta-color",
        "x-amz-date",
    ] {
        assert_key_matches_legacy(key, 1, 1);
    }
}

#[test]
fn claim_keys_match_legacy() {
    for key in ["groups", "roles", "email", "sub", "parent", "sa-policy"] {
        assert_key_matches_legacy(key, 1, 1);
    }
}

#[test]
fn secret_header_keys_match_legacy_through_forwarding() {
    for key in SECRET_HEADERS {
        assert_key_matches_legacy(key, 1, 1);
    }
}

// Negative cases: what must never happen.

fn request_with_every_secret() -> Case {
    Case::new("every_secret", Who::Sts)
        .header("Authorization", SIGV4_AUTHORIZATION)
        .header("X-Amz-Security-Token", SESSION_TOKEN)
        .header("X-Amz-Server-Side-Encryption-Customer-Key", CUSTOMER_KEY)
        .header("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key", CUSTOMER_KEY)
        .header("User-Agent", "facts-agent-value")
        .action(S3Action::ListBucketAction)
        .query(
            "prefix=facts-prefix-value&X-Amz-Security-Token=facts-query-session-token\
             &X-Amz-Signature=d0d1d2d3d4d5d6d7d8d9dadbdcdddedf&X-Amz-Credential=facts-sts%2F20261009",
        )
}

#[test]
fn facts_debug_prints_no_header_or_query_value() {
    let facts = request_with_every_secret().facts(OffsetDateTime::now_utc());
    let debug = format!("{facts:?}");
    for secret in [
        SIGV4_AUTHORIZATION,
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
        SESSION_TOKEN,
        CUSTOMER_KEY,
        "facts-query-session-token",
        "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf",
        "facts-agent-value",
        "facts-prefix-value",
    ] {
        assert!(!debug.contains(secret), "Debug printed {secret}: {debug}");
    }
    assert!(debug.contains("user-agent"), "Debug names the headers it holds: {debug}");
    assert!(debug.contains("\"prefix\""), "Debug names the query keys it holds: {debug}");
}

#[test]
fn facts_never_hold_secret_headers_or_query_credentials() {
    let facts = request_with_every_secret().facts(OffsetDateTime::now_utc());
    for name in SECRET_HEADERS {
        assert!(!facts.headers.contains_key(name), "{name} reached the facts");
    }
    assert_eq!(facts.list_query, vec![("prefix".to_owned(), "facts-prefix-value".to_owned())]);
}

#[test]
fn facts_conditions_carry_no_secret_value() {
    let case = request_with_every_secret();
    let credentials = credentials(case.who);
    let conditions = build_conditions(&AuthzPrincipal::new(&credentials, ""), &case.facts(OffsetDateTime::now_utc()));
    let values: Vec<&String> = conditions.values().flatten().collect();
    for secret in [SIGV4_AUTHORIZATION, SESSION_TOKEN, CUSTOMER_KEY, "facts-query-session-token"] {
        assert!(!values.iter().any(|value| value.contains(secret)), "{secret} reached a condition value");
    }
}

#[test]
fn unknown_client_address_is_the_empty_string_not_an_absent_key() {
    // The legacy derivation always emits `SourceIp`; an IP condition fails to parse
    // the empty string rather than finding no value. Kept as it was.
    let conditions = build_conditions(
        &AuthzPrincipal::new(&Credentials::default(), ""),
        &Case::new("peerless", Who::Anonymous).facts(OffsetDateTime::now_utc()),
    );
    assert_eq!(conditions.get("SourceIp"), Some(&vec![String::new()]));
}

#[test]
fn plaintext_and_unattested_transports_are_not_secure() {
    for case in [
        Case::new("no_client_info", Who::User).peer("192.0.2.30:80"),
        Case::new("spoofed_scheme_header", Who::User)
            .peer("192.0.2.31:80")
            .header("X-Forwarded-Proto", "https")
            .header("Forwarded", "proto=https"),
        Case::new("trusted_http", Who::User).trusted("203.0.113.30", Some("http")),
        Case::new("trusted_unknown_scheme", Who::User).trusted("203.0.113.31", None),
    ] {
        let conditions =
            build_conditions(&AuthzPrincipal::new(&credentials(case.who), ""), &case.facts(OffsetDateTime::now_utc()));
        assert_eq!(conditions["SecureTransport"], vec!["false".to_owned()], "{}", case.name);
    }
}

#[test]
fn forwarding_headers_never_set_the_source_ip() {
    let case = Case::new("spoofed", Who::User)
        .peer("192.0.2.32:443")
        .header("X-Forwarded-For", "198.51.100.40")
        .header("X-Real-IP", "198.51.100.41")
        .header("Forwarded", "for=198.51.100.42");
    let conditions = build_conditions(&AuthzPrincipal::new(&credentials(case.who), ""), &case.facts(OffsetDateTime::now_utc()));
    assert_eq!(conditions["SourceIp"], vec!["192.0.2.32".to_owned()]);
}

#[test]
fn absent_request_facts_leave_their_keys_absent() {
    let conditions = build_conditions(
        &AuthzPrincipal::new(&credentials(Who::User), ""),
        &Case::new("bare", Who::User)
            .header("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=unknown")
            .facts(OffsetDateTime::now_utc()),
    );
    for key in [
        "UserAgent",
        "Referer",
        "versionid",
        "signatureversion",
        "authType",
        "LocationConstraint",
        "signatureAge",
        "groups",
        "roles",
        "prefix",
    ] {
        assert!(!conditions.contains_key(key), "{key} was invented: {:?}", conditions.get(key));
    }
}

#[test]
fn listing_keys_ignore_other_actions_and_other_spellings() {
    let pairs = list_query_pairs(Some("Prefix=a&PREFIX=b&max_keys=1&delimiter%20=x&prefix=kept"));
    assert_eq!(pairs, vec![("prefix".to_owned(), "kept".to_owned())]);
    let mut conditions = HashMap::new();
    merge_list_query_conditions(Action::S3Action(S3Action::GetObjectAction), &pairs, &mut conditions);
    merge_list_query_conditions(Action::None, &pairs, &mut conditions);
    assert!(conditions.is_empty(), "{conditions:?}");
}

#[test]
fn root_account_needs_the_matching_root_key() {
    let root = credentials(Who::User);
    let conditions = |root_access_key: &str| {
        build_conditions(
            &AuthzPrincipal::new(&root, root_access_key),
            &Case::new("root", Who::User).facts(OffsetDateTime::now_utc()),
        )
    };
    assert_eq!(conditions("facts-user")["principaltype"], vec!["Account".to_owned()]);
    for other in ["", "facts-user-other", "facts-use", "FACTS-USER"] {
        assert_eq!(conditions(other)["principaltype"], vec!["User".to_owned()], "root key {other:?}");
    }
}

#[test]
fn conditions_depend_on_the_given_instant_only() {
    let case = Case::new("clock", Who::User).sigv4();
    let credentials = credentials(case.who);
    let principal = AuthzPrincipal::new(&credentials, "");
    let instant = OffsetDateTime::from_unix_timestamp(1_791_504_000).expect("instant");
    let facts = case.facts(instant);
    let first = build_conditions(&principal, &facts);
    assert_eq!(first, build_conditions(&principal, &facts));
    assert_eq!(first["CurrentTime"], vec!["2026-10-09T00:00:00Z".to_owned()]);
    assert_eq!(first["EpochTime"], vec!["1791504000".to_owned()]);
}

#[test]
fn internal_base_conditions_have_no_listing_keys() {
    let case = Case::new("internal_listing", Who::User)
        .action(S3Action::ListBucketAction)
        .query("prefix=a&delimiter=%2F&max-keys=2");
    let credentials = credentials(case.who);
    let principal = AuthzPrincipal::new(&credentials, "");
    let facts = case.facts(OffsetDateTime::now_utc());
    let base = base_conditions(&principal, &facts);
    for key in ["prefix", "delimiter", "max-keys"] {
        assert!(!base.contains_key(key), "{key}");
    }
    assert_eq!(build_conditions(&principal, &facts)["prefix"], vec!["a".to_owned()]);
}

#[test]
fn build_args_asks_about_the_target_as_the_principal() {
    let case = Case::new("args", Who::Federated).sigv4().version("v1");
    let credentials = credentials(case.who);
    let principal = AuthzPrincipal::new(&credentials, "").with_owner(true);
    let facts = case.facts(OffsetDateTime::now_utc());
    let conditions = build_conditions(&principal, &facts);
    let args = build_args(&principal, &facts, &conditions);
    assert_eq!(args.account, "facts-fed");
    assert_eq!(args.groups, &Some(vec!["fed-credential-group".to_owned()]));
    assert_eq!(args.action, Action::S3Action(S3Action::GetObjectAction));
    assert_eq!(args.bucket, "facts-bucket");
    assert_eq!(args.object, "facts/object.txt");
    assert!(args.is_owner);
    assert!(!args.deny_only);
    assert_eq!(args.claims, credentials.claims.as_ref().expect("federated claims"));
    assert!(std::ptr::eq(args.conditions, &conditions));

    let anonymous = Credentials::default();
    let principal = AuthzPrincipal::new(&anonymous, "");
    let args = build_args(&principal, &facts, &conditions);
    assert!(args.claims.is_empty());
    assert!(!args.is_owner, "ownership is never assumed");
    assert_eq!(args.account, "");
}
