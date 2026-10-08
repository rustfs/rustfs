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

//! The legacy condition values, pinned before `auth::facts` replaces them.
//!
//! Every request in [`cases`] runs through the legacy derivation frozen in
//! `auth::legacy_condition_oracle`, which must reproduce
//! `legacy_conditions.tsv` and agree with the production derivation it was
//! copied from. Run with `RUSTFS_AUTHZ_FACTS_BLESS=1` to rewrite the golden file
//! from the oracle.

use crate::auth::legacy_condition_oracle as oracle;
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

/// One request, as the legacy derivation is given it.
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

    /// The production derivation the oracle was copied from.
    fn current(&self, credentials: &Credentials, _now: OffsetDateTime) -> Conditions {
        let mut conditions = crate::auth::get_condition_values_with_query_and_client_info(
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
fn every_condition_value_matches_legacy() {
    for (case, legacy, mut current) in run_cases() {
        // The production derivation reads its own clock.
        let _ = legacy_now(case.name, &current, OffsetDateTime::UNIX_EPOCH, OffsetDateTime::now_utc());
        let mut legacy = legacy;
        for key in ["CurrentTime", "EpochTime"] {
            legacy.remove(key);
            current.remove(key);
        }
        assert_eq!(current, legacy, "{}", case.name);
    }
}
