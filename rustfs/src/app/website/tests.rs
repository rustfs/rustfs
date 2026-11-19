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

//! The website rules, and the host answering real buckets as both stacks serve them.

use super::*;
use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::app::storage_api::object_usecase::contract::object::ObjectIO as _;
use crate::app::storage_api::object_usecase::{StorageObjectOptions, StoragePutObjReader};
use crate::app::storage_api::test::ECStore;
use http_body_util::BodyExt;
use rustfs_gateway_types::persistence::{
    PersistedRedirect as Redirect, PersistedRoutingRule as RoutingRule, PersistedRoutingRuleCondition as Condition,
};
use serial_test::serial;

fn uri(value: &str) -> Uri {
    value.parse().expect("test URI should parse")
}

fn host(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(HOST, value.parse().expect("valid host"));
    headers
}

#[test]
fn website_host_is_isolated_from_s3_host_and_keeps_directory_suffix() {
    let domains = vec!["example.com".to_owned()];
    let mut headers = host("share.example.com:9000");
    let target = website_target(&uri("/docs/"), &headers, &domains).expect("website host matches");
    assert_eq!(target.bucket, "share");
    assert_eq!(target.key, "docs/");
    assert!(target.trailing_slash);
    headers.insert(HOST, "s3.other.test".parse().expect("valid host"));
    assert!(website_target(&uri("/share/docs/"), &headers, &domains).is_none());
    headers.insert(HOST, "my.bucket.example.com".parse().expect("valid host"));
    assert_eq!(
        website_target(&uri("/"), &headers, &domains)
            .expect("dotted bucket host matches")
            .bucket,
        "my.bucket"
    );
    headers.insert(HOST, "example.com".parse().expect("valid host"));
    assert_eq!(
        website_target(&uri("/share/key"), &headers, &domains)
            .expect("base domain is reserved")
            .bucket,
        ""
    );
    headers.insert(HOST, "share.example.com".parse().expect("valid host"));
    assert!(
        website_target(&uri("/%FF"), &headers, &domains)
            .expect("invalid path stays in website route")
            .invalid_path
    );
}

#[test]
fn website_host_claims_only_its_domains_and_nothing_without_one() {
    let server_ctx = ServerContextSlot::new();
    assert!(WebsiteHost::new(Vec::new(), "http", Arc::clone(&server_ctx)).is_none());
    let website = WebsiteHost::new(vec!["website.test".to_owned()], "http", server_ctx).expect("a configured domain");
    for claimed in [
        "website.test",
        "bucket.website.test",
        "BUCKET.Website.Test.",
        "bucket.website.test:9000",
    ] {
        assert!(website.claims(&uri("/key"), &host(claimed)), "{claimed}");
    }
    for unclaimed in [
        "localhost",
        "website.test.evil",
        "bucketwebsite.test",
        "s3.example.test",
        "a b",
    ] {
        assert!(!website.claims(&uri("/key"), &host(unclaimed)), "{unclaimed}");
    }
    assert!(!website.claims(&uri("/key"), &HeaderMap::new()), "no Host");
}

#[test]
fn website_redirect_encodes_path_and_preserves_query() {
    let target = uri("/old?download=1");
    let headers = host("site.example.com");
    let redirect = website_redirect(
        &target,
        &headers,
        "new.example.com",
        Some("https"),
        "new path/#one",
        StatusCode::MOVED_PERMANENTLY,
        "http",
    )
    .expect("valid redirect");
    assert_eq!(
        redirect.headers()[http::header::LOCATION],
        "https://new.example.com/new%20path/%23one?download=1"
    );
    let directory = website_relative_redirect(&target, "/new path/", StatusCode::FOUND).expect("valid directory redirect");
    assert_eq!(directory.headers()[http::header::LOCATION], "/new%20path/?download=1");
    let secure = website_redirect(&target, &headers, "new.example.com", None, "key", StatusCode::MOVED_PERMANENTLY, "https")
        .expect("TLS listener default scheme");
    assert_eq!(secure.headers()[http::header::LOCATION], "https://new.example.com/key?download=1");
    let same_host = website_redirect(&target, &headers, "", None, "key", StatusCode::MOVED_PERMANENTLY, "http")
        .expect("an empty host name keeps the request's host");
    assert_eq!(same_host.headers()[http::header::LOCATION], "http://site.example.com/key?download=1");
}

#[test]
fn website_rules_choose_first_match_after_object_status_is_known() {
    let error_rule = RoutingRule {
        condition: Some(Condition {
            http_error_code_returned_equals: Some("404".to_owned()),
            key_prefix_equals: None,
        }),
        redirect: Redirect::default(),
    };
    let prefix_rule = RoutingRule {
        condition: Some(Condition {
            http_error_code_returned_equals: None,
            key_prefix_equals: Some("docs/".to_owned()),
        }),
        redirect: Redirect::default(),
    };
    let rules = [error_rule, prefix_rule];
    assert_eq!(
        rules
            .iter()
            .position(|rule| website_rule_matches(rule, "docs/missing", StatusCode::NOT_FOUND)),
        Some(0)
    );
    assert_eq!(
        rules
            .iter()
            .position(|rule| website_rule_matches(rule, "docs/existing", StatusCode::OK)),
        Some(1)
    );
    assert_eq!(
        rules
            .iter()
            .position(|rule| website_rule_matches(rule, "other", StatusCode::FORBIDDEN)),
        None
    );
}

#[tokio::test]
async fn website_304_preserves_cache_headers_without_content() {
    let mut error = S3Error::new(S3ErrorCode::NotModified);
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("etag", "\"abc\""),
        ("cache-control", "max-age=60"),
        ("expires", "Tue, 14 Nov 2023 22:13:20 GMT"),
        ("content-length", "123"),
        ("content-type", "application/xml"),
        ("transfer-encoding", "chunked"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    error.set_headers(headers);
    let response = website_not_modified(&error);
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response.headers()["etag"], "\"abc\"");
    assert_eq!(response.headers()["cache-control"], "max-age=60");
    assert!(response.headers().contains_key("expires"));
    for dropped in ["content-length", "content-type", "transfer-encoding"] {
        assert!(!response.headers().contains_key(dropped), "{dropped}");
    }
    assert!(
        response
            .into_body()
            .collect()
            .await
            .expect("empty website body")
            .to_bytes()
            .is_empty()
    );
}

#[test]
fn website_conditions_are_refused_as_the_s3_decoder_refuses_them() {
    let refused = |name: &'static str, value: &'static str| {
        let mut headers = HeaderMap::new();
        headers.insert(name, HeaderValue::from_static(value));
        validated_conditions(&headers).expect_err(value).code().clone()
    };
    assert_eq!(refused("range", "bytes=5-1"), S3ErrorCode::InvalidRange);
    assert_eq!(refused("range", "bytes=0-1,3-4"), S3ErrorCode::InvalidRange);
    assert_eq!(refused("range", "items=0-1"), S3ErrorCode::InvalidRange);
    assert_eq!(refused("if-modified-since", "yesterday"), S3ErrorCode::InvalidArgument);
    assert_eq!(refused("if-unmodified-since", "2024-01-01"), S3ErrorCode::InvalidArgument);
    let mut headers = HeaderMap::new();
    headers.insert("range", HeaderValue::from_static("bytes=2-"));
    headers.insert("if-none-match", HeaderValue::from_static("*"));
    headers.insert("if-modified-since", HeaderValue::from_static("Tue, 14 Nov 2023 22:13:20 GMT"));
    assert!(matches!(
        validated_conditions(&headers).expect("well-formed conditions"),
        Some(Range::Int { first: 2, last: None })
    ));
    let stripped = without_conditions(&headers);
    assert!(stripped.is_empty(), "{stripped:?}");
}

/// A bucket served as a website, as `website_hosting_test` builds it.
struct Site {
    website: WebsiteHost,
    bucket: String,
}

const DOMAIN: &str = "website.test";
const PUBLIC_READ: &str = "public";

async fn put(store: &ECStore, bucket: &str, key: &str, body: &str, redirect: Option<&str>) {
    let mut options = StorageObjectOptions::default();
    if let Some(location) = redirect {
        options
            .user_defined
            .insert(rustfs_utils::http::AMZ_WEBSITE_REDIRECT_LOCATION.to_owned(), location.to_owned());
    }
    store
        .put_object(bucket, key, &mut StoragePutObjReader::from_vec(body.as_bytes().to_vec()), &options)
        .await
        .expect("seed a website object");
}

/// A fresh bucket holding the website pages, with `website` as its website configuration and,
/// when `policy` is `PUBLIC_READ`, the public-read policy that denies `private.txt`.
async fn site(website: Option<&str>, policy: &str) -> Site {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = ServerContextSlot::new();
    assert!(server_ctx.install(crate::app::gating_test_env::shared_gating_ambient().await));
    let bucket = format!("website-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create the website bucket");
    for (key, body) in [
        ("index.html", "home"),
        ("404.html", "error page"),
        ("docs", "ordinary object"),
        ("section/index.html", "section page"),
        ("private.txt", "private"),
    ] {
        put(store.as_ref(), &bucket, key, body, None).await;
    }
    put(store.as_ref(), &bucket, "move", "ordinary redirect object", Some("/new-page")).await;
    if let Some(website) = website {
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_WEBSITE_CONFIG,
                website.as_bytes().to_vec(),
            )
            .await
            .expect("store the website configuration");
    }
    if policy == PUBLIC_READ {
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")},
                {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/private.txt")}
            ]
        });
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).expect("policy JSON"),
            )
            .await
            .expect("store the public-read policy");
    }
    let website = WebsiteHost::new(vec![DOMAIN.to_owned()], "http", server_ctx).expect("a website domain");
    Site { website, bucket }
}

const INDEX_AND_ERROR: &str = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>404.html</Key></ErrorDocument></WebsiteConfiguration>";

/// Status, `Location`, and body of one website request.
struct Page {
    status: StatusCode,
    location: Option<String>,
    body: String,
}

impl Site {
    async fn ask(&self, method: Method, path: &str, extra: &[(&'static str, &'static str)]) -> Page {
        self.ask_host(&format!("{}.{DOMAIN}", self.bucket), method, path, extra).await
    }

    async fn ask_host(&self, host_name: &str, method: Method, path: &str, extra: &[(&'static str, &'static str)]) -> Page {
        let mut headers = host(host_name);
        for (name, value) in extra {
            headers.insert(*name, HeaderValue::from_static(value));
        }
        let response = self.website.answer(method, uri(path), headers, Extensions::new()).await;
        let status = response.status();
        let location = response
            .headers()
            .get(http::header::LOCATION)
            .map(|value| value.to_str().expect("an ASCII location").to_owned());
        let body = response.into_body().collect().await.expect("an in-memory body").to_bytes();
        Page {
            status,
            location,
            body: String::from_utf8_lossy(&body).into_owned(),
        }
    }
}

#[tokio::test]
#[serial]
async fn website_host_serves_pages_documents_and_redirects_anonymously() {
    let site = site(Some(INDEX_AND_ERROR), PUBLIC_READ).await;

    let home = site.ask(Method::GET, "/", &[]).await;
    assert_eq!((home.status, home.body.as_str()), (StatusCode::OK, "home"));
    let head = site.ask(Method::HEAD, "/", &[]).await;
    assert_eq!((head.status, head.body.as_str()), (StatusCode::OK, ""));
    let object = site.ask(Method::GET, "/docs", &[]).await;
    assert_eq!((object.status, object.body.as_str()), (StatusCode::OK, "ordinary object"));
    let section = site.ask(Method::GET, "/section/", &[]).await;
    assert_eq!((section.status, section.body.as_str()), (StatusCode::OK, "section page"));

    let missing = site.ask(Method::GET, "/missing", &[]).await;
    assert_eq!((missing.status, missing.body.as_str()), (StatusCode::NOT_FOUND, "error page"));
    let denied = site.ask(Method::GET, "/private.txt", &[]).await;
    assert_eq!((denied.status, denied.body.as_str()), (StatusCode::FORBIDDEN, "error page"));
    let head_missing = site.ask(Method::HEAD, "/missing", &[]).await;
    assert_eq!((head_missing.status, head_missing.body.as_str()), (StatusCode::NOT_FOUND, ""));
    let directory = site.ask(Method::GET, "/section?x=1", &[]).await;
    assert_eq!(directory.status, StatusCode::FOUND);
    assert_eq!(directory.location.as_deref(), Some("/section/?x=1"));
    let redirect = site.ask(Method::GET, "/move", &[]).await;
    assert_eq!(redirect.status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(redirect.location.as_deref(), Some("/new-page"));
    for condition in [("if-none-match", "*"), ("range", "bytes=999999-")] {
        let redirected = site.ask(Method::GET, "/move", &[condition]).await;
        assert_eq!(redirected.status, StatusCode::MOVED_PERMANENTLY, "{condition:?}");
        assert_eq!(redirected.location.as_deref(), Some("/new-page"), "{condition:?}");
    }
    let unsatisfiable = site.ask(Method::GET, "/index.html", &[("range", "bytes=999999-")]).await;
    // A client error is answered by the error document, under the error's own status.
    assert_eq!(
        (unsatisfiable.status, unsatisfiable.body.as_str()),
        (StatusCode::RANGE_NOT_SATISFIABLE, "error page")
    );
    let ranged = site.ask(Method::GET, "/index.html", &[("range", "bytes=1-2")]).await;
    assert_eq!((ranged.status, ranged.body.as_str()), (StatusCode::PARTIAL_CONTENT, "om"));
    let malformed = site
        .ask(Method::GET, "/index.html", &[("if-modified-since", "yesterday")])
        .await;
    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    assert!(!malformed.body.contains("<Error>"), "{}", malformed.body);
    let post = site.ask(Method::POST, "/", &[]).await;
    assert_eq!(post.status, StatusCode::METHOD_NOT_ALLOWED);
    let base_domain = site
        .ask_host(DOMAIN, Method::GET, &format!("/{}/index.html", site.bucket), &[])
        .await;
    assert_eq!(base_domain.status, StatusCode::NOT_FOUND);
    assert!(!base_domain.body.contains("<Error>"), "{}", base_domain.body);
    let no_bucket = site
        .ask_host(&format!("absent-{}.{DOMAIN}", site.bucket), Method::GET, "/", &[])
        .await;
    assert_eq!(no_bucket.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn website_host_refuses_what_the_bucket_does_not_serve() {
    // No website configuration: the bucket is not a website, whatever its policy.
    let unconfigured = site(None, PUBLIC_READ).await;
    let page = unconfigured.ask(Method::GET, "/", &[]).await;
    assert_eq!(page.status, StatusCode::NOT_FOUND);
    assert_eq!(page.body, "<html><body>Not Found</body></html>");

    // A website without a public-read policy is read as the anonymous caller it is: the index and
    // the error document are both refused, so the page is the plain refusal.
    let private = site(Some(INDEX_AND_ERROR), "none").await;
    for path in ["/", "/index.html", "/docs", "/missing"] {
        let page = private.ask(Method::GET, path, &[]).await;
        assert_eq!(page.status, StatusCode::FORBIDDEN, "{path}");
        assert_eq!(page.body, "<html><body>Forbidden</body></html>", "{path}");
    }
    // Whatever caller and ownership the request arrived with, a website read is anonymous.
    let mut owner = Extensions::new();
    owner.insert(ReqInfo {
        cred: Some(rustfs_credentials::Credentials {
            access_key: "website-owner".to_owned(),
            ..Default::default()
        }),
        is_owner: true,
        ..ReqInfo::default()
    });
    let host_name = format!("{}.{DOMAIN}", private.bucket);
    let response = private.website.answer(Method::GET, uri("/"), host(&host_name), owner).await;
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "an owner's request facts must not reach the read"
    );
    // A redirect location is only reported to a read the bucket allows.
    let hidden = private.ask(Method::GET, "/move", &[]).await;
    assert_eq!((hidden.status, hidden.location), (StatusCode::FORBIDDEN, None));

    // An index document that does not exist is answered by the error document's status.
    let no_index = site(
        Some("<WebsiteConfiguration><IndexDocument><Suffix>absent.html</Suffix></IndexDocument></WebsiteConfiguration>"),
        PUBLIC_READ,
    )
    .await;
    let page = no_index.ask(Method::GET, "/", &[]).await;
    assert_eq!(page.status, StatusCode::NOT_FOUND);
    assert_eq!(page.body, "<html><body>Not Found</body></html>");
}

#[tokio::test]
#[serial]
async fn website_host_applies_routing_rules_and_redirect_all() {
    let rules = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><RoutingRules>\
        <RoutingRule><Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition><Redirect><ReplaceKeyWith>missing-page</ReplaceKeyWith></Redirect></RoutingRule>\
        <RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><ReplaceKeyWith>docs-page</ReplaceKeyWith></Redirect></RoutingRule>\
        <RoutingRule><Condition><HttpErrorCodeReturnedEquals>416</HttpErrorCodeReturnedEquals></Condition><Redirect><ReplaceKeyWith>range-error</ReplaceKeyWith></Redirect></RoutingRule>\
        </RoutingRules></WebsiteConfiguration>";
    let site = site(Some(rules), PUBLIC_READ).await;
    let host_name = format!("{}.{DOMAIN}", site.bucket);
    let rule = site.ask(Method::GET, "/docs/missing", &[]).await;
    assert_eq!(rule.status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(rule.location, Some(format!("http://{host_name}/missing-page")));
    let ranged = site.ask(Method::GET, "/index.html", &[("range", "bytes=999999-")]).await;
    assert_eq!(ranged.status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(ranged.location, Some(format!("http://{host_name}/range-error")));
    let served = site.ask(Method::GET, "/index.html", &[]).await;
    assert_eq!((served.status, served.body.as_str()), (StatusCode::OK, "home"));

    let all = site_redirecting_all().await;
    let page = all.ask(Method::GET, "/a%20b?x=1", &[]).await;
    assert_eq!(page.status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(page.location.as_deref(), Some("http://destination.test/a%20b?x=1"));
}

async fn site_redirecting_all() -> Site {
    site(
        Some("<WebsiteConfiguration><RedirectAllRequestsTo><HostName>destination.test</HostName></RedirectAllRequestsTo></WebsiteConfiguration>"),
        "none",
    )
    .await
}
