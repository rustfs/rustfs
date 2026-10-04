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

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, Condition, ErrorDocument, IndexDocument, Redirect, RedirectAllRequestsTo, RoutingRule,
    VersioningConfiguration, WebsiteConfiguration,
};
use reqwest::header::HOST;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn website_host_serves_pages_without_changing_s3_reads() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_WEBSITE_DOMAINS", "website.test")])
        .await?;
    let s3 = env.create_s3_client();
    let bucket = "website-hosting";
    s3.create_bucket().bucket(bucket).send().await?;
    for (key, body) in [
        ("index.html", "home"),
        ("404.html", "error page"),
        ("docs", "ordinary object"),
        ("section/index.html", "section page"),
        ("private.txt", "private"),
    ] {
        s3.put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.as_bytes().to_vec()))
            .send()
            .await?;
    }
    s3.put_object()
        .bucket(bucket)
        .key("move")
        .website_redirect_location("/new-page")
        .body(ByteStream::from_static(b"ordinary redirect object"))
        .send()
        .await?;
    s3.put_bucket_website()
        .bucket(bucket)
        .website_configuration(
            WebsiteConfiguration::builder()
                .index_document(IndexDocument::builder().suffix("index.html").build()?)
                .error_document(ErrorDocument::builder().key("404.html").build()?)
                .build(),
        )
        .send()
        .await?;
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/private.txt")}
        ]
    })
    .to_string();
    s3.put_bucket_policy().bucket(bucket).policy(policy).send().await?;

    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let website = |path: &str| format!("{}{path}", env.url);
    let host = format!("{bucket}.website.test");
    let base_domain = http
        .get(website("/website-hosting/index.html"))
        .header(HOST, "website.test")
        .send()
        .await?;
    assert_eq!(base_domain.status(), 404);
    assert!(!base_domain.text().await?.contains("<Error>"));
    let home = http.get(website("/")).header(HOST, &host).send().await?;
    assert_eq!(home.status(), 200);
    assert_eq!(home.text().await?, "home");
    let head = http.head(website("/")).header(HOST, &host).send().await?;
    assert_eq!(head.status(), 200);
    assert_eq!(head.text().await?, "");
    let missing = http.get(website("/missing")).header(HOST, &host).send().await?;
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.text().await?, "error page");
    let denied = http.get(website("/private.txt")).header(HOST, &host).send().await?;
    assert_eq!(denied.status(), 403);
    assert_eq!(denied.text().await?, "error page");
    let collision = http.get(website("/docs")).header(HOST, &host).send().await?;
    assert_eq!(collision.status(), 200);
    assert_eq!(collision.text().await?, "ordinary object");
    let directory = http.get(website("/section")).header(HOST, &host).send().await?;
    assert_eq!(directory.status(), 302);
    assert_eq!(directory.headers()[reqwest::header::LOCATION], "/section/");
    let section = http.get(website("/section/")).header(HOST, &host).send().await?;
    assert_eq!(section.status(), 200);
    assert_eq!(section.text().await?, "section page");
    let redirect = http.get(website("/move")).header(HOST, &host).send().await?;
    assert_eq!(redirect.status(), 301);
    assert_eq!(redirect.headers()[reqwest::header::LOCATION], "/new-page");
    let redirect_with_condition = http
        .get(website("/move"))
        .header(HOST, &host)
        .header(reqwest::header::IF_NONE_MATCH, "*")
        .send()
        .await?;
    assert_eq!(redirect_with_condition.status(), 301);
    let redirect_with_range = http
        .get(website("/move"))
        .header(HOST, &host)
        .header(reqwest::header::RANGE, "bytes=999999-")
        .send()
        .await?;
    assert_eq!(redirect_with_range.status(), 301);
    let plain_range_error = http
        .get(website("/index.html"))
        .header(HOST, &host)
        .header(reqwest::header::RANGE, "bytes=999999-")
        .send()
        .await?;
    assert_eq!(plain_range_error.status(), 416);
    assert!(!plain_range_error.text().await?.contains("<Error>"));

    let ordinary = s3.get_object().bucket(bucket).key("move").send().await?;
    assert_eq!(ordinary.body.collect().await?.into_bytes(), "ordinary redirect object");
    let ranged = s3.get_object().bucket(bucket).key("move").range("bytes=0-7").send().await?;
    assert_eq!(ranged.body.collect().await?.into_bytes(), "ordinary");
    let head_object = s3.head_object().bucket(bucket).key("move").send().await?;
    let etag = head_object.e_tag().expect("object ETag");
    let unchanged = s3.get_object().bucket(bucket).key("move").if_none_match(etag).send().await;
    assert!(unchanged.is_err(), "ordinary S3 conditional GET should return NotModified");
    let presigned = s3
        .get_object()
        .bucket(bucket)
        .key("move")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300))?)
        .await?;
    let presigned_read = http.get(presigned.uri().to_string()).send().await?;
    assert_eq!(presigned_read.status(), 200);
    assert_eq!(presigned_read.text().await?, "ordinary redirect object");
    let listed = s3.list_objects_v2().bucket(bucket).send().await?;
    assert!(listed.contents().iter().any(|object| object.key() == Some("index.html")));
    let s3_missing = http.get(format!("{}/{bucket}/missing", env.url)).send().await?;
    assert!(s3_missing.status().is_client_error());
    assert!(s3_missing.text().await?.contains("<Error>"));
    s3.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    let first = s3
        .put_object()
        .bucket(bucket)
        .key("versioned.txt")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await?;
    s3.put_object()
        .bucket(bucket)
        .key("versioned.txt")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await?;
    let versioned = s3
        .get_object()
        .bucket(bucket)
        .key("versioned.txt")
        .version_id(first.version_id().expect("first object version"))
        .send()
        .await?;
    assert_eq!(versioned.body.collect().await?.into_bytes(), "first");

    s3.put_bucket_website()
        .bucket(bucket)
        .website_configuration(
            WebsiteConfiguration::builder()
                .index_document(IndexDocument::builder().suffix("index.html").build()?)
                .routing_rules(
                    RoutingRule::builder()
                        .condition(Condition::builder().http_error_code_returned_equals("404").build())
                        .redirect(Redirect::builder().replace_key_with("missing-page").build())
                        .build(),
                )
                .routing_rules(
                    RoutingRule::builder()
                        .condition(Condition::builder().key_prefix_equals("docs/").build())
                        .redirect(Redirect::builder().replace_key_with("docs-page").build())
                        .build(),
                )
                .routing_rules(
                    RoutingRule::builder()
                        .condition(Condition::builder().http_error_code_returned_equals("500").build())
                        .redirect(Redirect::builder().replace_key_with("wrong-status").build())
                        .build(),
                )
                .routing_rules(
                    RoutingRule::builder()
                        .condition(Condition::builder().http_error_code_returned_equals("416").build())
                        .redirect(Redirect::builder().replace_key_with("range-error").build())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    let rule = http.get(website("/docs/missing")).header(HOST, &host).send().await?;
    assert_eq!(rule.status(), 301);
    assert_eq!(
        rule.headers()[reqwest::header::LOCATION],
        "http://website-hosting.website.test/missing-page"
    );
    let ranged_error = http
        .get(website("/index.html"))
        .header(HOST, &host)
        .header(reqwest::header::RANGE, "bytes=999999-")
        .send()
        .await?;
    assert_eq!(ranged_error.status(), 301);
    assert_eq!(
        ranged_error.headers()[reqwest::header::LOCATION],
        "http://website-hosting.website.test/range-error"
    );

    s3.put_bucket_website()
        .bucket(bucket)
        .website_configuration(
            WebsiteConfiguration::builder()
                .redirect_all_requests_to(RedirectAllRequestsTo::builder().host_name("destination.test").build()?)
                .build(),
        )
        .send()
        .await?;
    let all = http.get(website("/a%20b?x=1")).header(HOST, &host).send().await?;
    assert_eq!(all.status(), 301);
    assert_eq!(all.headers()[reqwest::header::LOCATION], "http://destination.test/a%20b?x=1");

    let dotted = "my.website.bucket";
    s3.create_bucket().bucket(dotted).send().await?;
    s3.put_bucket_website()
        .bucket(dotted)
        .website_configuration(
            WebsiteConfiguration::builder()
                .redirect_all_requests_to(RedirectAllRequestsTo::builder().host_name("destination.test").build()?)
                .build(),
        )
        .send()
        .await?;
    let dotted_host = format!("{dotted}.website.test");
    let dotted_response = http.get(website("/key")).header(HOST, dotted_host).send().await?;
    assert_eq!(dotted_response.status(), 301);
    assert_eq!(dotted_response.headers()[reqwest::header::LOCATION], "http://destination.test/key");
    env.stop_server();
    Ok(())
}
