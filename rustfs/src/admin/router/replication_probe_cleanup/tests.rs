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

use super::{TargetClient, cleanup_replication_probe};
use aws_sdk_s3::config::{Credentials, Region, retry::RetryConfig};
use http::StatusCode;
use reqwest::Url;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const BUCKET: &str = "test-bucket";
const KEY: &str = "probe";
const VERSION: &str = "<Version><Key>probe</Key><VersionId>v1</VersionId></Version>";

#[derive(Debug)]
enum Call {
    Delete(String),
    List(Option<String>),
}

enum Reply {
    Disconnect,
    Http(StatusCode, String),
}

fn page(entries: &str, next_version: Option<&str>) -> Reply {
    let cursor = match next_version {
        Some(version) => format!(
            "<IsTruncated>true</IsTruncated><NextKeyMarker>{KEY}</NextKeyMarker><NextVersionIdMarker>{version}</NextVersionIdMarker>"
        ),
        None => "<IsTruncated>false</IsTruncated>".to_owned(),
    };
    Reply::Http(StatusCode::OK, format!("<ListVersionsResult>{entries}{cursor}</ListVersionsResult>"))
}

fn decode_request(line: &str) -> Call {
    let mut parts = line.split_whitespace();
    let method = parts.next().expect("HTTP method");
    let uri = parts.next().expect("HTTP request target");
    let url = Url::parse(&format!("http://localhost{uri}")).expect("relative request URI");
    let params: HashMap<_, _> = url.query_pairs().collect();
    match method {
        "DELETE" => {
            assert_eq!(url.path(), format!("/{BUCKET}/{KEY}"));
            Call::Delete(params.get("versionId").expect("explicit version").to_string())
        }
        "GET" => {
            assert_eq!(url.path().trim_end_matches('/'), format!("/{BUCKET}"));
            assert!(params.contains_key("versions"));
            assert_eq!(params.get("prefix").map(|value| value.as_ref()), Some(KEY));
            let version_marker = params.get("version-id-marker").map(ToString::to_string);
            assert_eq!(params.get("key-marker").map(|value| value.as_ref()), version_marker.as_ref().map(|_| KEY));
            Call::List(version_marker)
        }
        _ => panic!("unexpected cleanup request: {line}"),
    }
}

async fn run_cleanup(known: &[&str], mut respond: impl FnMut(Call) -> Reply) -> Result<(), String> {
    temp_env::async_with_vars([("NO_PROXY", Some("127.0.0.1")), ("no_proxy", Some("127.0.0.1"))], async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake S3");
        let endpoint = format!("http://{}", listener.local_addr().expect("bound address"));
        let target = TargetClient {
            client: Arc::new(aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(Region::new("us-east-1"))
                    .credentials_provider(Credentials::new("fixture", "fixture-secret", None, None, "cleanup-test"))
                    .endpoint_url(&endpoint)
                    .force_path_style(true)
                    .retry_config(RetryConfig::disabled())
                    .build(),
            )),
            endpoint,
            credentials: None,
            bucket: BUCKET.to_owned(),
            storage_class: String::new(),
            disable_proxy: true,
            arn: String::new(),
            reset_id: String::new(),
            site_peer_deployment_id: String::new(),
            secure: false,
            health_check_duration: Duration::ZERO,
            replicate_sync: false,
            timeouts: Default::default(),
        };
        let serve = async {
            loop {
                let (stream, _) = listener.accept().await.expect("accept SDK connection");
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.expect("request line");
                let call = decode_request(&line);
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.expect("request header"), 0, "incomplete SDK headers");
                    if line == "\r\n" {
                        break;
                    }
                }
                if let Reply::Http(status, body) = respond(call) {
                    let response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n{body}",
                        status,
                        body.len()
                    );
                    reader.get_mut().write_all(response.as_bytes()).await.expect("reply to SDK");
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = cleanup_replication_probe(&target, BUCKET, KEY, known.iter().copied().map(Some)) => result,
                () = serve => unreachable!("fake S3 should serve until cleanup completes"),
            }
        })
        .await
        .expect("cleanup must terminate")
    })
    .await
}

#[tokio::test]
async fn replication_probe_cleanup_lost_delete_response_confirmed_absent() {
    let mut deletes = 0;
    let mut confirmed = false;
    let outcome = run_cleanup(&["v1"], |call| match call {
        Call::Delete(id) => {
            assert_eq!(id, "v1");
            deletes += 1;
            Reply::Disconnect
        }
        Call::List(None) => {
            assert_eq!(deletes, 1);
            confirmed = true;
            page("", None)
        }
        other => panic!("unexpected {other:?}"),
    })
    .await;
    assert_eq!(outcome, Ok(()));
    assert_eq!(deletes, 1);
    assert!(confirmed);
}

#[tokio::test]
async fn replication_probe_cleanup_retries_listed_version_and_confirms_removal() {
    let mut deletes = 0;
    let mut confirmed = false;
    let outcome = run_cleanup(&["v1"], |call| match call {
        Call::Delete(id) => {
            assert_eq!(id, "v1");
            deletes += 1;
            match deletes {
                1 => Reply::Disconnect,
                2 => Reply::Http(StatusCode::NO_CONTENT, String::new()),
                _ => panic!("version already removed"),
            }
        }
        Call::List(None) if deletes == 1 => page(VERSION, None),
        Call::List(None) if deletes == 2 => {
            confirmed = true;
            page("", None)
        }
        other => panic!("unexpected {other:?}"),
    })
    .await;
    assert_eq!(outcome, Ok(()));
    assert!(confirmed);
}

#[tokio::test]
async fn replication_probe_cleanup_keeps_forbidden_delete_with_empty_listing() {
    let denied = "<Error><Code>AccessDenied</Code><Message>private-upstream-detail</Message></Error>";
    for body in [denied, ""] {
        let mut deletes = 0;
        let error = run_cleanup(&["v1"], |call| match call {
            Call::Delete(id) => {
                assert_eq!(id, "v1");
                deletes += 1;
                Reply::Http(StatusCode::FORBIDDEN, body.to_owned())
            }
            Call::List(None) => page("", None),
            other => panic!("unexpected {other:?}"),
        })
        .await
        .expect_err("explicit rejection must survive an empty listing");
        let description = error.to_ascii_lowercase();
        assert!(description.contains("delete"), "{error}");
        assert!(
            ["403", "forbidden", "permission", "accessdenied", "access denied"]
                .iter()
                .any(|word| description.contains(word)),
            "{error}"
        );
        assert!(!error.contains("private-upstream-detail"), "{error}");
        assert_eq!(deletes, 1);
    }
}

#[tokio::test]
async fn replication_probe_cleanup_requires_readable_confirmation() {
    let error = run_cleanup(&["v1"], |call| match call {
        Call::Delete(_) => Reply::Disconnect,
        Call::List(None) => Reply::Http(
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code><Message>private-upstream-detail</Message></Error>".to_owned(),
        ),
        other => panic!("unexpected {other:?}"),
    })
    .await
    .expect_err("cannot certify the target without a complete list");
    assert!(error.to_ascii_lowercase().contains("list"), "{error}");
    assert!(!error.contains("private-upstream-detail"), "{error}");
}

#[tokio::test]
async fn replication_probe_cleanup_still_listed_after_successful_deletes_fails() {
    let mut deletes = 0;
    let mut checked_after_repair = false;
    let error = run_cleanup(&["v1"], |call| match call {
        Call::Delete(id) => {
            assert_eq!(id, "v1");
            deletes += 1;
            Reply::Http(StatusCode::NO_CONTENT, String::new())
        }
        Call::List(None) => {
            checked_after_repair |= deletes == 2;
            page(VERSION, None)
        }
        other => panic!("unexpected {other:?}"),
    })
    .await
    .expect_err("target kept the version despite successful DELETE responses");
    assert!(!error.is_empty());
    assert_eq!(deletes, 2);
    assert!(checked_after_repair);
}

#[tokio::test]
async fn replication_probe_cleanup_pages_same_key_and_preserves_neighboring_versions() {
    const NEIGHBORS: &str = "<Version><Key>probe-neighbor</Key><VersionId>other-data</VersionId></Version><DeleteMarker><Key>probe-neighbor</Key><VersionId>other-marker</VersionId></DeleteMarker>";
    let last_page = format!("<Version><Key>probe</Key><VersionId>v3</VersionId></Version>{NEIGHBORS}");
    let mut deleted = Vec::new();
    let mut confirmed = false;
    let outcome = run_cleanup(&[], |call| match call {
        Call::Delete(id) => {
            assert!(matches!(id.as_str(), "v1" | "v2" | "v3"));
            deleted.push(id);
            Reply::Http(StatusCode::NO_CONTENT, String::new())
        }
        Call::List(None) if deleted.len() == 3 => {
            confirmed = true;
            page(NEIGHBORS, None)
        }
        Call::List(None) => page(VERSION, Some("v1")),
        Call::List(Some(cursor)) if cursor == "v1" => {
            page("<DeleteMarker><Key>probe</Key><VersionId>v2</VersionId></DeleteMarker>", Some("v2"))
        }
        Call::List(Some(cursor)) if cursor == "v2" => page(&last_page, None),
        other => panic!("unexpected {other:?}"),
    })
    .await;
    assert_eq!(outcome, Ok(()));
    deleted.sort();
    assert_eq!(deleted, ["v1", "v2", "v3"]);
    assert!(confirmed);
}

#[tokio::test]
async fn replication_probe_cleanup_rejects_cycling_pagination_cursor() {
    let mut reached_cycle = false;
    let error = run_cleanup(&[], |call| match call {
        Call::List(None) => page("", Some("first")),
        Call::List(Some(cursor)) if cursor == "first" => page("", Some("second")),
        Call::List(Some(cursor)) if cursor == "second" => {
            reached_cycle = true;
            page("", Some("first"))
        }
        other => panic!("unexpected {other:?}"),
    })
    .await
    .expect_err("cyclic version listing cannot certify cleanup");
    assert!(reached_cycle);
    let description = error.to_ascii_lowercase();
    assert!(
        ["pagination", "cursor", "marker"]
            .iter()
            .any(|word| description.contains(word)),
        "{error}"
    );
}

#[tokio::test]
async fn replication_probe_cleanup_accepts_absent_key_and_version() {
    for code in ["NoSuchKey", "NoSuchVersion"] {
        let outcome = run_cleanup(&["v1"], |call| match call {
            Call::Delete(_) => Reply::Http(StatusCode::NOT_FOUND, format!("<Error><Code>{code}</Code></Error>")),
            Call::List(None) => page("", None),
            other => panic!("unexpected {other:?}"),
        })
        .await;
        assert_eq!(outcome, Ok(()));
    }
}

#[tokio::test]
async fn replication_probe_cleanup_validates_the_entire_listing_before_deleting() {
    let name = format!("<Name>{BUCKET}</Name><Prefix>{KEY}</Prefix>");
    let too_many = (0..65)
        .map(|id| format!("<Version><Key>{KEY}</Key><VersionId>overflow-{id}</VersionId></Version>"))
        .collect::<Vec<_>>()
        .join("");
    let terminal = |contents: &str| format!("<ListVersionsResult>{contents}</ListVersionsResult>");
    let cases = [
        ("error payload with HTTP 200", "<Error><Code>AccessDenied</Code></Error>".to_owned()),
        ("unidentified terminal page", terminal("")),
        (
            "wrong bucket",
            terminal(&format!("<Name>other-bucket</Name><Prefix>{KEY}</Prefix>{VERSION}")),
        ),
        (
            "wrong prefix",
            terminal(&format!(
                "<Name>{BUCKET}</Name><Prefix>another-key</Prefix><IsTruncated>false</IsTruncated>"
            )),
        ),
        (
            "wrong starting key",
            terminal(&format!("{name}<KeyMarker>other</KeyMarker><IsTruncated>false</IsTruncated>")),
        ),
        (
            "wrong starting version",
            terminal(&format!(
                "{name}<VersionIdMarker>wrong-cursor</VersionIdMarker><IsTruncated>false</IsTruncated>"
            )),
        ),
        (
            "version missing its ID",
            terminal(&format!("{name}<IsTruncated>false</IsTruncated><Version><Key>{KEY}</Key></Version>")),
        ),
        (
            "delete marker with empty ID",
            terminal(&format!(
                "{name}<IsTruncated>false</IsTruncated><DeleteMarker><Key>{KEY}</Key><VersionId/></DeleteMarker>"
            )),
        ),
        ("overlong page", terminal(&format!("{name}<IsTruncated>false</IsTruncated>{too_many}"))),
    ];
    for (case, invalid_page) in cases {
        let mut saw_invalid_page = false;
        let result = run_cleanup(&[], |call| match call {
            Call::List(None) => Reply::Http(StatusCode::OK, terminal(&format!(
                "{name}{VERSION}<IsTruncated>true</IsTruncated><NextKeyMarker>{KEY}</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker>"
            ))),
            Call::List(Some(marker)) if marker == "v1" => {
                saw_invalid_page = true;
                Reply::Http(StatusCode::OK, invalid_page.clone())
            }
            other => panic!("{case}: unexpected {other:?} before completion of the scan"),
        })
        .await;
        assert!(saw_invalid_page, "{case}: the scan never reached the terminal page");
        assert!(result.is_err(), "{case}: invalid list certified cleanup");
    }
}

#[tokio::test]
async fn replication_probe_cleanup_accepts_legacy_short_page_with_bucket_identity() {
    let mut listed = false;
    let outcome = run_cleanup(&[], |call| match call {
        Call::List(None) => {
            listed = true;
            Reply::Http(
                StatusCode::OK,
                format!("<ListVersionsResult><Name>{BUCKET}</Name><Prefix>{KEY}</Prefix></ListVersionsResult>"),
            )
        }
        other => panic!("unexpected {other:?} for an empty short page"),
    })
    .await;
    assert_eq!(outcome, Ok(()));
    assert!(listed);
}
