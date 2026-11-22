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

//! Signed browser forms and their storage effects on the embedded stack.

mod effective;

use super::super as stack;
use super::WireClient;

/// A browser form has no AWS SDK operation. Its own signed-wire matrix is checked below.
pub(crate) const BROWSER_POST: &str = "PostObject";

async fn post_wire_observes_success_controls_and_storage(server: rustfs::embedded::RustFSServer, native: bool) {
    const BUCKET: &str = "browser-post-wire";
    const ETAG: &str = "5d41402abc4b2a76b9719d911017c592";
    stack::create_bucket_in_the_store(BUCKET).await;
    let endpoint = server.endpoint();
    let client = WireClient {
        endpoint: &endpoint,
        access_key: server.access_key(),
        secret_key: server.secret_key(),
    };

    for (key, extra, known, status, code, stored) in [
        ("default", None, true, 204, None, true),
        ("ok", Some(("success_action_status", "200")), true, 200, None, true),
        ("created", Some(("success_action_status", "201")), true, 201, None, true),
        (
            "invalid-status",
            Some(("success_action_status", "202")),
            true,
            400,
            Some("MalformedPOSTRequest"),
            false,
        ),
        (
            "redirect",
            Some(("success_action_redirect", "https://example.invalid/after?x=1")),
            true,
            303,
            None,
            true,
        ),
        (
            "relative",
            Some(("success_action_redirect", "/after")),
            true,
            400,
            Some("MalformedPOSTRequest"),
            false,
        ),
        ("blank", Some(("success_action_redirect", "")), true, 400, Some("InvalidArgument"), true),
        ("unknown/file", None, false, 204, None, true),
        ("etag-list", Some(("if-match", "\"first\", \"second\"")), true, 204, None, true),
        ("etag-wildcard", Some(("if-match", "*")), true, 204, None, true),
        (
            "etag-spaced-wildcard",
            Some(("if-match", " * ")),
            true,
            400,
            Some("InvalidArgument"),
            false,
        ),
        (
            "kms",
            Some(("x-amz-server-side-encryption", "aws:kms")),
            true,
            501,
            Some("NotImplemented"),
            false,
        ),
    ] {
        let before = stack::s3_request_counts();
        let form_key = if key == "unknown/file" { "unknown/${filename}" } else { key };
        let mut fields = vec![("key", form_key)];
        fields.extend(extra);
        let answer = client.post_form(BUCKET, &fields, &[], known, true).await;
        assert_eq!(answer.status, status, "{key}: {}", answer.body);
        if let Some(code) = code {
            assert!(answer.body.contains(&format!("<Code>{code}</Code>")), "{key}: {}", answer.body);
        }
        let read = client.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0)).await;
        assert_eq!(read.status, if stored { 200 } else { 404 }, "{key}: {}", read.body);
        if stored {
            assert_eq!(read.body, "hello", "{key}");
            assert_eq!(read.headers.get("content-length").map(String::as_str), Some("5"), "{key}");
        }
        match status {
            200 | 204 => assert!(answer.body.is_empty(), "{key}: {}", answer.body),
            201 => {
                let location = format!("{endpoint}/{BUCKET}/{key}");
                assert!(answer.body.contains(&format!("<Location>{location}</Location>")), "{}", answer.body);
                assert!(answer.body.contains(&format!("<Bucket>{BUCKET}</Bucket>")), "{}", answer.body);
                assert!(answer.body.contains(&format!("<Key>{key}</Key>")), "{}", answer.body);
                assert!(answer.body.contains(&format!("<ETag>{ETAG}</ETag>")), "{}", answer.body);
            }
            303 => assert_eq!(
                answer.headers.get("location"),
                Some(&format!("https://example.invalid/after?x=1&bucket={BUCKET}&key={key}&etag={ETAG}")),
            ),
            _ => {
                let message = match key {
                    "invalid-status" => "success_action_status must be one of 200, 201, or 204",
                    "relative" => "success_action_redirect must be a valid absolute URL",
                    "blank" => "Invalid redirect URL",
                    "kms" => "SSE-KMS is not supported for POST object uploads",
                    "etag-spaced-wildcard" => "if-match",
                    _ => panic!("missing expected message"),
                };
                assert!(answer.body.contains(message), "{key}: {}", answer.body);
            }
        }
        if status < 300 || status == 303 {
            assert!(!answer.headers.contains_key("etag"), "{key}: HTTP ETag was sent");
        }
        if status == 204 {
            let operation = if native { "s3:PostObject" } else { "s3:PutObject" };
            let counted = stack::counted_since(&before);
            assert_eq!(
                counted
                    .iter()
                    .filter(|((method, _, _), _)| *method == "POST")
                    .collect::<Vec<_>>(),
                vec![(&("POST", operation, "2xx"), &1)],
                "{key}: {counted:?}"
            );
        }
    }
    assert_versioned_post(&server, BUCKET, &client).await;

    let anonymous = WireClient {
        endpoint: &endpoint,
        access_key: "",
        secret_key: "",
    };
    let wrong_secret = WireClient {
        secret_key: "wrong-form-secret",
        ..client
    };
    stack::create_user_without_a_policy(stack::NO_POLICY_USER).await;
    let no_policy = WireClient {
        access_key: stack::NO_POLICY_USER,
        secret_key: stack::USER_SECRET_KEY,
        ..client
    };
    let unknown_key = WireClient {
        access_key: "browserpostunknown",
        ..no_policy
    };
    for (caller, key, control, code) in [
        (&anonymous, "anonymous", None, "AccessDenied"),
        (&anonymous, "denied-status", Some(("success_action_status", "202")), "AccessDenied"),
        (&wrong_secret, "corrupted-signature", None, "SignatureDoesNotMatch"),
        (&no_policy, "no-policy", None, "AccessDenied"),
        (&unknown_key, "unknown-key", None, "InvalidAccessKeyId"),
    ] {
        let mut fields = vec![("key", key)];
        fields.extend(control);
        let answer = caller.post_form(BUCKET, &fields, &[], true, true).await;
        assert_eq!(answer.status, 403, "{key}: {}", answer.body);
        assert!(answer.body.contains(&format!("<Code>{code}</Code>")), "{key}: {}", answer.body);
        assert_eq!(
            client
                .request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                .await
                .status,
            404
        );
    }
    server.shutdown().await;
}

async fn assert_versioned_post(server: &rustfs::embedded::RustFSServer, bucket: &str, client: &WireClient<'_>) {
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    const KEY: &str = "versioned-form";
    stack::s3_client(&server.endpoint(), server.access_key(), server.secret_key())
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable versioning");
    let notify = rustfs_notify::notification_system().expect("embedded notification runtime");
    let mut notifications = notify.subscribe_live_events();
    let answer = client.post_form(bucket, &[("key", KEY)], &[], false, true).await;
    assert_eq!(answer.status, 204, "{}", answer.body);
    assert!(!answer.headers.contains_key("etag"), "legacy-profile POST carries no HTTP ETag");
    assert!(
        !answer.headers.contains_key("x-amz-version-id"),
        "legacy-profile POST carries no HTTP version"
    );
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = notifications.recv().await.expect("notification from this server");
            if event.s3.bucket.name == bucket && event.s3.object.key == KEY {
                break event;
            }
        }
    })
    .await
    .expect("committed browser upload notification");
    assert_eq!(event.event_name, rustfs_s3_types::EventName::ObjectCreatedPost);
    let get = client.request("GET", &format!("/{bucket}/{KEY}"), b"", &[], Some(0)).await;
    assert_eq!(get.status, 200);
    assert_eq!(get.body, "hello");
    let version = get.headers.get("x-amz-version-id").expect("stored version");
    assert!(!version.is_empty());
    assert_eq!(event.s3.object.version_id.as_ref(), Some(version));
}

#[test]
#[serial_test::serial]
fn gateway_post_wire_preserves_success_controls_and_storage() {
    stack::with_stack(stack::GATEWAY, |server| post_wire_observes_success_controls_and_storage(server, true));
}

async fn post_response_location_is_absolute_and_percent_encoded(server: rustfs::embedded::RustFSServer) {
    const BUCKET: &str = "post-location-encoding";
    stack::create_bucket_in_the_store(BUCKET).await;
    let endpoint = server.endpoint();
    let client = WireClient {
        endpoint: &endpoint,
        access_key: server.access_key(),
        secret_key: server.secret_key(),
    };
    for (key, encoded_key) in [
        ("test/a+b c%d.txt", "test/a%2Bb%20c%25d.txt"),
        ("naïve/résumé.txt", "na%C3%AFve/r%C3%A9sum%C3%A9.txt"),
        ("a&b?c#d.txt", "a%26b%3Fc%23d.txt"),
        ("a-b_c.d~e/photo.jpg", "a-b_c.d~e/photo.jpg"),
    ] {
        let answer = client
            .post_form(BUCKET, &[("key", key), ("success_action_status", "201")], &[], true, true)
            .await;
        assert_eq!(answer.status, 201, "{key}: {}", answer.body);
        let location = format!("{endpoint}/{BUCKET}/{encoded_key}");
        let xml_key = key.replace("&", "&amp;");
        assert!(
            answer.body.contains(&format!("<Location>{location}</Location>")),
            "{key}: {}",
            answer.body
        );
        assert!(answer.body.contains(&format!("<Key>{xml_key}</Key>")), "{key}: {}", answer.body);
        assert!(
            !answer.body.contains(&format!("<Location>/{BUCKET}/{key}</Location>")),
            "{key}: {}",
            answer.body
        );
    }
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn gateway_post_response_location_is_absolute_and_percent_encoded() {
    stack::with_stack(stack::GATEWAY, post_response_location_is_absolute_and_percent_encoded);
}

#[test]
#[serial_test::serial]
fn gateway_post_filename_substitution_supports_streamed_uploads() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-filename";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        for (prefix, known) in [("known", true), ("stream", false)] {
            let key = format!("{prefix}/${{filename}}");
            let answer = root.post_form(BUCKET, &[("key", &key)], &[], known, true).await;
            assert_eq!(answer.status, 204, "{prefix}: {}", answer.body);
            let read = root
                .request("GET", &format!("/{BUCKET}/{prefix}/file"), b"", &[], Some(0))
                .await;
            assert_eq!(read.status, 200, "{prefix}: {}", read.body);
            assert_eq!(read.body, "hello");
        }
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_final_stream_failure_does_not_publish() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-incomplete";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let client = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        for (key, known, complete, expected) in [
            ("chunked-truncated", false, false, None),
            ("known-truncated", true, false, None),
            ("wrong-digest", false, true, Some("BadDigest")),
        ] {
            let mut fields = vec![("key", key)];
            if expected.is_some() {
                fields.push(("content-md5", "AAAAAAAAAAAAAAAAAAAAAA=="));
            }
            let answer = client.post_form(BUCKET, &fields, &[], known, complete).await;
            assert!(answer.status >= 400, "{key}: {}", answer.body);
            if let Some(code) = expected {
                assert!(answer.body.contains(&format!("<Code>{code}</Code>")), "{key}: {}", answer.body);
            }
            assert_eq!(
                client
                    .request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                    .await
                    .status,
                404,
                "{key}"
            );
        }
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_declared_file_length_does_not_replace_complete_form() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-false-tail";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let client = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let closing_length = format!("\r\n--{}--\r\n", super::POST_BOUNDARY).len();
        for (label, size) in [("small", 64), ("streamed", 17 * 1024 * 1024)] {
            let existing = format!("{label}-existing");
            let absent = format!("{label}-absent");
            let path = format!("/{BUCKET}/{existing}");
            let original = b"original";
            assert_eq!(client.request("PUT", &path, original, &[], Some(original.len())).await.status, 200);
            // The declared HTTP length implies `size` file bytes, but the
            // supposed closing delimiter is replaced by ordinary file bytes.
            let invalid = "x".repeat(size + closing_length);
            for key in [&existing, &absent] {
                let answer = client
                    .post_form_contents(BUCKET, &[("key", key)], &[], &invalid, true, false)
                    .await;
                assert!(answer.status >= 400, "{key}: {}", answer.body);
            }
            let existing_answer = client.request("GET", &path, b"", &[], Some(0)).await;
            assert_eq!(existing_answer.status, 200);
            assert_eq!(existing_answer.body, "original");
            let absent_answer = client.request("GET", &format!("/{BUCKET}/{absent}"), b"", &[], Some(0)).await;
            assert_eq!(absent_answer.status, 404);
        }
        let control = client.post_form(BUCKET, &[("key", "control")], &[], true, true).await;
        assert_eq!(control.status, 204, "{}", control.body);
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_uses_verified_form_session_policy() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        use rustfs_iam::sys::SESSION_POLICY_NAME;
        use rustfs_policy::auth::get_new_credentials_with_metadata;
        use serde_json::json;
        use std::collections::HashMap;

        const BUCKET: &str = "browser-post-session";
        const PARENT: &str = "postsessionparent";
        stack::create_bucket_in_the_store(BUCKET).await;
        stack::create_user_allowed_on_one_bucket(PARENT, "s3:PutObject", &format!("{BUCKET}/*")).await;
        let iam = stack::get_global_app_context()
            .expect("embedded server context")
            .iam()
            .handle();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect":"Allow", "Action":["s3:PutObject"], "Resource":[format!("arn:aws:s3:::{BUCKET}/allowed")]}],
        });
        let metadata = HashMap::from([
            (
                "exp".to_owned(),
                json!((time::OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()),
            ),
            ("parent".to_owned(), json!(PARENT)),
            (
                SESSION_POLICY_NAME.to_owned(),
                json!(base64_simd::URL_SAFE_NO_PAD.encode_to_string(policy.to_string().as_bytes())),
            ),
        ]);
        let mut credential =
            get_new_credentials_with_metadata(&metadata, server.secret_key()).expect("locally issued STS credential");
        credential.parent_user = PARENT.to_owned();
        let parent = iam.get_user(PARENT).await.expect("parent user").credentials;
        iam.set_temp_user_from_parent(&credential, &parent)
            .await
            .expect("publish STS user");

        let endpoint = server.endpoint();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let long_term = WireClient {
            endpoint: &endpoint,
            access_key: PARENT,
            secret_key: stack::USER_SECRET_KEY,
        };
        let temporary = WireClient {
            endpoint: &endpoint,
            access_key: &credential.access_key,
            secret_key: &credential.secret_key,
        };
        let token = credential.session_token.as_str();
        for (key, form_token, wire_token, status) in [
            ("allowed", Some(token), Some("wrong-wire-token"), 204),
            ("denied", Some(token), None, 403),
            ("wrong", Some("wrong-form-token"), Some(token), 403),
            ("missing", None, Some(token), 403),
        ] {
            let mut fields = vec![("key", key)];
            fields.extend(form_token.map(|value| ("x-amz-security-token", value)));
            let headers: Vec<_> = wire_token.map(|value| ("x-amz-security-token", value)).into_iter().collect();
            let answer = temporary.post_form(BUCKET, &fields, &headers, true, true).await;
            assert_eq!(answer.status, status, "{key}");
            if key == "denied" {
                assert!(answer.body.contains("<Code>AccessDenied</Code>"));
            }
            let get = root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0)).await;
            assert_eq!(get.status, if status == 204 { 200 } else { 404 }, "{key}");
            if status == 204 {
                assert_eq!(get.body, "hello");
            }
        }
        let control = long_term.post_form(BUCKET, &[("key", "denied")], &[], true, true).await;
        assert_eq!(control.status, 204, "the base IAM policy allows the denied key");
        server.shutdown().await;
    });
}

async fn post_wire_authorizes_form_values_before_success_status(server: rustfs::embedded::RustFSServer, native: bool) {
    const BUCKET: &str = "browser-post-policy";
    const USER: &str = "browserpostpolicy";
    stack::create_bucket_in_the_store(BUCKET).await;
    stack::create_user_without_a_policy(USER).await;
    let iam = stack::get_global_app_context()
        .expect("embedded server context")
        .iam()
        .handle();
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[
        {{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::{BUCKET}/*"}},
        {{"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::{BUCKET}/*",
        "Condition":{{"StringEquals":{{"s3:RequestObjectTag/classification":"private"}}}}}}
        ]}}"#
    );
    iam.set_policy(
        USER,
        rustfs_policy::policy::Policy::parse_config(policy.as_bytes()).expect("valid IAM policy"),
    )
    .await
    .expect("store policy");
    iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
        .await
        .expect("attach policy");

    let endpoint = server.endpoint();
    let root = WireClient {
        endpoint: &endpoint,
        access_key: server.access_key(),
        secret_key: server.secret_key(),
    };
    let caller = WireClient {
        endpoint: &endpoint,
        access_key: USER,
        secret_key: stack::USER_SECRET_KEY,
    };
    let allowed = caller
        .post_form(
            BUCKET,
            &[
                ("key", "allowed"),
                ("x-amz-tagging", "classification=public"),
                ("x-amz-meta-purpose", "form"),
            ],
            &[("x-amz-tagging", "classification=private")],
            true,
            true,
        )
        .await;
    assert_eq!(allowed.status, 204, "{}", allowed.body);
    let get = root.request("GET", &format!("/{BUCKET}/allowed"), b"", &[], Some(0)).await;
    assert_eq!(get.status, 200);
    assert_eq!(get.headers.get("x-amz-meta-purpose").map(String::as_str), Some("form"));

    let specific_form_etag = caller
        .post_form(
            BUCKET,
            &[("key", "etag-needs-read"), ("if-match", "\"first\", \"second\"")],
            &[],
            true,
            true,
        )
        .await;
    assert_eq!(specific_form_etag.status, 403, "{}", specific_form_etag.body);
    assert!(specific_form_etag.body.contains("<Code>AccessDenied</Code>"));
    let stored = root
        .request("GET", &format!("/{BUCKET}/etag-needs-read"), b"", &[], Some(0))
        .await;
    assert_eq!(stored.status, 404);
    for (key, success, expected) in [
        (
            "wire-etag-needs-read",
            None,
            if native { (403, "AccessDenied") } else { (404, "NoSuchKey") },
        ),
        (
            "wire-etag-before-status",
            Some(("success_action_status", "202")),
            if native {
                (403, "AccessDenied")
            } else {
                (400, "MalformedPOSTRequest")
            },
        ),
    ] {
        let mut fields = vec![("key", key), ("if-match", "*")];
        fields.extend(success);
        let answer = caller
            .post_form(BUCKET, &fields, &[("if-match", "\"specific\"")], true, true)
            .await;
        assert_eq!(answer.status, expected.0, "{}", answer.body);
        assert!(answer.body.contains(&format!("<Code>{}</Code>", expected.1)), "{}", answer.body);
        let stored = root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0)).await;
        assert_eq!(stored.status, 404);
    }

    for (key, tag, code) in [
        ("denied-before-status", "classification=private", "AccessDenied"),
        ("invalid-status-after-allow", "classification=public", "MalformedPOSTRequest"),
    ] {
        let answer = caller
            .post_form(
                BUCKET,
                &[("key", key), ("x-amz-tagging", tag), ("success_action_status", "202")],
                &[("x-amz-tagging", "classification=public")],
                true,
                true,
            )
            .await;
        assert!(answer.body.contains(&format!("<Code>{code}</Code>")), "{key}: {}", answer.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                .await
                .status,
            404,
            "{key}"
        );
    }
    // The initial PutObject gate sees the version header. The final POST gate
    // sees the form resource, which has no version, and must refuse it.
    let version_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[
        {{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::{BUCKET}/version-condition",
        "Condition":{{"StringEquals":{{"s3:versionid":"null"}}}}}}
        ]}}"#
    );
    iam.set_policy(
        USER,
        rustfs_policy::policy::Policy::parse_config(version_policy.as_bytes()).expect("version-conditioned PutObject policy"),
    )
    .await
    .expect("replace attached writer policy");
    for (status, expected) in [("202", (400, "MalformedPOSTRequest")), ("204", (403, "AccessDenied"))] {
        let answer = caller
            .post_form(
                BUCKET,
                &[("key", "version-condition"), ("success_action_status", status)],
                &[("x-rustfs-source-version-id", "null")],
                true,
                true,
            )
            .await;
        assert_eq!(answer.status, expected.0, "{}", answer.body);
        assert!(answer.body.contains(&format!("<Code>{}</Code>", expected.1)), "{}", answer.body);
        let stored = root
            .request("GET", &format!("/{BUCKET}/version-condition"), b"", &[], Some(0))
            .await;
        assert_eq!(stored.status, 404);
    }
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn gateway_post_wire_authorizes_form_values_before_success_status() {
    stack::with_stack(stack::GATEWAY, |server| {
        post_wire_authorizes_form_values_before_success_status(server, true)
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_staging_cannot_publish_to_a_recreated_bucket() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        use super::{POST_BOUNDARY, parse_wire_answer};
        use rustfs_storage_api::BucketOperations as _;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        const BUCKET: &str = "browser-post-recreated";
        const KEY: &str = "staged";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let mut socket = begin_staged_post(&root, BUCKET, KEY, "close").await;

        let store = stack::get_global_app_context()
            .expect("embedded server context")
            .object_store();
        store
            .delete_bucket(BUCKET, &Default::default())
            .await
            .expect("delete the original bucket");
        stack::create_bucket_in_the_store(BUCKET).await;
        let rest = format!("\r\n--{POST_BOUNDARY}--\r\n");
        socket
            .write_all(format!("{:x}\r\n{rest}\r\n0\r\n\r\n", rest.len()).as_bytes())
            .await
            .expect("finish staging");
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut raw))
            .await
            .expect("POST terminated")
            .expect("read wire response");
        let answer = parse_wire_answer(raw);
        let observed = root.request("GET", &format!("/{BUCKET}/{KEY}"), b"", &[], Some(0)).await;
        assert_eq!((answer.status, observed.status), (404, 404), "POST {} GET {}", answer.body, observed.body);
        assert!(answer.body.contains("<Code>NoSuchBucket</Code>"), "{}", answer.body);

        let control = root.post_form(BUCKET, &[("key", KEY)], &[], false, true).await;
        assert_eq!(control.status, 204, "{}", control.body);
        assert_eq!(root.request("GET", &format!("/{BUCKET}/{KEY}"), b"", &[], Some(0)).await.body, "hello");
        server.shutdown().await;
    });
}

async fn begin_staged_post(root: &WireClient<'_>, bucket: &str, key: &str, connection: &str) -> tokio::net::TcpStream {
    use super::POST_BOUNDARY;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt as _;
    let host = root.endpoint.strip_prefix("http://").expect("loopback HTTP");
    let op_count = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|series| series.op == "s3:PostObject")
            .map_or(0, |series| series.total)
    };
    let before = op_count();
    let mut socket = tokio::net::TcpStream::connect(host).await.expect("connect test listener");
    let prelude = root.post_prelude(bucket, &[("key", key)]);
    let initial = format!("{prelude}hello");
    socket.write_all(format!(
            "POST /{bucket} HTTP/1.1\r\nHost: {host}\r\nConnection: {connection}\r\nContent-Type: multipart/form-data; boundary={POST_BOUNDARY}\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{initial}\r\n",
            initial.len()
        ).as_bytes()).await.expect("send the form and first file chunk");
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        while op_count() == before {
            tick.tick().await;
        }
    })
    .await
    .expect("POST was authorized with the original bucket present");
    assert_eq!(op_count(), before + 1);

    socket
}

#[test]
#[serial_test::serial]
fn gateway_post_staging_rechecks_policy_before_publication() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        use super::{POST_BOUNDARY, parse_wire_answer};
        use std::time::Duration;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        const BUCKET: &str = "browser-post-policy-change";
        const USER: &str = "postpolicychange";
        const KEY: &str = "staged";
        stack::create_bucket_in_the_store(BUCKET).await;
        stack::create_user_without_a_policy(USER).await;
        let iam = stack::get_global_app_context()
            .expect("embedded server context")
            .iam()
            .handle();
        let policy = |effect| {
            let document = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{"Effect": effect, "Action": "s3:PutObject", "Resource": format!("arn:aws:s3:::{BUCKET}/*")}]
            });
            rustfs_policy::policy::Policy::parse_config(document.to_string().as_bytes()).expect("valid test policy")
        };
        iam.set_policy(USER, policy("Allow")).await.expect("install initial policy");
        iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
            .await
            .expect("attach policy");
        let endpoint = server.endpoint();
        let caller = WireClient {
            endpoint: &endpoint,
            access_key: USER,
            secret_key: stack::USER_SECRET_KEY,
        };
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let mut socket = begin_staged_post(&caller, BUCKET, KEY, "close").await;
        iam.set_policy(USER, policy("Deny"))
            .await
            .expect("revoke permission during staging");
        let rest = format!("\r\n--{POST_BOUNDARY}--\r\n");
        socket
            .write_all(format!("{:x}\r\n{rest}\r\n0\r\n\r\n", rest.len()).as_bytes())
            .await
            .expect("finish staging");
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut raw))
            .await
            .expect("POST terminated")
            .expect("read wire response");
        let answer = parse_wire_answer(raw);
        assert_eq!(answer.status, 403, "{}", answer.body);
        assert!(answer.body.contains("<Code>AccessDenied</Code>"), "{}", answer.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/{KEY}"), b"", &[], Some(0))
                .await
                .status,
            404
        );
        let control = root.post_form(BUCKET, &[("key", KEY)], &[], false, true).await;
        assert_eq!(control.status, 204, "{}", control.body);
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_stalled_file_read_times_out_without_publishing() {
    temp_env::with_var(rustfs_config::ENV_HTTP_REQUEST_BODY_READ_TIMEOUT, Some("1"), || {
        stack::with_stack(stack::GATEWAY, |server| async move {
            use super::parse_wire_answer;
            use std::time::Duration;
            use tokio::io::AsyncReadExt as _;

            const BUCKET: &str = "browser-post-stall";
            stack::create_bucket_in_the_store(BUCKET).await;
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            for connection in ["close", "keep-alive"] {
                let key = format!("never-finished-{connection}");
                let mut socket = begin_staged_post(&root, BUCKET, &key, connection).await;
                let mut raw = Vec::new();
                match tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut raw)).await {
                    Ok(result) => {
                        result.expect("read wire response");
                    }
                    Err(_) => panic!("stalled POST did not terminate; partial response: {}", String::from_utf8_lossy(&raw)),
                }
                let answer = parse_wire_answer(raw);
                assert_eq!(answer.status, 400, "{}", answer.body);
                assert_eq!(answer.headers.get("connection").map(String::as_str), Some("close"));
                assert!(answer.body.contains("<Code>RequestTimeout</Code>"), "{}", answer.body);
                assert_eq!(
                    root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                        .await
                        .status,
                    404
                );
            }
            server.shutdown().await;
        });
    });
}
