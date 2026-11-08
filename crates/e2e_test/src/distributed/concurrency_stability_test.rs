// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::harness::{DistCluster, DistLayout, TestResult, payload_for, s3_probe_client, sha256_hex, unique_bucket};
use crate::common::init_logging;
use aws_sdk_s3::{
    Client,
    error::{ProvideErrorMetadata, SdkError},
    primitives::ByteStream,
};
use std::any::TypeId;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::Instant;

const LOCK_ADMISSION_BUDGET: Duration = Duration::from_secs(45);

async fn after_lock_contention<T, E, F, Fut>(operation: &str, mut call: F) -> TestResult<Result<T, SdkError<E>>>
where
    E: Debug + ProvideErrorMetadata + 'static,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SdkError<E>>>,
{
    let started = Instant::now();
    let mut attempts = 0;
    // The workload uses unique unversioned keys. Repeat only explicitly
    // advertised lock backpressure; every step still needs its real result.
    let outcome = tokio::time::timeout(LOCK_ADMISSION_BUDGET, async {
        loop {
            attempts += 1;
            let result = call().await;
            let retry = result.as_ref().err().is_some_and(|error| {
                let Some(response) = error.raw_response() else {
                    return false;
                };
                let Some(service_error) = error.as_service_error() else {
                    return false;
                };
                // HEAD has no XML error body, so its SDK service error can
                // lack a code even when the server supplies retry headers.
                let namespace_code = service_error.code() == Some("ServiceUnavailable")
                    || (service_error.code().is_none()
                        && TypeId::of::<E>() == TypeId::of::<aws_sdk_s3::operation::head_object::HeadObjectError>());
                response.status().as_u16() == 503
                    && namespace_code
                    && response.headers().get("x-rustfs-error-cause") == Some("namespace_lock_timeout")
                    && response.headers().get("retry-after") == Some("1")
            });
            if !retry {
                return result;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;
    if attempts > 1 || !matches!(&outcome, Ok(Ok(_))) {
        eprintln!(
            "mixed workload request: operation={operation}, attempts={attempts}, elapsed={:?}",
            started.elapsed()
        );
    }
    outcome.map_err(|_| {
        format!("{operation} exceeded its {LOCK_ADMISSION_BUDGET:?} request budget after {attempts} attempts").into()
    })
}

async fn assert_workload_bytes(client: &Client, bucket: &str, key: &str, expected: &[u8]) -> TestResult {
    let output = after_lock_contention(&format!("GET {key}"), || client.get_object().bucket(bucket).key(key).send()).await??;
    let actual = output.body.collect().await?.into_bytes();
    if actual.as_ref() != expected {
        return Err(format!(
            "object {key} bytes mismatch: expected {} bytes sha256={} got {} bytes sha256={}",
            expected.len(),
            sha256_hex(expected),
            actual.len(),
            sha256_hex(&actual)
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type Reply = (u16, &'static str, Option<&'static str>, Option<&'static str>, &'static str);

    fn client(respond: impl Fn(usize) -> Reply + Send + Sync + 'static) -> (Client, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&requests);
        let transport = infallible_client_fn(move |_| {
            let (status, code, cause, retry_after, body) = respond(seen.fetch_add(1, Ordering::SeqCst));
            let body = if code.is_empty() {
                body.to_string()
            } else {
                format!("<Error><Code>{code}</Code></Error>")
            };
            let mut response = http::Response::builder()
                .status(status)
                .header("content-type", "application/xml")
                .header("content-length", body.len());
            if let Some(cause) = cause {
                response = response.header("x-rustfs-error-cause", cause);
            }
            if let Some(retry_after) = retry_after {
                response = response.header("retry-after", retry_after);
            }
            response.body(SdkBody::from(body)).expect("scripted S3 response")
        });
        let config = crate::common::build_test_s3_config(
            "http://contention.example.invalid",
            "synthetic-access",
            "synthetic-secret",
            None,
            "contention-test",
        );
        let client = Client::from_conf(config.to_builder().http_client(transport).build());
        (s3_probe_client(&client), requests)
    }

    fn contention() -> Reply {
        (503, "ServiceUnavailable", Some("namespace_lock_timeout"), Some("1"), "")
    }

    #[tokio::test(start_paused = true)]
    async fn advertised_contention_recovers_after_standard_sdk_attempts_are_exhausted() -> TestResult {
        let (client, requests) = client(|attempt| {
            if attempt < 3 {
                contention()
            } else {
                (200, "", None, None, "")
            }
        });
        let standard = Client::from_conf(
            client
                .config()
                .to_builder()
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(3))
                .build(),
        );
        standard
            .put_object()
            .bucket("bucket")
            .key("key")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .expect_err("three advertised lock responses exhaust the standard SDK policy");
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        requests.store(0, Ordering::SeqCst);
        let started = Instant::now();
        after_lock_contention("PUT recovery", || {
            client
                .put_object()
                .bucket("bucket")
                .key("key")
                .body(ByteStream::from_static(b"body"))
                .send()
        })
        .await??;
        assert_eq!(requests.load(Ordering::SeqCst), 4);
        assert!(started.elapsed() >= Duration::from_secs(3), "honor each Retry-After response");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn bodyless_head_contention_uses_explicit_retry_headers() -> TestResult {
        let (client, requests) = client(|attempt| {
            if attempt == 0 {
                (503, "", Some("namespace_lock_timeout"), Some("1"), "")
            } else {
                (200, "", None, None, "")
            }
        });
        after_lock_contention("HEAD recovery", || client.head_object().bucket("bucket").key("key").send()).await??;
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_or_malformed_responses_are_not_retried() -> TestResult {
        for reply in [
            (500, "InternalError", Some("namespace_lock_timeout"), Some("1"), ""),
            (403, "AccessDenied", None, None, ""),
            (404, "NoSuchKey", None, None, ""),
            (503, "ServiceUnavailable", Some("storage_write_quorum"), None, ""),
            (503, "ServiceUnavailable", Some("namespace_lock_unavailable"), Some("1"), ""),
            (503, "InternalError", Some("namespace_lock_timeout"), Some("1"), ""),
            (503, "", Some("namespace_lock_timeout"), Some("1"), ""),
            (503, "ServiceUnavailable", None, Some("1"), ""),
            (503, "ServiceUnavailable", Some("namespace_lock_timeout"), None, ""),
            (503, "ServiceUnavailable", Some("namespace_lock_timeout"), Some("2"), ""),
        ] {
            let (client, requests) = client(move |_| reply);
            let result = after_lock_contention("PUT fatal response", || {
                client
                    .put_object()
                    .bucket("bucket")
                    .key("key")
                    .body(ByteStream::from_static(b"body"))
                    .send()
            })
            .await?;
            assert!(result.is_err(), "fatal response cannot become success: {reply:?}");
            assert_eq!(requests.load(Ordering::SeqCst), 1, "fatal response must remain visible: {reply:?}");
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_lock_contention_exhausts_the_request_budget() {
        let (client, requests) = client(|_| contention());
        let started = Instant::now();
        let result = after_lock_contention("PUT unavailable", || {
            client
                .put_object()
                .bucket("bucket")
                .key("key")
                .body(ByteStream::from_static(b"body"))
                .send()
        })
        .await;
        assert!(result.is_err(), "persistent contention cannot acknowledge an operation");
        assert!(started.elapsed() >= LOCK_ADMISSION_BUDGET);
        assert!(requests.load(Ordering::SeqCst) > 1);
        assert!(requests.load(Ordering::SeqCst) <= LOCK_ADMISSION_BUDGET.as_secs() as usize + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn acknowledged_wrong_bytes_fail_without_retry() {
        let (client, requests) = client(|_| (200, "", None, None, "wrong bytes"));
        let error = assert_workload_bytes(&client, "bucket", "key", b"expected bytes")
            .await
            .expect_err("a successful but incorrect read is a consistency failure");
        assert!(error.to_string().contains("bytes mismatch"));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn four_node_high_concurrency_mixed_workload_is_consistent_on_every_node() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("conc");
    dist.create_bucket(&bucket).await?;
    // This consistency probe owns its admission policy; unrelated responses
    // must not be hidden by the SDK's general-purpose retries.
    let clients = Arc::new(dist.clients()?.iter().map(s3_probe_client).collect::<Vec<_>>());
    let worker_count = 24;
    let rounds = 4;
    let barrier = Arc::new(Barrier::new(worker_count));

    let mut handles = Vec::new();
    for idx in 0..worker_count {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let bucket = bucket.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let writer = &clients[idx % clients.len()];
            let reader = &clients[(idx + 1) % clients.len()];
            let copier = &clients[(idx + 2) % clients.len()];
            let mut retained = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let key = format!("source/worker-{idx:02}-round-{round}.bin");
                let copy_key = format!("retained/worker-{idx:02}-round-{round}.bin");
                let body = payload_for(&key, 64 * 1024);
                let context = format!("worker={idx} round={round}");
                after_lock_contention(&format!("{context} PUT source"), || {
                    writer
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(body.clone()))
                        .send()
                })
                .await??;

                let head = after_lock_contention(&format!("{context} HEAD source"), || {
                    reader.head_object().bucket(&bucket).key(&key).send()
                })
                .await??;
                if head.content_length() != Some(body.len() as i64) {
                    return Err(format!("HEAD returned the wrong size for {key}: {head:?}").into());
                }
                assert_workload_bytes(reader, &bucket, &key, &body).await?;

                after_lock_contention(&format!("{context} COPY retained"), || {
                    copier
                        .copy_object()
                        .bucket(&bucket)
                        .key(&copy_key)
                        .copy_source(format!("{bucket}/{key}"))
                        .send()
                })
                .await??;
                assert_workload_bytes(writer, &bucket, &copy_key, &body).await?;

                after_lock_contention(&format!("{context} DELETE source"), || {
                    writer.delete_object().bucket(&bucket).key(&key).send()
                })
                .await??;
                let missing = after_lock_contention(&format!("{context} HEAD deleted source"), || {
                    reader.head_object().bucket(&bucket).key(&key).send()
                })
                .await?
                .expect_err("deleted source key must not remain visible");
                if missing.raw_response().map(|response| response.status().as_u16()) != Some(404) {
                    return Err(format!("deleted source {key} returned an unexpected result: {missing:?}").into());
                }
                retained.push((copy_key, body));
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(retained)
        }));
    }

    let mut inventory = Vec::new();
    for handle in handles {
        inventory.extend(handle.await??);
    }
    assert_eq!(
        inventory.len(),
        worker_count * rounds,
        "every worker round must retain its acknowledged copy"
    );

    let expected_keys: BTreeSet<_> = inventory.iter().map(|(key, _)| key.as_str()).collect();
    for (node_idx, client) in clients.iter().enumerate() {
        let listed = after_lock_contention(&format!("node={node_idx} LIST retained"), || {
            client.list_objects_v2().bucket(&bucket).prefix("retained/").send()
        })
        .await??;
        let listed_keys: BTreeSet<_> = listed.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(listed_keys, expected_keys, "node {node_idx} returned a divergent retained-key listing");
        for (key, body) in &inventory {
            assert_workload_bytes(client, &bucket, key, body)
                .await
                .map_err(|error| format!("node {node_idx} failed to read {key}: {error}"))?;
        }
    }
    Ok(())
}
