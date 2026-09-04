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

//! Provider interoperability cases (ODM-20, rustfs/backlog#2167).
//!
//! One body per case, run against whichever source the environment names:
//! the in-process fake source locally, a MinIO container or a real cloud
//! provider under `.github/workflows/on-demand-migration-interop.yml`. The
//! source is resolved by [`OdmInteropEnv`], so a provider difference in
//! path-style addressing, region handling, ETag shape or list pagination
//! shows up as one of these assertions failing rather than as a second,
//! drifting copy of the suite.
//!
//! Consequently these cases assert only on what every S3 implementation has
//! to agree on — what the client receives and what RustFS stored — never on
//! the fake source's request journal, which a real provider does not have.
//! The journal-backed expectations stay in `get_basic_test.rs` and
//! `interaction_test.rs`.
//!
//! The first three cases are the minimum a cloud provider is asked for (GET
//! miss, HEAD miss, merged list pagination); the backfill case runs against
//! the MinIO container, whose object count the lane raises well past the fake
//! source's caps.

use super::common::{BackfillRequest, BoxError, OdmInteropEnv, SeedObject, interop_backfill_objects};
use bytes::Bytes;
use std::time::Duration;

type TestResult = Result<(), BoxError>;

const ODM_RESPONSE_HEADER: &str = "x-rustfs-on-demand-migration";
/// Background pulls land after the response that triggered them; generous for
/// a loaded runner talking to a container.
const SETTLE: Duration = Duration::from_secs(90);

/// Position-dependent payload so a misaligned or truncated copy is caught.
fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

/// A GET miss is answered from the source with the source's own ETag, and the
/// object it stored serves every later read locally.
#[tokio::test]
async fn interop_get_miss_pulls_from_the_source_and_serves_locally() -> TestResult {
    let case =
        OdmInteropEnv::start("interop_get_miss_pulls_from_the_source_and_serves_locally", "odm-interop-get", |_| {}).await?;
    let key = "interop/report.bin";
    let body = payload(200 * 1024);
    let etag = case.seed(&[SeedObject::new(key, body.clone())]).await?.remove(0);
    let quoted_etag = format!("\"{etag}\"");

    let first = case.env.raw_get(&case.bucket, key).await?;
    assert_eq!(first.status, 200, "{}", String::from_utf8_lossy(&first.body));
    assert_eq!(first.header(ODM_RESPONSE_HEADER), Some("source"), "a source answer is marked");
    assert_eq!(first.header("content-length"), Some(body.len().to_string().as_str()));
    assert_eq!(
        first.header("etag"),
        Some(quoted_etag.as_str()),
        "the source ETag is passed through unchanged"
    );
    assert_eq!(first.body, body, "the client receives the source bytes");

    assert!(
        case.env.wait_local_listed(&case.bucket, key, SETTLE).await?,
        "the inline pull must store the object locally"
    );
    let second = case.env.raw_get(&case.bucket, key).await?;
    assert_eq!(second.status, 200, "{}", String::from_utf8_lossy(&second.body));
    assert_eq!(second.header(ODM_RESPONSE_HEADER), None, "a local hit carries no source marker");
    assert_eq!(second.body, body, "the local copy is the source bytes");
    assert_eq!(
        second.header("etag"),
        Some(quoted_etag.as_str()),
        "preserve_etag keeps the source ETag on the stored object"
    );
    case.finish().await
}

/// A HEAD miss is proxied with the source's size and ETag and stores nothing.
#[tokio::test]
async fn interop_head_miss_answers_from_the_source_without_persisting() -> TestResult {
    let case =
        OdmInteropEnv::start("interop_head_miss_answers_from_the_source_without_persisting", "odm-interop-head", |_| {}).await?;
    let key = "interop/head-only.bin";
    let body = payload(9_000);
    let etag = case.seed(&[SeedObject::new(key, body.clone())]).await?.remove(0);

    let head = case
        .env
        .raw_object_request(http::Method::HEAD, &case.bucket, key, &[])
        .await?;
    assert_eq!(head.status, 200, "HEAD must be answered from the source");
    assert_eq!(head.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(head.header("content-length"), Some(body.len().to_string().as_str()));
    assert_eq!(head.header("etag"), Some(format!("\"{etag}\"").as_str()));
    assert!(head.body.is_empty(), "a HEAD carries no body");
    case.env.assert_local_absent(&case.bucket, key).await;

    // A key the source does not hold is a plain 404, not a source error.
    let missing = case
        .env
        .raw_object_request(http::Method::HEAD, &case.bucket, "interop/absent.bin", &[])
        .await?;
    assert_eq!(missing.status, 404, "a source miss is a 404");
    case.finish().await
}

/// The merged `ListObjectsV2` pages the source namespace in byte order, keeps
/// every page within `max_keys`, and lets a local object win a shared key.
#[tokio::test]
async fn interop_list_through_pages_the_source_namespace() -> TestResult {
    const SOURCE_KEYS: usize = 120;
    const PAGE_SIZE: i32 = 50;
    const SOURCE_BODY_LEN: usize = 3;
    const LOCAL_BODY_LEN: usize = 11;

    let case = OdmInteropEnv::start("interop_list_through_pages_the_source_namespace", "odm-interop-list", |spec| {
        spec.policy.list_through = true
    })
    .await?;
    let keys: Vec<String> = (0..SOURCE_KEYS).map(|index| format!("page/obj-{index:05}")).collect();
    let seeds: Vec<SeedObject> = keys
        .iter()
        .map(|key| SeedObject::new(key.clone(), payload(SOURCE_BODY_LEN)))
        .collect();
    case.seed(&seeds).await?;

    // Five keys the local bucket also holds, with a body length that tells the
    // two sides apart in the listing.
    let shared: Vec<String> = keys.iter().step_by(25).cloned().collect();
    for key in &shared {
        case.env
            .client
            .put_object()
            .bucket(&case.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(payload(LOCAL_BODY_LEN)))
            .send()
            .await?;
    }

    let mut listed: Vec<(String, i64)> = Vec::new();
    let mut token: Option<String> = None;
    let mut completed = false;
    for _ in 0..SOURCE_KEYS {
        let page = case
            .env
            .client
            .list_objects_v2()
            .bucket(&case.bucket)
            .prefix("page/")
            .max_keys(PAGE_SIZE)
            .set_continuation_token(token.take())
            .send()
            .await?;
        assert!(page.contents().len() <= PAGE_SIZE as usize, "a merged page must not exceed max_keys");
        for object in page.contents() {
            listed.push((object.key().unwrap_or_default().to_string(), object.size().unwrap_or_default()));
        }
        if !page.is_truncated().unwrap_or(false) {
            completed = true;
            break;
        }
        token = Some(
            page.next_continuation_token()
                .ok_or("truncated merged page without a continuation token")?
                .to_string(),
        );
    }
    assert!(completed, "the merged listing did not terminate");

    let listed_keys: Vec<String> = listed.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(listed_keys, keys, "the merged listing is the source namespace in byte order");
    for (key, size) in &listed {
        let expected = if shared.contains(key) {
            LOCAL_BODY_LEN
        } else {
            SOURCE_BODY_LEN
        };
        assert_eq!(*size, expected as i64, "{key} must be reported by the side that wins it");
    }
    case.finish().await
}

/// A backfill pulls every object under the run's source prefix. The count
/// comes from the environment: the fake source caps out around 4,096 stored
/// versions, while the MinIO lane runs the full production-shaped batch.
#[tokio::test]
async fn interop_backfill_pulls_every_source_object() -> TestResult {
    const KEY_PREFIX: &str = "cold/";
    let count = interop_backfill_objects()?;
    assert!(count > 0, "the backfill case needs at least one source object");
    let case = OdmInteropEnv::start("interop_backfill_pulls_every_source_object", "odm-interop-backfill", |_| {}).await?;

    let objects: Vec<SeedObject> = (0..count)
        .map(|index| SeedObject::new(format!("{KEY_PREFIX}{index:06}"), Bytes::from(format!("object-{index:06}"))))
        .collect();
    case.seed(&objects).await?;

    let started = case.env.start_backfill(&case.bucket, BackfillRequest::default()).await?;
    assert_eq!(started.status, 200, "start backfill: {}", started.body);

    // One pull is a HEAD plus a GET plus a local write; the ceiling scales
    // with the object count so raising it in the workflow does not need a
    // second knob here.
    let timeout = Duration::from_secs(180 + count as u64 / 5);
    let done = case
        .env
        .wait_for_backfill(&case.bucket, timeout, |job| job["state"] == "completed")
        .await?;
    for (name, expected) in [
        ("listed", count as u64),
        ("enqueued", count as u64),
        ("pulled", count as u64),
        ("failed", 0),
    ] {
        assert_eq!(
            done[name].as_u64().unwrap_or_else(|| panic!("{name} missing in {done}")),
            expected,
            "backfill {name}"
        );
    }
    assert_eq!(
        case.env.local_key_count(&case.bucket, KEY_PREFIX).await?,
        count,
        "every source object must be stored locally"
    );
    case.env
        .assert_local_present(&case.bucket, &objects[count - 1].key, &objects[count - 1].body)
        .await;
    case.finish().await
}
