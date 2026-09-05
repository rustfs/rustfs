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

//! `ListObjectsV2` list-through for on-demand migration (rustfs/backlog#2164):
//! when a bucket sets `policy.list_through`, the local listing and the source
//! listing are merged into one ordered page so clients see the whole namespace
//! while the migration runs.
//!
//! Only `ListObjectsV2` merges. `ListObjects` (v1) and `ListObjectVersions`
//! stay local: v1 has no opaque continuation token to carry two cursors, and a
//! version listing has no meaning for a source whose versions were never
//! pulled.

use super::storage_api::bucket_usecase::ECStore;
use super::storage_api::bucket_usecase::StorageObjectInfo as ObjectInfo;
use super::storage_api::bucket_usecase::StorageObjectOptions;
use super::storage_api::bucket_usecase::bucket::versioning_sys::BucketVersioningSys;
use super::storage_api::bucket_usecase::contract::list::{ListObjectsV2Info as StorageListObjectsV2Info, ListOperations as _};
use super::storage_api::bucket_usecase::contract::object::ObjectOperations as _;
use super::storage_api::bucket_usecase::s3::{S3Error, S3ErrorCode, S3Request, S3Result};
use super::storage_api::bucket_usecase::s3_api::bucket::ListObjectsV2Params;
use crate::app::object::shared::{odm_source_unavailable_error, odm_state_error_class};
use crate::error::ApiError;
use crate::on_demand_migration::{
    BucketOdmState, LIST_THROUGH_TOKEN_VERSION, ListEntryKey, ListPageError, ListThroughCursor, ListThroughMerger,
    ListThroughToken, ListThroughTokenError, MergeSide, OnDemandMigrationSys, SOURCE_LIST_MAX_RATE_WAIT, SourceClient,
    SourceError, SourceErrorPolicy, SourceListPlan, SourceListRequest, SourceObject, SourcePage, decode_continuation_token,
    source_list_plan,
};
use futures::StreamExt;
use http::HeaderMap;
use rustfs_utils::http::{SUFFIX_SOURCE_PROXY_REQUEST, get_header};
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;
use tracing::debug;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;

/// Storage class every source entry is reported with: the object is not local
/// yet, so the only class RustFS can vouch for is the default one.
const SOURCE_STORAGE_CLASS: &str = "STANDARD";

/// Enable only after every node serving continuation requests can read v2.
const ENV_LIST_PROGRESS_TOKENS: &str = "RUSTFS_ON_DEMAND_MIGRATION_LIST_V2_TOKENS";

/// Independent of the budget version: bare-v2 readers cannot read framing.
const ENV_LIST_FRAMED_TOKENS: &str = "RUSTFS_ON_DEMAND_MIGRATION_LIST_FRAMED_TOKENS";

/// Concurrent local metadata probes when a versioned bucket has to check
/// source-only keys for a shadowing delete marker.
const DELETE_MARKER_PROBE_CONCURRENCY: usize = 32;

/// Where the local side of a listing resumes.
pub(crate) enum LocalListCursor {
    /// Continuation token for the local store, `None` for the first page.
    Token(Option<String>),
    /// The local side of a merged listing was already exhausted, so a listing
    /// that no longer merges has nothing left to return.
    Exhausted,
}

/// Reads the (already base64-decoded) continuation token.
///
/// This runs whether or not the bucket merges: a token handed out while
/// `list_through` was on must keep paginating the local side after it is
/// turned off, and a tampered envelope must be rejected either way.
pub(crate) fn decode_list_cursor(decoded: Option<&str>) -> S3Result<Option<ListThroughToken>> {
    match decoded.map(decode_continuation_token).transpose() {
        Ok(Some(ListThroughCursor::Merged(token))) => Ok(Some(*token)),
        Ok(_) => Ok(None),
        Err(err) => Err(invalid_continuation_token(&err)),
    }
}

/// The local cursor to use when the request is answered locally, given the
/// decoded token.
pub(crate) fn local_cursor(decoded: Option<&str>, merged: Option<&ListThroughToken>) -> LocalListCursor {
    match merged {
        Some(token) if token.local_done => LocalListCursor::Exhausted,
        Some(token) => LocalListCursor::Token(token.local.clone()),
        None => LocalListCursor::Token(decoded.map(str::to_string)),
    }
}

/// A framed chain keeps its envelope when the bucket stops consulting source.
pub(crate) fn preserve_framed_local_cursor(info: &mut ListObjectsV2Info, previous: Option<&ListThroughToken>) {
    let Some(previous) = previous.filter(|token| token.framed) else {
        return;
    };
    let Some(next) = info.next_continuation_token.take() else {
        return;
    };
    let mut token = previous.clone();
    token.local = Some(next);
    token.local_done = false;
    if let Some(last_key) = info
        .objects
        .iter()
        .map(|object| object.name.as_str())
        .chain(info.prefixes.iter().map(String::as_str))
        .max()
    {
        token.last_key = Some(
            token
                .last_key
                .as_deref()
                .map_or(last_key, |previous| previous.max(last_key))
                .to_string(),
        );
        token.v = LIST_THROUGH_TOKEN_VERSION;
        token.no_progress = None;
    }
    info.next_continuation_token = Some(token.encode());
}

fn invalid_continuation_token(err: &ListThroughTokenError) -> S3Error {
    debug!(error = %err, "rejected an on-demand migration list continuation token");
    S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid continuation token".to_string())
}

/// The bucket's live migration state when this request must merge the source.
///
/// `None` keeps the caller on the plain local listing: the module is off, the
/// bucket has no source, `list_through` is off, or the request carries the
/// `source-proxy-request` anti-loop marker and therefore comes from a peer
/// that must be answered locally.
pub(crate) async fn list_through_state<T>(
    store: &ECStore,
    bucket: &str,
    req: &S3Request<T>,
    params: &ListObjectsV2Params,
) -> S3Result<Option<Arc<BucketOdmState>>> {
    if get_header(&req.headers, SUFFIX_SOURCE_PROXY_REQUEST).is_some() {
        return Ok(None);
    }
    let sys = OnDemandMigrationSys::get();
    if !sys.is_module_enabled() {
        return Ok(None);
    }
    let Some(state) = sys.state(bucket).filter(|state| state.config().policy.list_through) else {
        return Ok(None);
    };
    if params.max_keys == 0
        || matches!(
            source_list_plan(&params.prefix, state.config().filter.prefix.as_deref(), params.delimiter.as_deref()),
            SourceListPlan::Skip,
        )
    {
        return Ok(None);
    }
    let Some(expected_incarnation) = super::storage_api::bucket_usecase::access::odm_read_generation(req, bucket)? else {
        return Ok(None);
    };
    let incarnation = store.bucket_incarnation_id(bucket).await.map_err(ApiError::from)?;
    if incarnation != expected_incarnation {
        return Ok(None);
    }
    Ok(state.filter_incarnation(incarnation))
}

/// A merged page plus whether the source had to be left out of it.
pub(crate) struct ListThroughOutcome {
    pub(crate) info: ListObjectsV2Info,
    /// The source could not be consulted; the answer is local state only and
    /// carries `x-rustfs-on-demand-migration-list: local_only`.
    pub(crate) degraded: bool,
}

/// One entry of either side's buffer, in listing order.
enum SideEntry {
    Object(Box<ObjectInfo>),
    Prefix(String),
}

impl SideEntry {
    fn key(&self) -> ListEntryKey {
        match self {
            SideEntry::Object(info) => ListEntryKey::object(info.name.clone()),
            SideEntry::Prefix(prefix) => ListEntryKey::prefix(prefix.clone()),
        }
    }
}

/// Interleaves a listing page's objects and common prefixes into the single
/// ordered sequence S3 paginates over. Both inputs are already sorted.
fn interleave(objects: Vec<ObjectInfo>, prefixes: Vec<String>) -> Vec<SideEntry> {
    let mut merged = Vec::with_capacity(objects.len() + prefixes.len());
    let mut objects = objects.into_iter().peekable();
    let mut prefixes = prefixes.into_iter().peekable();
    loop {
        let take_object = match (objects.peek(), prefixes.peek()) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(object), Some(prefix)) => object.name.as_str() < prefix.as_str(),
        };
        if take_object {
            merged.push(SideEntry::Object(Box::new(objects.next().expect("peeked"))));
        } else {
            merged.push(SideEntry::Prefix(prefixes.next().expect("peeked")));
        }
    }
    merged
}

/// One source listing entry in the local namespace. The ETag, size and
/// last-modified are the source's own; the storage class is `STANDARD` and the
/// owner (added by the output builder) is this bucket's, because the object is
/// not local yet and RustFS can vouch for nothing else.
fn source_object_info(bucket: &str, object: SourceObject) -> ObjectInfo {
    ObjectInfo {
        bucket: bucket.to_string(),
        name: object.key,
        mod_time: object.last_modified.map(OffsetDateTime::from),
        size: i64::try_from(object.size).unwrap_or(i64::MAX),
        etag: object.etag,
        storage_class: Some(SOURCE_STORAGE_CLASS.to_string()),
        ..Default::default()
    }
}

fn source_page_entries(bucket: &str, page: SourcePage) -> Vec<SideEntry> {
    let objects = page
        .objects
        .into_iter()
        .filter(|object| !object.key.is_empty())
        .map(|object| source_object_info(bucket, object))
        .collect();
    interleave(objects, page.common_prefixes)
}

/// Runs one merged `ListObjectsV2` page.
///
/// Cost: at most two listings per side per request — the first page of each
/// side, plus one refill when the previous page had already consumed most of
/// what that side buffered. A full walk of N merged keys at `max_keys = K`
/// therefore costs ceil(N/K) requests and between ceil(N/K) and 2*ceil(N/K)
/// source listings.
pub(crate) async fn merged_list_objects_v2(
    store: &Arc<ECStore>,
    state: &Arc<BucketOdmState>,
    bucket: &str,
    params: &ListObjectsV2Params,
    fetch_owner: bool,
    incl_deleted: bool,
    token: Option<&ListThroughToken>,
) -> S3Result<ListThroughOutcome> {
    let policy = &state.config().policy;
    let max_keys = usize::try_from(params.max_keys).unwrap_or(0);
    let mut merger = ListThroughMerger::new(max_keys, token);
    let mut buffers: [Vec<Option<SideEntry>>; 2] = [Vec::new(), Vec::new()];
    let mut degraded = false;

    let plan = source_list_plan(&params.prefix, state.config().filter.prefix.as_deref(), params.delimiter.as_deref());
    let mut client: Option<Arc<SourceClient>> = None;
    match &plan {
        // Nothing the source holds can appear under this prefix; that is a
        // filter decision, not a degradation.
        SourceListPlan::Skip => merger.disable_source(),
        _ => match state.client() {
            Ok(ready) if state.breaker().allow_request() => client = Some(Arc::clone(ready)),
            Ok(_) => degrade_or_fail(&mut merger, &mut degraded, policy.source_error, "breaker_open")?,
            Err(error) => degrade_or_fail(&mut merger, &mut degraded, policy.source_error, odm_state_error_class(error))?,
        },
    }

    while let Some(fetch) = merger.next_fetch() {
        let page = match fetch.side {
            MergeSide::Local => {
                let info = Arc::clone(store)
                    .list_objects_v2(
                        bucket,
                        &params.prefix,
                        fetch.token.clone(),
                        params.delimiter.clone(),
                        params.max_keys,
                        fetch_owner,
                        params.start_after_for_query.clone(),
                        incl_deleted,
                    )
                    .await
                    .map_err(ApiError::from)?;
                let objects = info.objects.into_iter().filter(|object| !object.name.is_empty()).collect();
                (interleave(objects, info.prefixes), info.is_truncated, info.next_continuation_token)
            }
            MergeSide::Source => {
                let client = client.as_ref().expect("the source side is disabled without a client");
                match fetch_source_page(state, client, bucket, params, &plan, fetch.token.as_deref()).await {
                    Ok(page) => page,
                    Err(class) => {
                        degrade_or_fail(&mut merger, &mut degraded, policy.source_error, class)?;
                        continue;
                    }
                }
            }
        };
        let (entries, is_truncated, next_token) = page;
        let kept: Vec<SideEntry> = entries
            .into_iter()
            .filter(|entry| merger.accepts(&entry.key().name))
            .collect();
        let keys: Vec<ListEntryKey> = kept.iter().map(SideEntry::key).collect();
        if let Err(error) = merger.push_page(fetch.side, keys, is_truncated, next_token) {
            match fetch.side {
                MergeSide::Source => {
                    degrade_or_fail(&mut merger, &mut degraded, policy.source_error, "invalid_pagination")?;
                    continue;
                }
                MergeSide::Local => return Err(S3Error::with_message(S3ErrorCode::InternalError, error.to_string())),
            }
        }
        buffers[usize::from(fetch.side == MergeSide::Source)].extend(kept.into_iter().map(Some));
    }

    let issue_progress_tokens = rustfs_utils::get_env_bool(ENV_LIST_PROGRESS_TOKENS, false);
    let framed = token.is_some_and(|token| token.framed) || rustfs_utils::get_env_bool(ENV_LIST_FRAMED_TOKENS, false);
    let outcome = match merger.finish(issue_progress_tokens) {
        Ok(outcome) => outcome,
        Err(ListPageError::NoProgress(MergeSide::Source)) => {
            degrade_or_fail(&mut merger, &mut degraded, policy.source_error, "invalid_pagination")?;
            merger
                .finish(issue_progress_tokens)
                .map_err(|error| S3Error::with_message(S3ErrorCode::InternalError, error.to_string()))?
        }
        Err(error) => return Err(S3Error::with_message(S3ErrorCode::InternalError, error.to_string())),
    };
    drop(merger);
    let mut objects = Vec::with_capacity(outcome.picks.len());
    let mut prefixes = Vec::new();
    let mut source_only_keys = Vec::new();
    for pick in &outcome.picks {
        let buffer = &mut buffers[usize::from(pick.side == MergeSide::Source)];
        // Every pick names a distinct buffered entry, so taking it is safe.
        match buffer[pick.index].take().expect("a merged pick names a buffered entry") {
            SideEntry::Object(info) => {
                if pick.side == MergeSide::Source {
                    source_only_keys.push(info.name.clone());
                }
                objects.push(*info);
            }
            SideEntry::Prefix(prefix) => prefixes.push(prefix),
        }
    }

    if !source_only_keys.is_empty() && policy.respect_local_delete_marker && bucket_keeps_delete_markers(bucket).await {
        let shadowed = local_delete_markers(store, bucket, &source_only_keys).await;
        objects.retain(|object| !shadowed.contains(&object.name));
    }

    Ok(ListThroughOutcome {
        info: ListObjectsV2Info {
            is_truncated: outcome.is_truncated,
            continuation_token: None,
            next_continuation_token: outcome.next_token.map(|mut token| {
                token.framed = framed;
                token.encode()
            }),
            objects,
            prefixes,
        },
        degraded,
    })
}

/// Applies `policy.source_error` to a source failure: `propagate` fails the
/// listing with 424, `not_found` answers from local state alone.
fn degrade_or_fail(
    merger: &mut ListThroughMerger,
    degraded: &mut bool,
    policy: SourceErrorPolicy,
    class: &'static str,
) -> S3Result<()> {
    match policy {
        SourceErrorPolicy::Propagate => Err(odm_source_unavailable_error(class)),
        SourceErrorPolicy::NotFound => {
            merger.disable_source();
            *degraded = true;
            Ok(())
        }
    }
}

type SourceFetch = (Vec<SideEntry>, bool, Option<String>);

/// One source listing, rate-limited per bucket. The error is the class label
/// for the caller's `source_error` decision; the source's own message is never
/// surfaced.
async fn fetch_source_page(
    state: &Arc<BucketOdmState>,
    client: &SourceClient,
    bucket: &str,
    params: &ListObjectsV2Params,
    plan: &SourceListPlan,
    token: Option<&str>,
) -> Result<SourceFetch, &'static str> {
    let Some(wait) = state.list_rate_limiter().reserve(SOURCE_LIST_MAX_RATE_WAIT) else {
        return Err("rate_limited");
    };
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }

    let request = match plan {
        SourceListPlan::Skip => return Ok((Vec::new(), false, None)),
        SourceListPlan::Page { prefix } => SourceListRequest {
            prefix: Some(prefix.as_str()),
            delimiter: params.delimiter.as_deref(),
            // S3 ignores start-after once a continuation token is present, so
            // the client's own start-after only applies to the first page.
            start_after: token.is_none().then_some(params.start_after_for_query.as_deref()).flatten(),
            continuation_token: token,
            max_keys: params.max_keys,
        },
        // Everything under `filter.prefix` rolls into one common prefix. An
        // empty truncated probe must still follow its cursor before declaring
        // that prefix absent.
        SourceListPlan::Folded { probe_prefix, .. } => SourceListRequest {
            prefix: Some(probe_prefix.as_str()),
            continuation_token: token,
            max_keys: 1,
            ..Default::default()
        },
    };

    let started = Instant::now();
    let result = client.list_page(&request).await;
    // A 404 on a listing is the bucket, not a key: keep it out of the negative
    // cache but let the breaker see everything else (same rule as the backfill).
    match &result {
        Err(SourceError::NotFound) => {}
        other => state.observe_source(started.elapsed(), "", other.as_ref().err()),
    }
    let page = result.map_err(|err| err.class_label())?;

    match plan {
        SourceListPlan::Folded { common_prefix, .. } => {
            let exists = !page.objects.is_empty() || !page.common_prefixes.is_empty();
            Ok((
                if exists {
                    vec![SideEntry::Prefix(common_prefix.clone())]
                } else {
                    Vec::new()
                },
                !exists && page.is_truncated,
                if exists { None } else { page.next_continuation_token },
            ))
        }
        _ => {
            let is_truncated = page.is_truncated;
            let next = page.next_continuation_token.clone();
            Ok((source_page_entries(bucket, page), is_truncated, next))
        }
    }
}

/// Whether a delete on this bucket leaves a marker behind. Only such a bucket
/// can shadow a source key the way the read path does.
async fn bucket_keeps_delete_markers(bucket: &str) -> bool {
    BucketVersioningSys::enabled(bucket).await || BucketVersioningSys::suspended(bucket).await
}

/// The subset of `keys` whose latest local version is a delete marker.
///
/// The local `ListObjectsV2` never returns delete markers, so a versioned
/// bucket has to probe the source-only keys of the page: at most `max_keys`
/// metadata reads, run with bounded concurrency and without the namespace lock
/// — a listing is not a linearizable read, and the alternative is one lock per
/// key per page. A lookup that fails for any other reason leaves the key
/// visible rather than hiding data on a transient error.
async fn local_delete_markers(store: &Arc<ECStore>, bucket: &str, keys: &[String]) -> std::collections::HashSet<String> {
    futures::stream::iter(keys.iter().cloned())
        .map(|key| {
            let store = Arc::clone(store);
            let bucket = bucket.to_string();
            async move {
                let options = StorageObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };
                let info = store.get_object_info(&bucket, &key, &options).await;
                matches!(info, Ok(info) if info.delete_marker).then_some(key)
            }
        })
        .buffer_unordered(DELETE_MARKER_PROBE_CONCURRENCY)
        .filter_map(|shadowed| async move { shadowed })
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::DefaultBucketUsecase;
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ecstore};
    use crate::app::storage_api::bucket_usecase::s3::{
        GetObjectInput, HeadObjectInput, ListObjectsInput, ListObjectsV2Input, ListObjectsV2Output, S3Request, S3Response,
        XmlSerialize, XmlSerializer,
    };
    use crate::app::storage_api::test::StoragePutObjReader;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
    use crate::app::storage_api::test::contract::object::ObjectIO as _;
    use crate::on_demand_migration::{
        FilterConfig, MAX_LIST_NO_PROGRESS_PAGES, OnDemandMigrationConfig, PathStyle, PolicyConfig, Provider, SourceConfig,
        SourceCredentials, TlsConfig,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn token(local: Option<&str>, local_done: bool) -> ListThroughToken {
        ListThroughToken {
            framed: false,
            t: "odm-list".to_string(),
            v: 1,
            local: local.map(str::to_string),
            local_done,
            source: Some("source-2".to_string()),
            source_done: false,
            last_key: Some("k".to_string()),
            no_progress: None,
        }
    }

    fn info(name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn names(entries: &[SideEntry]) -> Vec<String> {
        entries.iter().map(|entry| entry.key().name).collect()
    }

    #[test]
    fn interleave_orders_objects_and_prefixes_as_one_sequence() {
        let entries = interleave(vec![info("a"), info("b0"), info("c")], vec!["a/".to_string(), "b/".to_string()]);
        assert_eq!(names(&entries), vec!["a", "a/", "b/", "b0", "c"]);
        assert!(matches!(entries[1], SideEntry::Prefix(_)));
    }

    #[test]
    fn source_entries_report_the_source_facts_with_the_default_storage_class() {
        let page = SourcePage {
            objects: vec![SourceObject {
                key: "photos/a.jpg".to_string(),
                etag: Some("abc".to_string()),
                size: 7,
                last_modified: Some(std::time::SystemTime::UNIX_EPOCH),
                storage_class: Some("GLACIER".to_string()),
                is_multipart_etag: false,
            }],
            common_prefixes: vec!["photos/2024/".to_string()],
            is_truncated: false,
            next_continuation_token: None,
        };
        let entries = source_page_entries("photos", page);
        assert_eq!(names(&entries), vec!["photos/2024/", "photos/a.jpg"]);
        let SideEntry::Object(object) = &entries[1] else {
            panic!("expected an object entry");
        };
        assert_eq!(object.etag.as_deref(), Some("abc"));
        assert_eq!(object.size, 7);
        assert_eq!(object.mod_time, Some(OffsetDateTime::UNIX_EPOCH));
        assert_eq!(
            object.storage_class.as_deref(),
            Some(SOURCE_STORAGE_CLASS),
            "a source storage class is never echoed"
        );
    }

    #[test]
    fn a_merged_token_survives_list_through_being_turned_off() {
        let resume = token(Some("local-2"), false);
        let encoded = resume.encode();
        let decoded = decode_list_cursor(Some(&encoded)).expect("a valid envelope decodes");
        assert_eq!(decoded.as_ref(), Some(&resume));
        assert!(matches!(
            local_cursor(Some(&encoded), decoded.as_ref()),
            LocalListCursor::Token(Some(local)) if local == "local-2"
        ));

        let encoded = token(None, true).encode();
        let decoded = decode_list_cursor(Some(&encoded)).expect("a valid envelope decodes");
        assert!(matches!(local_cursor(Some(&encoded), decoded.as_ref()), LocalListCursor::Exhausted));
    }

    #[test]
    fn a_v2_token_keeps_the_local_cursor_when_list_through_is_turned_off() {
        for framed in [false, true] {
            let mut resume = token(Some("local-2"), false);
            resume.framed = framed;
            resume.v = 2;
            resume.no_progress = Some(MAX_LIST_NO_PROGRESS_PAGES - 1);
            let encoded = resume.encode();
            let decoded = decode_list_cursor(Some(&encoded)).expect("a v2 envelope decodes");
            assert_eq!(decoded.as_ref(), Some(&resume));
            assert!(matches!(
                local_cursor(Some(&encoded), decoded.as_ref()),
                LocalListCursor::Token(Some(local)) if local == "local-2"
            ));
            resume.local_done = true;
            let encoded = resume.encode();
            let decoded = decode_list_cursor(Some(&encoded)).expect("v2 with local EOF decodes");
            assert!(matches!(local_cursor(Some(&encoded), decoded.as_ref()), LocalListCursor::Exhausted));
        }
    }

    #[test]
    fn a_plain_local_token_is_passed_through_and_a_tampered_one_is_rejected() {
        let json_key = r#"{"t":"odm-list","v":1,"local_done":true}"#;
        assert!(decode_list_cursor(Some(json_key)).expect("valid local key").is_none());
        assert!(matches!(local_cursor(Some(json_key), None), LocalListCursor::Token(Some(local)) if local == json_key));
        assert!(
            decode_list_cursor(Some("photos/a.jpg"))
                .expect("plain markers decode")
                .is_none()
        );
        assert!(matches!(
            local_cursor(Some("photos/a.jpg"), None),
            LocalListCursor::Token(Some(local)) if local == "photos/a.jpg"
        ));

        let tampered = token(Some("local-2"), false).encode().replace("\"v\":1", "\"v\":9");
        let err = decode_list_cursor(Some(&tampered)).expect_err("a bumped version is rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn degrade_or_fail_follows_the_source_error_policy() {
        let mut merger = ListThroughMerger::new(10, None);
        let mut degraded = false;
        assert!(
            degrade_or_fail(&mut merger, &mut degraded, SourceErrorPolicy::Propagate, "server_error").is_err(),
            "propagate must surface the failure"
        );
        assert!(!degraded);

        degrade_or_fail(&mut merger, &mut degraded, SourceErrorPolicy::NotFound, "server_error")
            .expect("not_found degrades instead of failing");
        assert!(degraded);
        assert_eq!(merger.next_fetch().map(|fetch| fetch.side), Some(MergeSide::Local));
    }

    /// Serves exactly the scripted S3 pages and joins every connection before
    /// returning. A source retry or unexpected operation fails the test.
    async fn scripted_list_source(pages: Vec<String>) -> (String, tokio_util::task::AbortOnDropHandle<Vec<String>>) {
        let (endpoint, server, _) = list_source(pages.into_iter()).await;
        (endpoint, server)
    }

    async fn list_source(
        pages: impl Iterator<Item = String> + Send + 'static,
    ) -> (
        String,
        tokio_util::task::AbortOnDropHandle<Vec<String>>,
        tokio_util::sync::CancellationToken,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listing source");
        let address = listener.local_addr().expect("listing source address");
        let stop = tokio_util::sync::CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in pages {
                let (mut stream, _) = tokio::select! {
                    _ = server_stop.cancelled() => break,
                    accepted = listener.accept() => accepted.expect("accept source listing"),
                };
                let mut request = Vec::new();
                let mut chunk = [0; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).await.expect("read signed listing request");
                    assert!(count > 0, "source request must include complete headers");
                    request.extend_from_slice(&chunk[..count]);
                    assert!(request.len() <= 32 * 1024, "listing request headers must be bounded");
                }
                let first_line = String::from_utf8_lossy(&request)
                    .lines()
                    .next()
                    .expect("request line")
                    .to_string();
                // The SDK joins the bucket endpoint with the LIST operation's `/` path.
                assert!(
                    first_line.starts_with("GET /source-bucket/?"),
                    "expected a path-style bucket-root LIST request, got {first_line:?}"
                );
                assert!(first_line.contains("list-type=2"), "expected a ListObjectsV2 query, got {first_line:?}");
                requests.push(first_line);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.expect("write source page");
                stream.shutdown().await.expect("finish source response");
            }
            requests
        });
        (format!("http://{address}"), tokio_util::task::AbortOnDropHandle::new(server), stop)
    }

    fn source_xml(next: Option<&str>, truncated: bool, key: Option<&str>) -> String {
        let next = next
            .map(|token| format!("<NextContinuationToken>{token}</NextContinuationToken>"))
            .unwrap_or_default();
        let contents = key
            .map(|key| format!("<Contents><Key>{key}</Key><Size>1</Size></Contents>"))
            .unwrap_or_default();
        format!(
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>"
        )
    }

    struct ListThroughTestState {
        bucket: String,
        module_enabled: bool,
    }

    impl Drop for ListThroughTestState {
        fn drop(&mut self) {
            let sys = OnDemandMigrationSys::get();
            sys.remove(&self.bucket);
            sys.set_module_enabled(self.module_enabled);
        }
    }

    async fn source_policy_input(
        endpoint: String,
        policy: SourceErrorPolicy,
        resume_source: Option<&str>,
        filter_prefix: Option<&str>,
    ) -> (ListThroughTestState, ListObjectsV2Input) {
        let store = shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("odm-list-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create list-through bucket");
        store
            .put_object(
                &bucket,
                "z-local",
                &mut StoragePutObjReader::from_vec(vec![1]),
                &StorageObjectOptions::default(),
            )
            .await
            .expect("seed real local listing");
        let sys = OnDemandMigrationSys::get();
        let state_guard = ListThroughTestState {
            bucket: bucket.clone(),
            module_enabled: sys.is_module_enabled(),
        };
        sys.set_module_enabled(true);
        let config = OnDemandMigrationConfig {
            version: 1,
            enabled: true,
            source: SourceConfig {
                provider: Provider::Minio,
                endpoint: Some(endpoint),
                region: "us-east-1".into(),
                bucket: "source-bucket".into(),
                path_style: PathStyle::Path,
                credentials: Some(SourceCredentials {
                    access_key: "test-access".into(),
                    secret_key: "test-secret".into(),
                    session_token: None,
                }),
                tls: TlsConfig::default(),
                azure: None,
                gcs: None,
            },
            filter: FilterConfig {
                prefix: filter_prefix.map(str::to_string),
                ..Default::default()
            },
            policy: PolicyConfig {
                list_through: true,
                source_error: policy,
                ..Default::default()
            },
        };
        sys.apply_for_incarnation(
            &bucket,
            store.bucket_incarnation_id(&bucket).await.expect("bucket identity"),
            Some(&config),
        )
        .await;
        assert!(
            sys.state(&bucket).expect("ODM state installed").client().is_ok(),
            "fake source client must build"
        );
        let continuation_token = resume_source.map(|source| {
            let token = ListThroughToken {
                framed: false,
                t: "odm-list".into(),
                v: 1,
                local: None,
                local_done: false,
                source: Some(source.into()),
                source_done: false,
                last_key: None,
                no_progress: None,
            };
            base64_simd::STANDARD.encode_to_string(token.encode().as_bytes())
        });
        let input = ListObjectsV2Input {
            bucket,
            max_keys: Some(2),
            continuation_token,
            delimiter: filter_prefix.map(|_| "/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        };
        (state_guard, input)
    }

    async fn execute_source_list(input: ListObjectsV2Input) -> S3Result<S3Response<ListObjectsV2Output>> {
        let request = S3Request {
            input,
            method: http::Method::GET,
            uri: http::Uri::from_static("/?list-type=2"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        tokio::time::timeout(
            Duration::from_secs(10),
            DefaultBucketUsecase::from_global().execute_list_objects_v2(request),
        )
        .await
        .expect("listing must complete within its bounded source budget")
    }

    async fn native_list_source(
        provider: Provider,
        pages: Vec<(String, String)>,
    ) -> (
        String,
        tokio_util::task::AbortOnDropHandle<Vec<String>>,
        tokio_util::sync::CancellationToken,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind native listing source");
        let address = listener.local_addr().expect("native source address");
        let stop = tokio_util::sync::CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let mut pages = pages.into_iter();
            let mut requests = Vec::new();
            loop {
                let (mut stream, _) = tokio::select! {
                    _ = server_stop.cancelled() => break,
                    accepted = listener.accept() => accepted.expect("accept native source request"),
                };
                let (target, body) = pages.next().expect("native source must not receive an extra request");
                let mut request = Vec::new();
                let mut chunk = [0; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).await.expect("read native source request");
                    assert!(count > 0, "native request needs complete headers");
                    request.extend_from_slice(&chunk[..count]);
                    assert!(request.len() <= 32 * 1024, "native request headers must be bounded");
                }
                let text = String::from_utf8(request).expect("native HTTP request text");
                let first_line = text.lines().next().expect("native request line");
                assert_eq!(first_line, format!("GET {target} HTTP/1.1"));
                let authorization = text
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                    .map(|(_, value)| value.trim())
                    .expect("native credential must be used");
                assert!(authorization.starts_with(if provider == Provider::Azure {
                    "SharedKey acct:"
                } else {
                    "Bearer "
                }));
                requests.push(first_line.to_string());
                let response = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                stream.write_all(response.as_bytes()).await.expect("write native source page");
                stream.shutdown().await.expect("finish native source response");
            }
            assert!(pages.next().is_none(), "every scripted native page must have been requested");
            requests
        });
        (format!("http://{address}"), tokio_util::task::AbortOnDropHandle::new(server), stop)
    }

    fn native_list_target(provider: Provider, cursor: bool, max_keys: i32) -> String {
        if provider == Provider::Azure {
            format!(
                "/source-bucket?restype=container&comp=list{}&maxresults={max_keys}",
                if cursor { "&marker=opaque%2B%2F%3D" } else { "" }
            )
        } else {
            format!(
                "/storage/v1/b/source-bucket/o?{}maxResults={max_keys}",
                if cursor { "pageToken=opaque%2B%2F%3D&" } else { "" }
            )
        }
    }

    fn native_list_body(provider: Provider, entries: &str, prefixes: bool, next: bool) -> String {
        if provider == Provider::Azure {
            format!(
                "<EnumerationResults><Blobs>{entries}{}</Blobs><NextMarker>{}</NextMarker></EnumerationResults>",
                if prefixes {
                    "<BlobPrefix><Name>目录/子/</Name></BlobPrefix>"
                } else {
                    ""
                },
                if next { "opaque+/=" } else { "" }
            )
        } else {
            format!(
                r#"{{"items":[{entries}],"prefixes":{},"nextPageToken":{}}}"#,
                if prefixes { r#"["目录/子/"]"# } else { "[]" },
                if next { r#""opaque+/=""# } else { "null" }
            )
        }
    }

    #[cfg(feature = "gcs")]
    fn native_test_service_account() -> String {
        // The real Google credentials implementation signs locally. Generate a
        // disposable key instead of storing private key material in the fixture.
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).expect("generate fixture service-account key");
        serde_json::json!({
            "type": "service_account",
            "client_email": "fixture@example.invalid",
            "private_key_id": "fixture-key",
            "private_key": key.serialize_pem(),
            "project_id": "fixture-project"
        })
        .to_string()
    }

    async fn native_source_policy_request(
        provider: Provider,
        policy: SourceErrorPolicy,
        pages: Vec<(String, String)>,
        service_account: &str,
        max_keys: i32,
    ) -> (S3Result<S3Response<ListObjectsV2Output>>, Vec<String>) {
        let (endpoint, server, stop) = native_list_source(provider, pages).await;
        let (_state_guard, mut input) = source_policy_input(endpoint.clone(), policy, None, None).await;
        input.max_keys = Some(max_keys);
        let sys = OnDemandMigrationSys::get();
        let installed = sys.state(&input.bucket).expect("installed source state");
        let mut config = installed.config().clone();
        config.source = serde_json::from_value(serde_json::json!({
            "provider": provider,
            "endpoint": endpoint,
            "region": "us-east-1",
            "bucket": "source-bucket",
            "azure": if provider == Provider::Azure { serde_json::json!({ "account": "acct", "account_key": "c2VjcmV0LWtleQ==" }) } else { serde_json::Value::Null },
            "gcs": if provider == Provider::GcsNative { serde_json::json!({ "service_account_json": service_account }) } else { serde_json::Value::Null }
        })).expect("native source configuration");
        sys.apply_for_incarnation(&input.bucket, installed.incarnation_id(), Some(&config))
            .await;
        let state = sys.state(&input.bucket).expect("native source state");
        state
            .client()
            .unwrap_or_else(|error| panic!("{provider:?} native client must build: {error:?}"));
        let result = execute_source_list(input).await;
        stop.cancel();
        let requests = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("native source server must finish")
            .expect("native source requests must match the script");
        (result, requests)
    }

    #[test]
    #[serial_test::serial]
    fn native_list_through_malformed_fields_follow_both_source_policies() {
        run_large_stack_test("native-list-through-fields", || async {
            temp_env::async_with_vars(
                [("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")), ("HTTP_PROXY", None), ("HTTPS_PROXY", None),
                 ("ALL_PROXY", None), ("http_proxy", None), ("https_proxy", None), ("all_proxy", None),
                 ("NO_PROXY", Some("*")), ("no_proxy", Some("*"))],
                async {
                    #[cfg(feature = "gcs")]
                    let service_account = native_test_service_account();
                    #[cfg(not(feature = "gcs"))]
                    let service_account = String::new();
                    for provider in [Provider::Azure, #[cfg(feature = "gcs")] Provider::GcsNative] {
                        let invalid = if provider == Provider::Azure {
                            ["<Blob><Name>bad</Name></Blob>",
                             "<Blob><Name>bad</Name><Properties><Content-Length>-1</Content-Length></Properties></Blob>",
                             "<Blob><Name>bad</Name><Properties><Content-Length>18446744073709551616</Content-Length></Properties></Blob>",
                             "<Blob><Properties><Content-Length>1</Content-Length></Properties></Blob>"]
                        } else {
                            [r#"{"name":"bad"}"#, r#"{"name":"bad","size":"-1"}"#,
                             r#"{"name":"bad","size":"18446744073709551616"}"#, r#"{"size":"1"}"#]
                        };
                        let valid = if provider == Provider::Azure {
                            "<Blob><Name>a-source</Name><Properties><Content-Length>1</Content-Length></Properties></Blob>"
                        } else { r#"{"name":"a-source","size":"1"}"# };
                        for policy in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound] {
                            for entry in invalid {
                                for refill in [false, true] {
                                    let mut pages = Vec::new();
                                    if refill {
                                        pages.push((native_list_target(provider, false, 2), native_list_body(provider, valid, false, true)));
                                    }
                                    let entries = if refill { entry.to_string() } else if provider == Provider::Azure {
                                        format!("{valid}{entry}")
                                    } else { format!("{valid},{entry}") };
                                    pages.push((native_list_target(provider, refill, 2), native_list_body(provider, &entries, false, false)));
                                    let (result, requests) = native_source_policy_request(provider, policy, pages, &service_account, 2).await;
                                    assert_eq!(requests.len(), if refill { 2 } else { 1 }, "{provider:?} {policy:?} {entry}");
                                    if policy == SourceErrorPolicy::Propagate {
                                        let err = result.expect_err("malformed native page must propagate");
                                        assert_eq!(err.status_code(), Some(http::StatusCode::FAILED_DEPENDENCY));
                                        assert_eq!(err.code(), &S3ErrorCode::Custom("SourceUnavailable".into()));
                                        assert_eq!(err.message(), Some("other"));
                                    } else {
                                        // Reuse the complete local-only assertions, including no
                                        // leaked source objects, no cursor and the degraded header.
                                        assert_source_policy_result(result, policy);
                                    }
                                }
                            }
                        }
                    }
                },
            ).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn native_list_through_preserves_valid_empty_pages_and_zero_size_objects() {
        run_large_stack_test("native-list-through-valid", || async {
            temp_env::async_with_vars(
                [
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    #[cfg(feature = "gcs")]
                    let service_account = native_test_service_account();
                    #[cfg(not(feature = "gcs"))]
                    let service_account = String::new();
                    for provider in [
                        Provider::Azure,
                        #[cfg(feature = "gcs")]
                        Provider::GcsNative,
                    ] {
                        let valid = if provider == Provider::Azure {
                            "<Blob><Name>目录/空</Name><Properties><Content-Length>0</Content-Length></Properties></Blob>"
                        } else {
                            r#"{"name":"目录/空","size":"0"}"#
                        };
                        for policy in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound] {
                            let (result, requests) = native_source_policy_request(
                                provider,
                                policy,
                                vec![
                                    (native_list_target(provider, false, 3), native_list_body(provider, "", false, true)),
                                    (native_list_target(provider, true, 3), native_list_body(provider, valid, true, false)),
                                ],
                                &service_account,
                                3,
                            )
                            .await;
                            assert_eq!(requests.len(), 2);
                            let response = result.expect("valid native listing must succeed under either policy");
                            assert_ne!(
                                response
                                    .headers
                                    .get("x-rustfs-on-demand-migration-list")
                                    .and_then(|value| value.to_str().ok()),
                                Some("local_only")
                            );
                            let output = response.output;
                            let objects = output.contents.expect("local and source objects");
                            assert_eq!(
                                objects
                                    .iter()
                                    .map(|object| (object.key.as_deref(), object.size))
                                    .collect::<Vec<_>>(),
                                vec![(Some("z-local"), Some(1)), (Some("目录/空"), Some(0))]
                            );
                            assert_eq!(
                                output
                                    .common_prefixes
                                    .expect("native prefix")
                                    .into_iter()
                                    .map(|prefix| prefix.prefix)
                                    .collect::<Vec<_>>(),
                                vec![Some("目录/子/".to_string())]
                            );
                            assert_eq!(output.key_count, Some(3));
                            assert_eq!(output.is_truncated, Some(false));
                            assert!(output.next_continuation_token.is_none());
                        }
                    }
                },
            )
            .await;
        });
    }

    async fn source_policy_request(
        pages: Vec<String>,
        policy: SourceErrorPolicy,
        resume_source: Option<&str>,
        filter_prefix: Option<&str>,
    ) -> (S3Result<S3Response<ListObjectsV2Output>>, Vec<String>) {
        let (endpoint, server) = scripted_list_source(pages).await;
        let (_state_guard, input) = source_policy_input(endpoint, policy, resume_source, filter_prefix).await;
        let result = execute_source_list(input).await;
        let requests = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("source connections must finish")
            .expect("source server must not panic");
        (result, requests)
    }

    #[test]
    #[serial_test::serial]
    fn stale_bucket_state_cannot_send_get_head_or_list_to_the_source() {
        run_large_stack_test("list-through-incarnation", || async {
            temp_env::async_with_vars(
                [
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    let (endpoint, server, stop) = list_source(std::iter::repeat(source_xml(None, false, Some("source")))).await;
                    let (_guard, input) = source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, None).await;
                    let sys = OnDemandMigrationSys::get();
                    let old = sys.state(&input.bucket).expect("original source state");
                    let store = shared_gating_ecstore().await;
                    let get = S3Request {
                        input: GetObjectInput {
                            bucket: input.bucket.clone(),
                            key: "missing".into(),
                            ..Default::default()
                        },
                        method: http::Method::GET,
                        uri: http::Uri::from_static("/missing"),
                        headers: HeaderMap::new(),
                        extensions: http::Extensions::new(),
                        credentials: None,
                        region: None,
                        service: None,
                        trailing_headers: None,
                    };
                    let authorized_generation =
                        super::super::storage_api::bucket_usecase::access::load_bucket_generation_from_store(
                            &store,
                            &get,
                            &input.bucket,
                        )
                        .await
                        .expect("capture the identity before authorization");
                    store
                        .delete_bucket(
                            &input.bucket,
                            &DeleteBucketOptions {
                                force: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("delete original bucket");
                    store
                        .make_bucket(&input.bucket, &MakeBucketOptions::default())
                        .await
                        .expect("recreate bucket");
                    let replacement = store
                        .bucket_incarnation_id(&input.bucket)
                        .await
                        .expect("replacement identity");
                    assert_ne!(replacement, old.incarnation_id());
                    sys.remove(&input.bucket);
                    let mut without_source = get.clone();
                    super::super::storage_api::bucket_usecase::access::prepare_odm_read_generation(
                        &store,
                        &mut without_source,
                        &input.bucket,
                    )
                    .await;
                    for state_incarnation in [old.incarnation_id(), replacement] {
                        sys.apply_for_incarnation(&input.bucket, state_incarnation, Some(old.config()))
                            .await;
                        // Both a stale runtime and a newly published replacement must reject
                        // requests already authorized for the deleted incarnation.
                        for capture in 0..3 {
                            if capture == 0 && state_incarnation == replacement {
                                continue;
                            }
                            let mut get = if capture == 2 { without_source.clone() } else { get.clone() };
                            if capture == 1 {
                                get.extensions.insert(authorized_generation.clone());
                            }
                            let mut head = get.clone().map_input(|_| HeadObjectInput {
                                bucket: input.bucket.clone(),
                                key: "missing".into(),
                                ..Default::default()
                            });
                            head.method = http::Method::HEAD;
                            let mut list = get.clone().map_input(|_| input.clone());
                            list.uri = http::Uri::from_static("/?list-type=2");
                            let usecase = crate::app::object::DefaultObjectUsecase::from_global();
                            let get_error = tokio::time::timeout(Duration::from_secs(10), usecase.execute_get_object(get))
                                .await
                                .expect("GET stays local")
                                .expect_err("local object is absent");
                            assert_eq!(*get_error.code(), S3ErrorCode::NoSuchKey);
                            let head_error = tokio::time::timeout(Duration::from_secs(10), usecase.execute_head_object(head))
                                .await
                                .expect("HEAD stays local")
                                .expect_err("local object is absent");
                            assert_eq!(*head_error.code(), S3ErrorCode::NoSuchKey);
                            let listing = tokio::time::timeout(
                                Duration::from_secs(10),
                                DefaultBucketUsecase::from_global().execute_list_objects_v2(list),
                            )
                            .await
                            .expect("LIST stays local")
                            .expect("replacement bucket lists locally");
                            assert_eq!(listing.output.key_count, Some(0));
                        }
                    }
                    stop.cancel();
                    assert!(server.await.expect("source server must remain unused").is_empty());
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_v1_stays_local_with_xml_safe_key_markers() {
        run_large_stack_test("list-through-v1-local-markers", || async {
            temp_env::async_with_vars(
                [
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    let (endpoint, server, stop) =
                        list_source(std::iter::repeat(source_xml(None, false, Some("a-source")))).await;
                    let (_state_guard, source_input) =
                        source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, None).await;
                    let store = shared_gating_ecstore().await;
                    store
                        .put_object(
                            &source_input.bucket,
                            "a&local",
                            &mut StoragePutObjReader::from_vec(vec![1]),
                            &StorageObjectOptions::default(),
                        )
                        .await
                        .expect("seed a second local object");

                    for delimiter in [None, Some("/".to_string())] {
                        let mut input = ListObjectsInput {
                            bucket: source_input.bucket.clone(),
                            max_keys: Some(1),
                            delimiter,
                            ..Default::default()
                        };
                        for (index, expected_key) in ["a&local", "z-local"].into_iter().enumerate() {
                            let request_marker = input.marker.clone().unwrap_or_default();
                            let response = tokio::time::timeout(
                                Duration::from_secs(10),
                                DefaultBucketUsecase::from_global().execute_list_objects(S3Request {
                                    input: input.clone(),
                                    method: http::Method::GET,
                                    uri: http::Uri::from_static("/"),
                                    headers: HeaderMap::new(),
                                    extensions: http::Extensions::new(),
                                    credentials: None,
                                    region: None,
                                    service: None,
                                    trailing_headers: None,
                                }),
                            )
                            .await
                            .expect("v1 pagination must finish")
                            .expect("list-through must not change v1 listing");
                            assert!(!response.headers.contains_key("x-rustfs-on-demand-migration-list"));
                            let output = response.output;
                            let contents = output.contents.as_ref().expect("local page contents");
                            assert_eq!(contents.len(), 1);
                            assert_eq!(contents[0].key.as_deref(), Some(expected_key));
                            assert_eq!(output.marker.as_deref(), Some(request_marker.as_str()));
                            assert_eq!(output.is_truncated, Some(index == 0));
                            assert_eq!(output.next_marker.as_deref(), (index == 0).then_some(expected_key));

                            let mut xml = Vec::new();
                            XmlSerialize::serialize(&output, &mut XmlSerializer::new(&mut xml))
                                .expect("serialize the real v1 response");
                            assert!(!xml.contains(&0), "XML 1.0 forbids NUL in NextMarker");
                            let mut reader = quick_xml::Reader::from_reader(xml.as_slice());
                            loop {
                                if reader.read_event().expect("v1 response must be well-formed XML")
                                    == quick_xml::events::Event::Eof
                                {
                                    break;
                                }
                            }
                            input.marker = output.next_marker;
                        }
                    }
                    stop.cancel();
                    let requests = server.await.expect("source server must not panic");
                    assert!(requests.is_empty(), "ListObjects v1 must issue no remote LIST requests: {requests:?}");
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_invalid_source_pagination_obeys_policy_on_the_handler_path() {
        run_large_stack_test("list-through-source-policy", || async {
            temp_env::async_with_vars(
                [
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    for policy in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound] {
                        for next in [None, Some(""), Some("stuck")] {
                            for key in [None, Some("a-source")] {
                                let (result, requests) =
                                    source_policy_request(vec![source_xml(next, true, key)], policy, Some("stuck"), None).await;
                                assert_eq!(requests.len(), 1, "a malformed source page must not be retried");
                                assert!(requests[0].contains("continuation-token=stuck"));
                                assert_source_policy_result(result, policy);
                            }
                        }
                        let (result, requests) = source_policy_request(
                            vec![
                                source_xml(Some("stuck"), true, Some("a-source")),
                                source_xml(Some("stuck"), true, None),
                            ],
                            policy,
                            None,
                            None,
                        )
                        .await;
                        assert_eq!(requests.len(), 2, "the failure must occur during a real refill");
                        assert!(!requests[0].contains("continuation-token="));
                        assert!(requests[1].contains("continuation-token=stuck"));
                        assert_source_policy_result(result, policy);
                    }
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_empty_advancing_source_pages_reach_eof_on_the_handler_path() {
        run_large_stack_test("list-through-empty-source-pages", || async {
            temp_env::async_with_vars(
                [
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    for filter_prefix in [None, Some("photos/2024/")] {
                        let source_key = if filter_prefix.is_some() {
                            "photos/2024/a-source"
                        } else {
                            "a-source"
                        };
                        let (result, requests) = source_policy_request(
                            vec![
                                source_xml(Some("opaque-next"), true, None),
                                source_xml(None, false, Some(source_key)),
                            ],
                            SourceErrorPolicy::Propagate,
                            None,
                            filter_prefix,
                        )
                        .await;
                        assert_eq!(requests.len(), 2, "an empty truncated source page must reach its successor");
                        assert!(requests[1].contains("continuation-token=opaque-next"));
                        let response = result.expect("empty progressing source page is valid");
                        assert!(!response.headers.contains_key("x-rustfs-on-demand-migration-list"));
                        let output = response.output;
                        let objects: Vec<_> = output
                            .contents
                            .unwrap_or_default()
                            .into_iter()
                            .map(|object| object.key.expect("listed object key"))
                            .collect();
                        if filter_prefix.is_some() {
                            assert_eq!(objects, vec!["z-local"]);
                            assert_eq!(
                                output
                                    .common_prefixes
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|prefix| prefix.prefix.expect("rolled-up prefix"))
                                    .collect::<Vec<_>>(),
                                vec!["photos/"]
                            );
                        } else {
                            assert_eq!(objects, vec!["a-source", "z-local"]);
                            assert!(output.common_prefixes.unwrap_or_default().is_empty());
                        }
                        assert_eq!(output.key_count, Some(2));
                        assert_eq!(output.is_truncated, Some(false));
                        assert!(output.next_continuation_token.is_none());
                    }
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_cross_request_empty_cursor_cycle_obeys_policy() {
        run_large_stack_test("list-through-cross-request-cursor-cycle", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, Some("true")),
                    (ENV_LIST_FRAMED_TOKENS, None),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    for policy in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound] {
                        let pages = ["B", "C", "A"].map(|next| source_xml(Some(next), true, None));
                        let (endpoint, server, stop) = list_source(pages.into_iter().cycle()).await;
                        let (_state_guard, mut input) = source_policy_input(endpoint, policy, Some("A"), None).await;
                        let mut seen = std::collections::HashSet::from([input
                            .continuation_token
                            .clone()
                            .expect("the first request resumes source cursor A")]);
                        let mut client_requests = 0;
                        let mut empty_pages = 0;
                        let terminal = tokio::time::timeout(Duration::from_secs(30), async {
                            loop {
                                client_requests += 1;
                                let response = match execute_source_list(input.clone()).await {
                                    Ok(response) => response,
                                    Err(error) => break Err(error),
                                };
                                if response.headers.contains_key("x-rustfs-on-demand-migration-list") {
                                    break Ok(response);
                                }
                                let output = response.output;
                                assert!(output.contents.as_ref().is_none_or(Vec::is_empty));
                                assert!(output.common_prefixes.as_ref().is_none_or(Vec::is_empty));
                                assert_eq!(output.key_count, Some(0));
                                assert_eq!(output.is_truncated, Some(true));
                                let next = output
                                    .next_continuation_token
                                    .expect("a truncated page must carry its cursor");
                                assert!(
                                    seen.insert(next.clone()),
                                    "a cross-request source cursor cycle must not return an identical empty merged token"
                                );
                                assert!(!decode_wire_token(&next).framed, "the budget switch cannot enable framing");
                                empty_pages += 1;
                                input.continuation_token = Some(next);
                            }
                        })
                        .await
                        .expect("a source cursor cycle must terminate within a bounded client pagination chain");
                        assert_eq!(empty_pages, usize::from(MAX_LIST_NO_PROGRESS_PAGES - 1));
                        assert_eq!(client_requests, usize::from(MAX_LIST_NO_PROGRESS_PAGES));
                        assert_source_policy_result(terminal, policy);
                        stop.cancel();
                        let requests = tokio::time::timeout(Duration::from_secs(5), server)
                            .await
                            .expect("cyclic source server must stop")
                            .expect("cyclic source server must not panic");
                        assert_eq!(requests.len(), 2 * client_requests, "the sixteenth empty page exhausts the budget");
                        for (index, request) in requests.iter().enumerate() {
                            let source_cursor = ["A", "B", "C"][index % 3];
                            assert!(
                                request.contains(&format!("continuation-token={source_cursor}")),
                                "the real SDK must follow the returned source cursor: {request}"
                            );
                        }
                    }
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_default_rollout_continues_v2_without_issuing_it_from_v1() {
        run_large_stack_test("list-through-reader-first-rollout", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, None),
                    (ENV_LIST_FRAMED_TOKENS, None),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    for (policy, framed) in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound]
                        .into_iter()
                        .flat_map(|policy| [false, true].map(|framed| (policy, framed)))
                    {
                        let pages = ["B", "C", "A"].map(|next| source_xml(Some(next), true, None));
                        let (endpoint, server, stop) = list_source(pages.into_iter().cycle()).await;
                        let (_state_guard, mut input) = source_policy_input(endpoint, policy, Some("A"), None).await;
                        let original = input.continuation_token.clone();
                        for _ in 0..3 {
                            let response = execute_source_list(input.clone()).await.expect("reader-only v1 behavior");
                            assert!(!response.headers.contains_key("x-rustfs-on-demand-migration-list"));
                            assert_eq!(response.output.key_count, Some(0));
                            assert_eq!(response.output.is_truncated, Some(true));
                            let next = response.output.next_continuation_token.expect("resumable empty page");
                            let raw = base64_simd::STANDARD.decode_to_vec(&next).expect("base64 continuation token");
                            let decoded = std::str::from_utf8(&raw).expect("JSON token");
                            let token = decode_list_cursor(Some(decoded)).expect("v1 reader").expect("merged token");
                            assert!(!token.framed, "the default cannot begin issuing framed tokens");
                            assert_eq!(token.v, 1, "the default rollout cannot begin issuing v2");
                            assert_eq!(token.no_progress, None);
                            assert!(!decoded.contains("no_progress"), "ordinary v1 wire shape stays unchanged");
                            input.continuation_token = Some(next);
                        }
                        assert_eq!(input.continuation_token, original, "default rollout retains the known v1 limitation");

                        let raw = base64_simd::STANDARD
                            .decode_to_vec(input.continuation_token.as_ref().expect("v1 token"))
                            .expect("base64 continuation token");
                        let mut token = decode_list_cursor(Some(std::str::from_utf8(&raw).expect("JSON token")))
                            .expect("v1 reader")
                            .expect("merged token");
                        token.framed = framed;
                        token.v = 2;
                        token.no_progress = Some(MAX_LIST_NO_PROGRESS_PAGES - 2);
                        input.continuation_token = Some(base64_simd::STANDARD.encode_to_string(token.encode().as_bytes()));
                        let response = execute_source_list(input.clone()).await.expect("reader-only node resumes v2");
                        assert_eq!(response.output.key_count, Some(0));
                        assert_eq!(response.output.is_truncated, Some(true));
                        let next = response.output.next_continuation_token.expect("last allowed empty cursor");
                        let raw = base64_simd::STANDARD.decode_to_vec(&next).expect("base64 continuation token");
                        let token = decode_list_cursor(Some(std::str::from_utf8(&raw).expect("JSON token")))
                            .expect("v2 reader")
                            .expect("merged token");
                        assert_eq!(token.framed, framed, "reader-only nodes retain the incoming framing");
                        assert_eq!(token.v, 2);
                        assert_eq!(token.no_progress, Some(MAX_LIST_NO_PROGRESS_PAGES - 1));
                        input.continuation_token = Some(next);
                        assert_source_policy_result(execute_source_list(input).await, policy);
                        stop.cancel();
                        let requests = tokio::time::timeout(Duration::from_secs(5), server)
                            .await
                            .expect("cyclic source server must stop")
                            .expect("source server must not panic");
                        assert_eq!(requests.len(), 10, "five handler requests each fetched two source pages");
                        for (index, request) in requests.iter().enumerate() {
                            let cursor = ["A", "B", "C"][index % 3];
                            assert!(request.contains(&format!("continuation-token={cursor}")), "{request}");
                        }
                    }
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_empty_advancing_pages_resume_across_handler_requests() {
        run_large_stack_test("list-through-resumable-empty-pages", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, Some("true")),
                    (ENV_LIST_FRAMED_TOKENS, None),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    for filter_prefix in [None, Some("photos/2024/")] {
                        let source_key = filter_prefix.map_or("a-source", |_| "photos/2024/a-source");
                        let (endpoint, server) = scripted_list_source(vec![
                            source_xml(Some("A"), true, None),
                            source_xml(Some("B"), true, None),
                            source_xml(Some("C"), true, None),
                            source_xml(None, false, Some(source_key)),
                        ])
                        .await;
                        let (_state_guard, mut input) =
                            source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, filter_prefix).await;
                        let first = execute_source_list(input.clone())
                            .await
                            .expect("valid empty pages must remain resumable");
                        assert!(!first.headers.contains_key("x-rustfs-on-demand-migration-list"));
                        assert!(first.output.contents.as_ref().is_none_or(Vec::is_empty));
                        assert!(first.output.common_prefixes.as_ref().is_none_or(Vec::is_empty));
                        assert_eq!(first.output.key_count, Some(0));
                        assert_eq!(first.output.is_truncated, Some(true));
                        input.continuation_token = Some(first.output.next_continuation_token.expect("empty advancing cursor"));

                        let second = execute_source_list(input)
                            .await
                            .expect("a progressing empty chain must reach its data");
                        assert!(!second.headers.contains_key("x-rustfs-on-demand-migration-list"));
                        let output = second.output;
                        let objects = output
                            .contents
                            .unwrap_or_default()
                            .into_iter()
                            .map(|object| object.key.expect("listed object key"))
                            .collect::<Vec<_>>();
                        let prefixes = output
                            .common_prefixes
                            .unwrap_or_default()
                            .into_iter()
                            .map(|prefix| prefix.prefix.expect("listed common prefix"))
                            .collect::<Vec<_>>();
                        if filter_prefix.is_some() {
                            assert_eq!(objects, vec!["z-local"]);
                            assert_eq!(prefixes, vec!["photos/"]);
                        } else {
                            assert_eq!(objects, vec!["a-source", "z-local"]);
                            assert!(prefixes.is_empty());
                        }
                        assert_eq!(output.key_count, Some(2));
                        assert_eq!(output.is_truncated, Some(false));
                        assert!(output.next_continuation_token.is_none());
                        let requests = tokio::time::timeout(Duration::from_secs(5), server)
                            .await
                            .expect("finite source connections must finish")
                            .expect("finite source server must not panic");
                        assert_eq!(requests.len(), 4);
                        assert!(!requests[0].contains("continuation-token="));
                        for (request, cursor) in requests[1..].iter().zip(["A", "B", "C"]) {
                            assert!(request.contains(&format!("continuation-token={cursor}")), "{request}");
                        }
                    }
                },
            )
            .await;
        });
    }

    fn decode_wire_token(wire: &str) -> ListThroughToken {
        let raw = base64_simd::STANDARD.decode_to_vec(wire).expect("base64 continuation token");
        decode_list_cursor(Some(std::str::from_utf8(&raw).expect("UTF-8 cursor")))
            .expect("valid continuation token")
            .expect("merged continuation token")
    }

    #[test]
    fn framed_local_continuations_preserve_json_markers_and_zero_sized_budgets() {
        let json_key = r#"{"t":"odm-list","v":1,"local_done":true}"#;
        let mut resume = token(Some("local-2"), false);
        resume.framed = true;
        resume.v = 2;
        resume.no_progress = Some(15);
        let mut page = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some(json_key.to_string()),
            objects: vec![info(json_key)],
            ..Default::default()
        };
        preserve_framed_local_cursor(&mut page, Some(&resume));
        let raw = page.next_continuation_token.expect("local continuation");
        assert!(raw.starts_with("\0odm-list:"));
        let decoded = decode_list_cursor(Some(&raw))
            .expect("framed local continuation")
            .expect("envelope");
        assert!(decoded.framed);
        assert_eq!(
            decoded.local.as_deref(),
            Some(json_key),
            "the local marker is embedded without another encoding"
        );
        assert_eq!(decoded.source, resume.source);
        assert_eq!(decoded.last_key.as_deref(), Some(json_key));
        assert_eq!(decoded.v, 1);
        assert_eq!(decoded.no_progress, None);
        assert!(matches!(local_cursor(Some(&raw), Some(&decoded)), LocalListCursor::Token(Some(local)) if local == json_key));

        let mut prefix_page = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some("photos/".to_string()),
            prefixes: vec!["photos/".to_string()],
            ..Default::default()
        };
        preserve_framed_local_cursor(&mut prefix_page, Some(&resume));
        let prefix = decode_list_cursor(prefix_page.next_continuation_token.as_deref())
            .expect("prefix continuation")
            .expect("framed prefix envelope");
        assert!(prefix.framed);
        assert_eq!(prefix.last_key.as_deref(), Some("photos/"));
        assert_eq!(prefix.v, 1);
        assert_eq!(prefix.no_progress, None);

        let mut zero = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: resume.local.clone(),
            ..Default::default()
        };
        preserve_framed_local_cursor(&mut zero, Some(&resume));
        assert_eq!(
            decode_list_cursor(zero.next_continuation_token.as_deref()).expect("zero-sized continuation"),
            Some(resume.clone())
        );

        resume.framed = false;
        let mut ordinary = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some(json_key.to_string()),
            ..Default::default()
        };
        preserve_framed_local_cursor(&mut ordinary, Some(&resume));
        assert_eq!(ordinary.next_continuation_token.as_deref(), Some(json_key));
    }

    #[test]
    #[serial_test::serial]
    fn list_through_historical_v2_budget_exhausts_on_the_next_reader_only_request() {
        run_large_stack_test("list-through-historical-budget", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, None),
                    (ENV_LIST_FRAMED_TOKENS, None),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None), ("HTTPS_PROXY", None), ("ALL_PROXY", None),
                    ("http_proxy", None), ("https_proxy", None), ("all_proxy", None),
                    ("NO_PROXY", Some("*")), ("no_proxy", Some("*")),
                ],
                async {
                    for policy in [SourceErrorPolicy::Propagate, SourceErrorPolicy::NotFound] {
                        let (endpoint, server) = scripted_list_source(vec![
                            source_xml(Some("B"), true, None), source_xml(Some("C"), true, None),
                        ]).await;
                        let (_state_guard, mut input) = source_policy_input(endpoint, policy, None, None).await;
                        // Fixed bytes from the pre-framing writer, independent of today's encoder.
                        input.continuation_token = Some("eyJ0Ijoib2RtLWxpc3QiLCJ2IjoyLCJsb2NhbCI6bnVsbCwibG9jYWxfZG9uZSI6ZmFsc2UsInNvdXJjZSI6IkEiLCJzb3VyY2VfZG9uZSI6ZmFsc2UsImxhc3Rfa2V5IjpudWxsLCJub19wcm9ncmVzcyI6MTV9".to_string());
                        assert_source_policy_result(execute_source_list(input).await, policy);
                        let requests = tokio::time::timeout(Duration::from_secs(5), server).await
                            .expect("finite source server must finish").expect("source server must not panic");
                        assert_eq!(requests.len(), 2, "the old count=15 must terminate without starting a new budget");
                        for (request, cursor) in requests.iter().zip(["A", "B"]) {
                            assert!(request.contains(&format!("continuation-token={cursor}")), "{request}");
                        }
                    }
                },
            ).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_framing_survives_budget_reset_and_local_only_pagination() {
        run_large_stack_test("list-through-framing-local-pagination", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, None),
                    (ENV_LIST_FRAMED_TOKENS, Some("true")),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None),
                    ("HTTPS_PROXY", None),
                    ("ALL_PROXY", None),
                    ("http_proxy", None),
                    ("https_proxy", None),
                    ("all_proxy", None),
                    ("NO_PROXY", Some("*")),
                    ("no_proxy", Some("*")),
                ],
                async {
                    let (endpoint, server) = scripted_list_source(vec![
                        source_xml(Some("A"), true, None),
                        source_xml(Some("B"), true, None),
                        source_xml(Some("C"), true, None),
                        source_xml(Some("D"), true, None),
                        source_xml(None, false, Some("0-source")),
                    ])
                    .await;
                    let (_state_guard, mut input) = source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, None).await;
                    let json_key = r#"{"t":"odm-list","v":1,"local_done":true}"#;
                    let expected_local = ["a-local", "b-local", "z-local", json_key, "~last"];
                    let store = shared_gating_ecstore().await;
                    for key in ["a-local", "b-local", json_key, "~last"] {
                        store
                            .put_object(
                                &input.bucket,
                                key,
                                &mut StoragePutObjReader::from_vec(vec![1]),
                                &StorageObjectOptions::default(),
                            )
                            .await
                            .expect("seed paginated local keys");
                    }
                    input.max_keys = Some(1);
                    let first = execute_source_list(input.clone()).await.expect("legitimate empty page");
                    assert_eq!(first.output.key_count, Some(0));
                    assert_eq!(first.output.is_truncated, Some(true));
                    let next = first.output.next_continuation_token.expect("first framed cursor");
                    let token = decode_wire_token(&next);
                    assert!(token.framed, "the independent switch permits first framing issuance");
                    assert_eq!(token.v, 1, "framing issuance cannot enable the no-progress budget");
                    assert_eq!(token.no_progress, None);
                    input.continuation_token = Some(next);
                    let budget_page =
                        temp_env::async_with_vars([(ENV_LIST_PROGRESS_TOKENS, Some("true"))], execute_source_list(input.clone()))
                            .await
                            .expect("another valid empty page starts a budget only when enabled");
                    assert_eq!(budget_page.output.key_count, Some(0));
                    assert_eq!(budget_page.output.is_truncated, Some(true));
                    let next = budget_page.output.next_continuation_token.expect("framed v2 cursor");
                    let token = decode_wire_token(&next);
                    assert!(token.framed);
                    assert_eq!(token.v, 2);
                    assert_eq!(token.no_progress, Some(1));
                    input.continuation_token = Some(next);

                    temp_env::async_with_vars(
                        [(ENV_LIST_PROGRESS_TOKENS, None::<&str>), (ENV_LIST_FRAMED_TOKENS, None)],
                        async {
                            let second = execute_source_list(input.clone())
                                .await
                                .expect("reader-only node reaches source data");
                            assert_eq!(second.output.key_count, Some(1));
                            assert_eq!(second.output.is_truncated, Some(true));
                            assert_eq!(second.output.contents.expect("source object")[0].key.as_deref(), Some("0-source"));
                            let next = second.output.next_continuation_token.expect("remaining local listing");
                            let token = decode_wire_token(&next);
                            assert!(token.framed, "resetting the budget must not downgrade the framing");
                            assert_eq!(token.v, 1);
                            assert_eq!(token.no_progress, None);
                            assert!(token.source_done);
                            input.continuation_token = Some(next);

                            OnDemandMigrationSys::get().remove(&input.bucket);
                            for (index, key) in expected_local.iter().enumerate() {
                                let page = execute_source_list(input.clone()).await.expect("local-only continuation");
                                assert!(!page.headers.contains_key("x-rustfs-on-demand-migration-list"));
                                assert_eq!(page.output.key_count, Some(1));
                                let keys: Vec<_> = page
                                    .output
                                    .contents
                                    .expect("one local object")
                                    .into_iter()
                                    .map(|object| object.key.expect("local key"))
                                    .collect();
                                assert_eq!(
                                    keys,
                                    vec![key.to_string()],
                                    "no duplicate or omitted key after disabling list-through"
                                );
                                let truncated = index + 1 < expected_local.len();
                                assert_eq!(page.output.is_truncated, Some(truncated));
                                if truncated {
                                    let next = page.output.next_continuation_token.expect("local side still has keys");
                                    let token = decode_wire_token(&next);
                                    assert!(token.framed);
                                    assert_eq!(token.v, 1);
                                    assert_eq!(token.no_progress, None);
                                    assert!(token.local.as_deref().expect("local marker").starts_with(*key));
                                    assert_eq!(token.last_key.as_deref(), Some(*key));
                                    input.continuation_token = Some(next);
                                } else {
                                    assert!(page.output.next_continuation_token.is_none());
                                }
                            }
                        },
                    )
                    .await;
                    let requests = tokio::time::timeout(Duration::from_secs(5), server)
                        .await
                        .expect("finite source server must finish")
                        .expect("source server must not panic");
                    assert_eq!(
                        requests.len(),
                        5,
                        "format changes and local-only continuation perform no additional source I/O"
                    );
                    assert!(!requests[0].contains("continuation-token="));
                    for (request, cursor) in requests[1..].iter().zip(["A", "B", "C", "D"]) {
                        assert!(request.contains(&format!("continuation-token={cursor}")), "{request}");
                    }
                },
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_through_zero_sized_framed_request_preserves_its_budget() {
        run_large_stack_test("list-through-framed-zero-size", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, None), (ENV_LIST_FRAMED_TOKENS, None),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None), ("HTTPS_PROXY", None), ("ALL_PROXY", None),
                    ("http_proxy", None), ("https_proxy", None), ("all_proxy", None),
                    ("NO_PROXY", Some("*")), ("no_proxy", Some("*")),
                ],
                async {
                    let (endpoint, server) = scripted_list_source(vec![
                        source_xml(Some("B"), true, None), source_xml(Some("C"), true, None),
                    ]).await;
                    let (_state_guard, mut input) = source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, None).await;
                    let wire = concat!("\0odm-list:", r#"{"t":"odm-list","v":2,"local":null,"local_done":true,"source":"A","source_done":false,"last_key":null,"no_progress":15}"#);
                    input.continuation_token = Some(base64_simd::STANDARD.encode_to_string(wire.as_bytes()));
                    input.max_keys = Some(0);
                    let zero = execute_source_list(input.clone()).await.expect("zero-sized request does not spend the budget");
                    assert_eq!(zero.output.key_count, Some(0));
                    assert_eq!(zero.output.is_truncated, Some(true));
                    let next = zero.output.next_continuation_token.expect("unconsumed source");
                    let token = decode_wire_token(&next);
                    assert!(token.framed);
                    assert_eq!(token.v, 2);
                    assert_eq!(token.no_progress, Some(15));
                    assert_eq!(token.source.as_deref(), Some("A"));
                    assert_eq!(next, input.continuation_token.as_ref().expect("original cursor").as_str());
                    let state = OnDemandMigrationSys::get().state(&input.bucket).expect("source state");
                    assert_eq!(state.stats().snapshot(state.breaker().state()).source_latency.count, 0, "zero-sized request must not fetch the source");
                    input.continuation_token = Some(next);
                    input.max_keys = Some(2);
                    assert_source_policy_result(execute_source_list(input).await, SourceErrorPolicy::Propagate);
                    let requests = tokio::time::timeout(Duration::from_secs(5), server).await
                        .expect("finite source server must finish").expect("source server must not panic");
                    assert_eq!(requests.len(), 2, "only the resumed nonzero request fetches the source");
                    assert_eq!(state.stats().snapshot(state.breaker().state()).source_latency.count, 2);
                    for (request, cursor) in requests.iter().zip(["A", "B"]) {
                        assert!(request.contains(&format!("continuation-token={cursor}")), "{request}");
                    }
                },
            ).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn zero_sized_merged_cursors_preserve_each_side_and_wire_format() {
        run_large_stack_test("list-through-zero-side-matrix", || async {
            temp_env::async_with_vars(
                [
                    (ENV_LIST_PROGRESS_TOKENS, Some("true")),
                    (ENV_LIST_FRAMED_TOKENS, Some("true")),
                    ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
                    ("HTTP_PROXY", None), ("HTTPS_PROXY", None), ("ALL_PROXY", None),
                    ("http_proxy", None), ("https_proxy", None), ("all_proxy", None),
                    ("NO_PROXY", Some("*")), ("no_proxy", Some("*")),
                ],
                async {
                    for framed in [false, true] {
                        for (local_done, source_done) in [(false, true), (true, false), (false, false), (true, true)] {
                            let (endpoint, server, stop) = list_source(std::iter::repeat(source_xml(Some("unexpected"), true, None))).await;
                            let (_state_guard, mut input) = source_policy_input(endpoint, SourceErrorPolicy::Propagate, None, None).await;
                            let json = format!(r#"{{"t":"odm-list","v":2,"local":"local-marker","local_done":{local_done},"source":"source-marker","source_done":{source_done},"last_key":null,"no_progress":15}}"#);
                            let wire = if framed { format!("\0odm-list:{json}") } else { json };
                            let original = base64_simd::STANDARD.encode_to_string(wire.as_bytes());
                            input.continuation_token = Some(original.clone());
                            input.max_keys = Some(0);
                            let output = execute_source_list(input).await.expect("zero page remains local").output;
                            let has_more = !local_done || !source_done;
                            assert_eq!(output.key_count, Some(0));
                            assert_eq!(output.is_truncated, Some(has_more), "framed={framed}, local_done={local_done}, source_done={source_done}");
                            assert_eq!(output.next_continuation_token.as_deref(), has_more.then_some(original.as_str()));
                            if let Some(next) = output.next_continuation_token {
                                let token = decode_wire_token(&next);
                                assert_eq!(token.framed, framed);
                                assert_eq!(token.v, 2);
                                assert_eq!(token.no_progress, Some(15));
                                assert_eq!(token.local_done, local_done);
                                assert_eq!(token.source_done, source_done);
                                assert_eq!(token.local.as_deref(), Some("local-marker"));
                                assert_eq!(token.source.as_deref(), Some("source-marker"));
                            }
                            stop.cancel();
                            let requests = tokio::time::timeout(Duration::from_secs(5), server).await
                                .expect("unused source must finish").expect("source server must not panic");
                            assert!(requests.is_empty(), "zero-sized request must not access the source: {requests:?}");
                        }
                    }
                },
            ).await;
        });
    }

    fn assert_source_policy_result(result: S3Result<S3Response<ListObjectsV2Output>>, policy: SourceErrorPolicy) {
        match policy {
            SourceErrorPolicy::Propagate => {
                let error = result.expect_err("propagate must expose malformed pagination");
                assert_eq!(error.status_code(), Some(http::StatusCode::FAILED_DEPENDENCY));
                assert_eq!(error.code(), &S3ErrorCode::Custom("SourceUnavailable".into()));
                assert_eq!(error.message(), Some("invalid_pagination"));
            }
            SourceErrorPolicy::NotFound => {
                let response = result.expect("not_found must preserve the local listing");
                assert_eq!(
                    response
                        .headers
                        .get("x-rustfs-on-demand-migration-list")
                        .expect("local_only header"),
                    "local_only"
                );
                let output = response.output;
                assert_eq!(
                    output
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .map(|object| object.key.expect("local key"))
                        .collect::<Vec<_>>(),
                    vec!["z-local"]
                );
                assert_eq!(output.is_truncated, Some(false));
                assert_eq!(output.key_count, Some(1));
                assert!(output.next_continuation_token.is_none());
            }
        }
    }
}
