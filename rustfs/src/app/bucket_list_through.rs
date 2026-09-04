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
use super::storage_api::bucket_usecase::bucket::on_demand_migration::{
    BucketOdmState, ListEntryKey, ListThroughCursor, ListThroughMerger, ListThroughToken, ListThroughTokenError, MergeSide,
    OnDemandMigrationSys, SOURCE_LIST_MAX_RATE_WAIT, SourceClient, SourceError, SourceErrorPolicy, SourceListPlan,
    SourceListRequest, SourceObject, SourcePage, decode_continuation_token, source_list_plan,
};
use super::storage_api::bucket_usecase::bucket::versioning_sys::BucketVersioningSys;
use super::storage_api::bucket_usecase::contract::list::{ListObjectsV2Info as StorageListObjectsV2Info, ListOperations as _};
use super::storage_api::bucket_usecase::contract::object::ObjectOperations as _;
use super::storage_api::bucket_usecase::s3::{S3Error, S3ErrorCode, S3Result};
use super::storage_api::bucket_usecase::s3_api::bucket::ListObjectsV2Params;
use crate::app::object::shared::{odm_source_unavailable_error, odm_state_error_class};
use crate::error::ApiError;
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
pub(crate) fn list_through_state(bucket: &str, headers: &HeaderMap) -> Option<Arc<BucketOdmState>> {
    if get_header(headers, SUFFIX_SOURCE_PROXY_REQUEST).is_some() {
        return None;
    }
    let sys = OnDemandMigrationSys::get();
    if !sys.is_module_enabled() {
        return None;
    }
    let state = sys.state(bucket)?;
    state.config().policy.list_through.then_some(state)
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
/// Cost: one local listing plus at most two source listings per request (the
/// first page of each side, plus one refill when the previous page consumed
/// most of what a side had buffered).
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
        buffers[usize::from(fetch.side == MergeSide::Source)].extend(kept.into_iter().map(Some));
        merger.push_page(fetch.side, keys, is_truncated, next_token);
    }

    let outcome = merger.finish();
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
            next_continuation_token: outcome.next_token.map(|token| token.encode()),
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
            start_after: token.is_none().then(|| params.start_after_for_query.as_deref()).flatten(),
            continuation_token: token,
            max_keys: params.max_keys,
        },
        // Everything under `filter.prefix` rolls into one common prefix, so a
        // single bounded listing settles whether it exists.
        SourceListPlan::Folded { probe_prefix, .. } => SourceListRequest {
            prefix: Some(probe_prefix.as_str()),
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
                exists
                    .then(|| vec![SideEntry::Prefix(common_prefix.clone())])
                    .unwrap_or_default(),
                false,
                None,
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

    fn token(local: Option<&str>, local_done: bool) -> ListThroughToken {
        ListThroughToken {
            t: "odm-list".to_string(),
            v: 1,
            local: local.map(str::to_string),
            local_done,
            source: Some("source-2".to_string()),
            source_done: false,
            last_key: Some("k".to_string()),
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
    fn a_plain_local_token_is_passed_through_and_a_tampered_one_is_rejected() {
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
}
