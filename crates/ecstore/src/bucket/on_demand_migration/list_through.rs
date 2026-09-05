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

//! Optional `ListObjectsV2` list-through (`policy.list_through`,
//! rustfs/backlog#2164): the local listing and the source listing are merged
//! into one ordered page so clients see the whole namespace while a bucket is
//! migrating.
//!
//! Everything here is pure. The handler owns the I/O and the payloads; this
//! module owns the ordering, the page boundary, and the opaque continuation
//! token that carries both cursors.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// The only continuation-token envelope version this build reads and writes.
pub const LIST_THROUGH_TOKEN_VERSION: u32 = 1;

/// Envelope marker. A bucket that is *not* merging hands out the local
/// listing's own marker, so the decoder needs a positive signal before it
/// treats an opaque token as a merged one.
const LIST_THROUGH_TOKEN_TAG: &str = "odm-list";
// Object keys cannot contain NUL (bucket::utils::is_valid_object_prefix),
// so this framing cannot collide with a local key used as an opaque marker.
const LIST_THROUGH_TOKEN_PREFIX: &str = "\0odm-list:";

/// Pages fetched per side per request: the first page, plus at most one refill
/// when the first one was mostly consumed by the previous page. Two pages of
/// `max_keys` always cover a full merged page, so this is a bound, not a
/// heuristic.
pub const MAX_LIST_FETCHES_PER_SIDE: usize = 2;

/// Per-bucket ceiling on source `ListObjectsV2` calls, in calls per second.
pub const SOURCE_LIST_RATE_PER_SEC: u32 = 10;

/// How long a listing may wait for a source rate-limit slot before it gives up
/// and answers from local state alone.
pub const SOURCE_LIST_MAX_RATE_WAIT: Duration = Duration::from_secs(1);

/// One listing entry as the merge orders it: an object key, or — under a
/// delimiter — a rolled-up common prefix. Both sort by `name` alone, which is
/// how S3 interleaves `Contents` and `CommonPrefixes` on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListEntryKey {
    pub name: String,
    pub is_prefix: bool,
}

impl ListEntryKey {
    pub fn object(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            is_prefix: false,
        }
    }

    pub fn prefix(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            is_prefix: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeSide {
    Local,
    Source,
}

/// One entry of the merged page: the side it came from and its index in that
/// side's buffer, in push order. The caller keeps the payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergePick {
    pub side: MergeSide,
    pub index: usize,
}

/// The continuation-token envelope. Opaque to clients: it is serialized as
/// framed JSON and then base64-encoded by the same helper as a local marker.
///
/// A `null` cursor with `done = false` means "list that side from the start";
/// `done = true` means the side is finished and must not be listed again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListThroughToken {
    /// Envelope marker, always [`LIST_THROUGH_TOKEN_TAG`].
    pub t: String,
    pub v: u32,
    #[serde(default)]
    pub local: Option<String>,
    #[serde(default)]
    pub local_done: bool,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub source_done: bool,
    /// Last entry the previous page consumed. A side whose page was only
    /// partially consumed is re-listed from the same cursor and everything at
    /// or below this key is dropped, which is delimiter-safe: a rolled-up
    /// common prefix compares as itself, never as its members.
    #[serde(default)]
    pub last_key: Option<String>,
}

impl ListThroughToken {
    fn new(local: SideCursor, source: SideCursor, last_key: Option<String>) -> Self {
        Self {
            t: LIST_THROUGH_TOKEN_TAG.to_string(),
            v: LIST_THROUGH_TOKEN_VERSION,
            local: local.token,
            local_done: local.done,
            source: source.token,
            source_done: source.done,
            last_key,
        }
    }

    pub fn encode(&self) -> String {
        // The envelope is built here from owned strings, so serialization
        // cannot fail; the fallback keeps the signature infallible.
        format!("{LIST_THROUGH_TOKEN_PREFIX}{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// What a decoded (base64-stripped) continuation token turned out to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListThroughCursor {
    /// A plain local listing marker: the bucket was not merging when the token
    /// was issued, or the client is paginating a non-merged listing.
    Local(String),
    Merged(Box<ListThroughToken>),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ListThroughTokenError {
    #[error("continuation token version {0} is not supported")]
    UnsupportedVersion(u32),
    /// The message never echoes the token: it is client-controlled input.
    #[error("continuation token is malformed")]
    Malformed,
}

/// Classifies an already base64-decoded continuation token.
///
/// Only a framed JSON object is read as a merged token;
/// anything else is a local marker, so a bucket that turns `list_through` off
/// keeps paginating with the tokens it handed out. A token that *is* an
/// envelope but was tampered with (unknown version, unknown field, truncated
/// JSON) is an error, never a silent fallback.
pub fn decode_continuation_token(decoded: &str) -> Result<ListThroughCursor, ListThroughTokenError> {
    let Some(payload) = decoded.strip_prefix(LIST_THROUGH_TOKEN_PREFIX) else {
        return Ok(ListThroughCursor::Local(decoded.to_string()));
    };
    let value = serde_json::from_str::<serde_json::Value>(payload).map_err(|_| ListThroughTokenError::Malformed)?;
    if value.get("t").and_then(serde_json::Value::as_str) != Some(LIST_THROUGH_TOKEN_TAG) {
        return Err(ListThroughTokenError::Malformed);
    }
    match value.get("v").and_then(serde_json::Value::as_u64) {
        Some(version) if version == u64::from(LIST_THROUGH_TOKEN_VERSION) => {}
        Some(version) => return Err(ListThroughTokenError::UnsupportedVersion(version.min(u64::from(u32::MAX)) as u32)),
        None => return Err(ListThroughTokenError::Malformed),
    }
    serde_json::from_value::<ListThroughToken>(value)
        .map(|token| ListThroughCursor::Merged(Box::new(token)))
        .map_err(|_| ListThroughTokenError::Malformed)
}

/// How the source must be listed for a request, given `filter.prefix`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceListPlan {
    /// The request prefix and `filter.prefix` are disjoint: the source holds
    /// nothing this listing could show.
    Skip,
    /// Ordinary paged listing under `prefix`, rolled up with the request's
    /// delimiter — the source's own roll-up boundary matches the request's.
    Page { prefix: String },
    /// `filter.prefix` reaches past a delimiter, so every key the source could
    /// contribute rolls into this one common prefix. Bounded probes follow
    /// empty progressing pages until a key proves existence or the source ends.
    Folded { probe_prefix: String, common_prefix: String },
}

/// Intersects the request prefix with `filter.prefix` and decides how (or
/// whether) the source is listed.
pub fn source_list_plan(request_prefix: &str, filter_prefix: Option<&str>, delimiter: Option<&str>) -> SourceListPlan {
    let filter = filter_prefix.unwrap_or_default();
    let source_prefix = if filter.starts_with(request_prefix) {
        filter
    } else if request_prefix.starts_with(filter) {
        request_prefix
    } else {
        return SourceListPlan::Skip;
    };

    let Some(delimiter) = delimiter.filter(|delimiter| !delimiter.is_empty()) else {
        return SourceListPlan::Page {
            prefix: source_prefix.to_string(),
        };
    };

    // `source_prefix` always starts with `request_prefix`, so this slice is on
    // a character boundary.
    let extra = &source_prefix[request_prefix.len()..];
    match extra.find(delimiter) {
        Some(at) => SourceListPlan::Folded {
            probe_prefix: source_prefix.to_string(),
            common_prefix: format!("{request_prefix}{}", &extra[..at + delimiter.len()]),
        },
        None => SourceListPlan::Page {
            prefix: source_prefix.to_string(),
        },
    }
}

/// Where one side resumes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SideCursor {
    pub token: Option<String>,
    pub done: bool,
}

/// One page a side actually fetched this round.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FetchedPage {
    /// Token it was fetched with; `None` means from the start of the listing.
    token: Option<String>,
    /// Entries it contributed to the buffer, after the `last_key` filter.
    count: usize,
    /// Cursor for the page after it, `None` when it was the last one.
    next_token: Option<String>,
}

/// Where a side resumes after `consumed` of its buffered entries were taken.
///
/// A fully consumed page advances to its successor; a partially consumed one
/// is re-listed from the same cursor next time and re-filtered by `last_key`.
fn advance_cursor(pages: &[FetchedPage], consumed: usize) -> SideCursor {
    let mut remaining = consumed;
    let mut cursor = SideCursor { token: None, done: true };
    for page in pages {
        if remaining >= page.count {
            remaining -= page.count;
            cursor = match &page.next_token {
                Some(next) => SideCursor {
                    token: Some(next.clone()),
                    done: false,
                },
                None => SideCursor { token: None, done: true },
            };
        } else {
            cursor = SideCursor {
                token: page.token.clone(),
                done: false,
            };
            break;
        }
    }
    cursor
}

/// A page the merge driver still needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub side: MergeSide,
    pub token: Option<String>,
}

/// Invalid pagination metadata. Opaque cursor values are never included in errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ListPageError {
    #[error("truncated listing has no continuation token")]
    Missing,
    #[error("truncated listing has an empty continuation token")]
    Empty,
    #[error("truncated listing repeats a continuation token")]
    Repeated,
}

pub(crate) fn validate_list_page(is_truncated: bool, token: Option<&str>, next_token: Option<&str>) -> Result<(), ListPageError> {
    if is_truncated {
        match next_token {
            None => return Err(ListPageError::Missing),
            Some("") => return Err(ListPageError::Empty),
            Some(next) if Some(next) == token => return Err(ListPageError::Repeated),
            Some(_) => {}
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct SideState {
    start: SideCursor,
    pages: Vec<FetchedPage>,
    entries: Vec<ListEntryKey>,
    more: bool,
    disabled: bool,
}

impl SideState {
    fn from_cursor(token: Option<String>, done: bool) -> Self {
        Self {
            start: SideCursor { token, done },
            ..Default::default()
        }
    }

    fn needs_page(&self, max_keys: usize) -> Option<Option<String>> {
        if self.disabled || self.start.done {
            return None;
        }
        match self.pages.last() {
            None => Some(self.start.token.clone()),
            Some(last) => {
                let room = self.entries.len() < max_keys;
                let capped = self.pages.len() >= MAX_LIST_FETCHES_PER_SIDE;
                (self.more && room && !capped).then(|| last.next_token.clone())
            }
        }
    }
}

/// The merged page, once both sides have handed over everything they will.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeOutcome {
    /// Entries of the merged page, in wire order; indices point into each
    /// side's buffer in push order.
    pub picks: Vec<MergePick>,
    pub is_truncated: bool,
    /// `Some` exactly when `is_truncated`.
    pub next_token: Option<ListThroughToken>,
}

/// Drives one merged page: the caller asks [`Self::next_fetch`] what to list,
/// hands the page back with [`Self::push_page`], and finishes with
/// [`Self::finish`]. Nothing here does I/O, so the same driver is exercised by
/// the property test and by the handler.
#[derive(Debug)]
pub struct ListThroughMerger {
    max_keys: usize,
    last_key: Option<String>,
    local: SideState,
    source: SideState,
}

impl ListThroughMerger {
    /// `token` is the envelope from the client's continuation token, absent on
    /// the first page of a listing.
    pub fn new(max_keys: usize, token: Option<&ListThroughToken>) -> Self {
        let (local, source, last_key) = match token {
            Some(token) => (
                SideState::from_cursor(token.local.clone(), token.local_done),
                SideState::from_cursor(token.source.clone(), token.source_done),
                token.last_key.clone(),
            ),
            None => (SideState::default(), SideState::default(), None),
        };
        Self {
            max_keys,
            last_key,
            local,
            source,
        }
    }

    /// Whether an entry the listing returned still belongs to this page: a
    /// re-listed page repeats what the previous page already consumed.
    pub fn accepts(&self, name: &str) -> bool {
        self.last_key.as_deref().is_none_or(|bound| name > bound)
    }

    /// The source contributes nothing to this page: it failed, is rate-limited,
    /// or `filter.prefix` excludes it.
    pub fn disable_source(&mut self) {
        self.source.disabled = true;
        // A refill can fail after a valid first page. A local-only response
        // must discard both that source payload and its ordering horizon.
        self.source.entries.clear();
        self.source.pages.clear();
        self.source.more = false;
    }

    pub fn next_fetch(&self) -> Option<FetchRequest> {
        for (side, state) in [(MergeSide::Local, &self.local), (MergeSide::Source, &self.source)] {
            if let Some(token) = state.needs_page(self.max_keys) {
                return Some(FetchRequest { side, token });
            }
        }
        None
    }

    /// Records one fetched page. `entries` must be sorted by `name` and already
    /// filtered with [`Self::accepts`]; the caller keeps the matching payloads
    /// in the same order.
    pub fn push_page(
        &mut self,
        side: MergeSide,
        entries: Vec<ListEntryKey>,
        is_truncated: bool,
        next_token: Option<String>,
    ) -> Result<(), ListPageError> {
        let state = match side {
            MergeSide::Local => &mut self.local,
            MergeSide::Source => &mut self.source,
        };
        let token = match state.pages.last() {
            Some(last) => last.next_token.clone(),
            None => state.start.token.clone(),
        };
        validate_list_page(is_truncated, token.as_deref(), next_token.as_deref())?;
        // Also reject a cycle through an earlier page in this bounded fetch.
        if is_truncated && state.pages.iter().any(|page| page.token == next_token) {
            return Err(ListPageError::Repeated);
        }
        state.more = is_truncated;
        state.pages.push(FetchedPage {
            token,
            count: entries.len(),
            next_token: is_truncated.then_some(next_token).flatten(),
        });
        state.entries.extend(entries);
        Ok(())
    }

    pub fn finish(self) -> MergeOutcome {
        let Self {
            max_keys,
            last_key,
            local,
            source,
        } = self;

        // A side with more pages behind it can only be trusted up to the last
        // key it handed over: past that horizon the other side's entries could
        // still be deduplicated by one we have not seen, which is what keeps
        // "local wins on equal keys" true across page boundaries.
        let horizon = [
            local
                .more
                .then(|| local.entries.last().map_or("", |entry| entry.name.as_str())),
            source
                .more
                .then(|| source.entries.last().map_or("", |entry| entry.name.as_str())),
        ]
        .into_iter()
        .flatten()
        .min();

        let mut picks = Vec::with_capacity(max_keys.min(local.entries.len() + source.entries.len()));
        let mut consumed_local = 0usize;
        let mut consumed_source = 0usize;
        let mut consumed_key: Option<String> = None;

        while picks.len() < max_keys {
            let next_local = local.entries.get(consumed_local).map(|entry| entry.name.as_str());
            let next_source = source.entries.get(consumed_source).map(|entry| entry.name.as_str());
            let name = match (next_local, next_source) {
                (None, None) => break,
                (Some(name), None) | (None, Some(name)) => name,
                (Some(left), Some(right)) => left.min(right),
            };
            if horizon.is_some_and(|horizon| name > horizon) {
                break;
            }
            let take_local = next_local == Some(name);
            let take_source = next_source == Some(name);
            consumed_key = Some(name.to_string());
            if take_local {
                picks.push(MergePick {
                    side: MergeSide::Local,
                    index: consumed_local,
                });
                consumed_local += 1;
            } else {
                picks.push(MergePick {
                    side: MergeSide::Source,
                    index: consumed_source,
                });
            }
            if take_source {
                consumed_source += 1;
            }
        }

        let local_cursor = advance_cursor(&local.pages, consumed_local);
        let source_cursor = if source.disabled {
            // Keep the source where it was so a recovered source resumes there;
            // this page is answered from local state alone.
            source.start.clone()
        } else {
            advance_cursor(&source.pages, consumed_source)
        };
        let local_left = !local_cursor.done || consumed_local < local.entries.len();
        let source_left = !source.disabled && (!source_cursor.done || consumed_source < source.entries.len());
        let is_truncated = local_left || source_left;

        let last_key = consumed_key.or(last_key);
        MergeOutcome {
            picks,
            is_truncated,
            next_token: is_truncated.then(|| ListThroughToken::new(local_cursor, source_cursor, last_key)),
        }
    }
}

/// Token bucket capping source `ListObjectsV2` calls for one bucket.
///
/// A caller that cannot be served inside its budget is refused rather than
/// queued: a listing degrades to local state instead of holding the request
/// open behind other tenants' listings.
#[derive(Debug)]
pub struct SourceListRateLimiter {
    rate_per_sec: f64,
    burst: f64,
    state: Mutex<RateLimiterState>,
}

#[derive(Debug)]
struct RateLimiterState {
    tokens: f64,
    updated_at: Instant,
}

impl SourceListRateLimiter {
    pub fn new(rate_per_sec: u32) -> Self {
        let rate_per_sec = f64::from(rate_per_sec.max(1));
        Self {
            rate_per_sec,
            burst: rate_per_sec,
            state: Mutex::new(RateLimiterState {
                tokens: rate_per_sec,
                updated_at: Instant::now(),
            }),
        }
    }

    /// Reserves one call, returning how long the caller must wait before making
    /// it, or `None` when that wait would exceed `max_wait` (nothing is
    /// reserved then).
    pub fn reserve(&self, max_wait: Duration) -> Option<Duration> {
        self.reserve_at(Instant::now(), max_wait)
    }

    pub fn reserve_at(&self, now: Instant, max_wait: Duration) -> Option<Duration> {
        let mut state = self.state.lock();
        let elapsed = now.saturating_duration_since(state.updated_at).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.rate_per_sec).min(self.burst);
        state.updated_at = now;
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            return Some(Duration::ZERO);
        }
        let wait = Duration::from_secs_f64((1.0 - state.tokens) / self.rate_per_sec);
        if wait > max_wait {
            return None;
        }
        state.tokens -= 1.0;
        Some(wait)
    }
}

impl Default for SourceListRateLimiter {
    fn default() -> Self {
        Self::new(SOURCE_LIST_RATE_PER_SEC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// One `ListObjectsV2` page over a sorted key set, with the S3 rules the
    /// merge relies on: delimiter roll-up, `max_keys`, and a continuation
    /// token that resumes after the last entry the page returned.
    fn reference_page(
        keys: &[String],
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max_keys: usize,
    ) -> (Vec<ListEntryKey>, bool, Option<String>) {
        let mut entries: Vec<ListEntryKey> = Vec::new();
        for key in keys.iter().filter(|key| key.starts_with(prefix)) {
            let entry = match delimiter.and_then(|delimiter| key[prefix.len()..].find(delimiter).map(|at| (delimiter, at))) {
                Some((delimiter, at)) => ListEntryKey::prefix(&key[..prefix.len() + at + delimiter.len()]),
                None => ListEntryKey::object(key.clone()),
            };
            if entries.last().is_none_or(|last| last.name != entry.name) {
                entries.push(entry);
            }
        }
        if let Some(after) = after {
            entries.retain(|entry| entry.name.as_str() > after);
        }
        let truncated = entries.len() > max_keys;
        entries.truncate(max_keys);
        let next = truncated.then(|| entries.last().map(|entry| entry.name.clone())).flatten();
        (entries, truncated && next.is_some(), next)
    }

    /// Full pagination through the merger, returning every entry it emitted and
    /// the page sizes it produced.
    fn walk(
        local: &[String],
        source: &[String],
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> (Vec<(ListEntryKey, MergeSide)>, Vec<usize>) {
        let mut emitted = Vec::new();
        let mut page_sizes = Vec::new();
        let mut token: Option<ListThroughToken> = None;
        for _ in 0..10_000 {
            let mut merger = ListThroughMerger::new(max_keys, token.as_ref());
            let mut buffers = [Vec::<ListEntryKey>::new(), Vec::<ListEntryKey>::new()];
            while let Some(fetch) = merger.next_fetch() {
                let keys = match fetch.side {
                    MergeSide::Local => local,
                    MergeSide::Source => source,
                };
                let (entries, truncated, next) = reference_page(keys, prefix, delimiter, fetch.token.as_deref(), max_keys);
                let kept: Vec<ListEntryKey> = entries.into_iter().filter(|entry| merger.accepts(&entry.name)).collect();
                buffers[usize::from(fetch.side == MergeSide::Source)].extend(kept.iter().cloned());
                merger
                    .push_page(fetch.side, kept, truncated, next)
                    .expect("reference provider pages must advance");
            }
            let outcome = merger.finish();
            assert_eq!(outcome.is_truncated, outcome.next_token.is_some());
            if outcome.is_truncated {
                assert_ne!(outcome.next_token, token, "every truncated merged page must make progress");
            }
            page_sizes.push(outcome.picks.len());
            for pick in &outcome.picks {
                let entry = buffers[usize::from(pick.side == MergeSide::Source)][pick.index].clone();
                emitted.push((entry, pick.side));
            }
            if !outcome.is_truncated {
                return (emitted, page_sizes);
            }
            token = outcome.next_token;
        }
        panic!("merged pagination did not terminate");
    }

    fn expected(local: &[String], source: &[String], prefix: &str, delimiter: Option<&str>) -> Vec<ListEntryKey> {
        // This oracle builds the complete namespace independently of the
        // provider's page/marker helper and the production merger.
        let mut namespace = std::collections::BTreeMap::new();
        for key in local.iter().chain(source) {
            let Some(suffix) = key.strip_prefix(prefix) else {
                continue;
            };
            if let Some(delimiter) = delimiter.filter(|delimiter| !delimiter.is_empty())
                && let Some((directory, _)) = suffix.split_once(delimiter)
            {
                namespace.insert(format!("{prefix}{directory}{delimiter}"), true);
                continue;
            }
            namespace.insert(key.clone(), false);
        }
        namespace
            .into_iter()
            .map(|(name, is_prefix)| ListEntryKey { name, is_prefix })
            .collect()
    }

    #[test]
    fn reference_page_rolls_up_and_paginates() {
        let keys = vec!["a/1".to_string(), "a/2".to_string(), "b".to_string(), "c/1".to_string()];
        let (entries, truncated, next) = reference_page(&keys, "", Some("/"), None, 2);
        assert_eq!(entries, vec![ListEntryKey::prefix("a/"), ListEntryKey::object("b")]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b"));
    }

    #[test]
    fn merged_pages_are_ordered_and_local_wins_on_equal_keys() {
        let local = vec!["a".to_string(), "c".to_string()];
        let source = vec!["b".to_string(), "c".to_string(), "d".to_string()];
        let (emitted, sizes) = walk(&local, &source, "", None, 2);
        let names: Vec<&str> = emitted.iter().map(|(entry, _)| entry.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c", "d"]);
        assert_eq!(emitted[2].1, MergeSide::Local, "the shared key must come from local");
        assert!(sizes.iter().all(|size| *size <= 2), "{sizes:?}");
    }

    #[test]
    fn source_only_listing_paginates_without_a_local_side() {
        let source: Vec<String> = (0..7).map(|index| format!("k{index}")).collect();
        let (emitted, _) = walk(&[], &source, "", None, 3);
        assert_eq!(emitted.len(), 7);
        assert!(emitted.iter().all(|(_, side)| *side == MergeSide::Source));
    }

    #[test]
    fn a_disabled_source_answers_from_local_alone() {
        let mut merger = ListThroughMerger::new(10, None);
        merger.disable_source();
        assert_eq!(
            merger.next_fetch(),
            Some(FetchRequest {
                side: MergeSide::Local,
                token: None
            })
        );
        merger
            .push_page(MergeSide::Local, vec![ListEntryKey::object("a")], false, None)
            .expect("local EOF is valid");
        assert_eq!(merger.next_fetch(), None);
        let outcome = merger.finish();
        assert_eq!(outcome.picks.len(), 1);
        assert!(!outcome.is_truncated);
        assert!(outcome.next_token.is_none());
    }

    #[test]
    fn a_degraded_page_keeps_the_source_cursor_for_the_next_one() {
        let resume = ListThroughToken {
            t: LIST_THROUGH_TOKEN_TAG.to_string(),
            v: LIST_THROUGH_TOKEN_VERSION,
            local: Some("local-1".to_string()),
            local_done: false,
            source: Some("source-1".to_string()),
            source_done: false,
            last_key: Some("a".to_string()),
        };
        let mut merger = ListThroughMerger::new(1, Some(&resume));
        merger.disable_source();
        merger
            .push_page(
                MergeSide::Local,
                vec![ListEntryKey::object("b"), ListEntryKey::object("c")],
                true,
                Some("local-2".to_string()),
            )
            .expect("local cursor advances");
        let outcome = merger.finish();
        assert!(outcome.is_truncated);
        let token = outcome.next_token.expect("truncated page carries a token");
        assert_eq!(token.source.as_deref(), Some("source-1"), "the source cursor must not move");
        assert!(!token.source_done);
        assert_eq!(token.last_key.as_deref(), Some("b"));
        assert_eq!(token.local.as_deref(), Some("local-1"), "a partly read page is re-listed");
    }

    #[test]
    fn truncated_pages_require_a_nonempty_advancing_cursor() {
        for side in [MergeSide::Local, MergeSide::Source] {
            for entries in [vec![], vec![ListEntryKey::object("a")]] {
                for (next, expected) in [
                    (None, Err(ListPageError::Missing)),
                    (Some(""), Err(ListPageError::Empty)),
                    (Some("stuck"), Err(ListPageError::Repeated)),
                    (Some("advances"), Ok(())),
                ] {
                    let resume = ListThroughToken::new(
                        SideCursor {
                            token: Some("stuck".into()),
                            done: false,
                        },
                        SideCursor {
                            token: Some("stuck".into()),
                            done: false,
                        },
                        None,
                    );
                    let mut merger = ListThroughMerger::new(2, Some(&resume));
                    let result = merger.push_page(side, entries.clone(), true, next.map(str::to_string));
                    assert_eq!(result, expected, "{side:?}, {entries:?}, {next:?}");
                    let state = if side == MergeSide::Local {
                        &merger.local
                    } else {
                        &merger.source
                    };
                    assert_eq!(state.pages.len(), usize::from(result.is_ok()), "invalid page must not be accepted");
                }
            }
        }
    }

    #[test]
    fn repeated_empty_cursor_is_rejected_before_an_identical_page_can_escape() {
        let resume = ListThroughToken::new(
            SideCursor { token: None, done: true },
            SideCursor {
                token: Some("stuck".into()),
                done: false,
            },
            None,
        );
        let mut merger = ListThroughMerger::new(2, Some(&resume));
        assert_eq!(
            merger.next_fetch(),
            Some(FetchRequest {
                side: MergeSide::Source,
                token: Some("stuck".into())
            })
        );
        assert_eq!(
            merger.push_page(MergeSide::Source, vec![], true, Some("stuck".into())),
            Err(ListPageError::Repeated)
        );
    }

    #[test]
    fn empty_pages_may_advance_within_the_fetch_budget_until_eof() {
        let mut merger = ListThroughMerger::new(2, None);
        merger.push_page(MergeSide::Local, vec![], false, None).expect("local EOF");
        for next in ["opaque-z", "opaque-a"] {
            assert_eq!(merger.next_fetch().expect("bounded source fetch").side, MergeSide::Source);
            merger
                .push_page(MergeSide::Source, vec![], true, Some(next.into()))
                .expect("opaque cursor advances regardless of sort order");
        }
        assert!(merger.next_fetch().is_none(), "two source fetches exhaust the request budget");
        let outcome = merger.finish();
        assert!(outcome.picks.is_empty());
        assert!(outcome.is_truncated);
        let token = outcome.next_token.expect("empty progressing page has a cursor");
        assert_eq!(token.source.as_deref(), Some("opaque-a"));
        let mut merger = ListThroughMerger::new(2, Some(&token));
        assert_eq!(merger.next_fetch().expect("source resumes").token.as_deref(), Some("opaque-a"));
        merger
            .push_page(MergeSide::Source, vec![ListEntryKey::object("result")], false, None)
            .expect("source EOF");
        let outcome = merger.finish();
        assert_eq!(
            outcome.picks,
            vec![MergePick {
                side: MergeSide::Source,
                index: 0
            }]
        );
        assert!(!outcome.is_truncated);
        assert!(outcome.next_token.is_none());
    }

    #[test]
    fn a_cursor_cycle_inside_the_fetch_budget_is_rejected() {
        let resume = ListThroughToken::new(
            SideCursor { token: None, done: true },
            SideCursor {
                token: Some("first".into()),
                done: false,
            },
            None,
        );
        let mut merger = ListThroughMerger::new(2, Some(&resume));
        merger
            .push_page(MergeSide::Source, vec![], true, Some("second".into()))
            .expect("first page advances");
        assert_eq!(
            merger.push_page(MergeSide::Source, vec![], true, Some("first".into())),
            Err(ListPageError::Repeated)
        );
    }

    #[test]
    fn source_refill_failure_discards_buffered_source_entries_and_horizon() {
        let mut merger = ListThroughMerger::new(2, None);
        merger
            .push_page(MergeSide::Local, vec![ListEntryKey::object("z")], false, None)
            .expect("local EOF");
        merger
            .push_page(MergeSide::Source, vec![ListEntryKey::object("a")], true, Some("stuck".into()))
            .expect("first source page advances");
        assert_eq!(merger.next_fetch().expect("source refill is required").token.as_deref(), Some("stuck"));
        assert_eq!(
            merger.push_page(MergeSide::Source, vec![], true, Some("stuck".into())),
            Err(ListPageError::Repeated)
        );
        merger.disable_source();
        let outcome = merger.finish();
        assert_eq!(
            outcome.picks,
            vec![MergePick {
                side: MergeSide::Local,
                index: 0
            }]
        );
        assert!(!outcome.is_truncated);
        assert!(outcome.next_token.is_none());
    }

    #[test]
    fn list_through_static_namespace_boundary_matrix() {
        let corpus = [
            "a",
            "a/",
            "a/b",
            "a/b/child",
            "a0",
            "b",
            "b/leaf",
            "quote\"&<",
            "space key",
            "z",
            "é",
            "中/文",
        ];
        for count in [0, 1, 3, 4, corpus.len()] {
            let keys: Vec<String> = corpus[..count].iter().map(|key| (*key).to_string()).collect();
            for placement in 0..3 {
                let (local, source): (Vec<_>, Vec<_>) =
                    keys.iter()
                        .enumerate()
                        .fold((vec![], vec![]), |(mut local, mut source), (index, key)| {
                            if placement != 1 || index % 2 == 0 {
                                local.push(key.clone());
                            }
                            if placement != 0 || index % 2 == 0 {
                                source.push(key.clone());
                            }
                            (local, source)
                        });
                for prefix in ["", "a", "a/", "中/"] {
                    for delimiter in [None, Some("/")] {
                        for max_keys in [1, 3, 4] {
                            let oracle = expected(&local, &source, prefix, delimiter);
                            let (emitted, sizes) = walk(&local, &source, prefix, delimiter, max_keys);
                            assert_eq!(
                                emitted.iter().map(|(entry, _)| entry.clone()).collect::<Vec<_>>(),
                                oracle,
                                "count={count}, placement={placement}, prefix={prefix}, delimiter={delimiter:?}, max={max_keys}"
                            );
                            let expected_sizes: Vec<_> = if oracle.is_empty() {
                                vec![0]
                            } else {
                                oracle.chunks(max_keys).map(<[ListEntryKey]>::len).collect()
                            };
                            assert_eq!(sizes, expected_sizes, "exact max and max+1 boundaries must agree");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn list_through_large_overlap_walk_keeps_all_5300_keys() {
        let source: Vec<_> = (0..5000).map(|index| format!("k{index:05}")).collect();
        let local: Vec<_> = (4800..5300).map(|index| format!("k{index:05}")).collect();
        let (emitted, sizes) = walk(&local, &source, "", None, 333);
        assert_eq!(emitted.len(), 5300);
        for (index, (entry, side)) in emitted.iter().enumerate() {
            assert_eq!(entry.name, format!("k{index:05}"));
            assert_eq!(*side, if index >= 4800 { MergeSide::Local } else { MergeSide::Source });
        }
        assert_eq!(sizes, [vec![333; 15], vec![305]].concat());
    }

    #[test]
    fn token_round_trips_and_rejects_tampering() {
        let token = ListThroughToken::new(
            SideCursor {
                token: Some("l".to_string()),
                done: false,
            },
            SideCursor { token: None, done: true },
            Some("k".to_string()),
        );
        let encoded = token.encode();
        assert_eq!(decode_continuation_token(&encoded), Ok(ListThroughCursor::Merged(Box::new(token))));

        let bumped = encoded.replace("\"v\":1", "\"v\":2");
        assert_eq!(decode_continuation_token(&bumped), Err(ListThroughTokenError::UnsupportedVersion(2)));

        let extra = encoded.replace("{", "{\"x\":1,");
        assert_eq!(decode_continuation_token(&extra), Err(ListThroughTokenError::Malformed));

        let truncated = &encoded[..encoded.len() - 3];
        assert_eq!(decode_continuation_token(truncated), Err(ListThroughTokenError::Malformed));

        let no_version = "\0odm-list:{\"t\":\"odm-list\"}";
        assert_eq!(decode_continuation_token(no_version), Err(ListThroughTokenError::Malformed));
    }

    #[test]
    fn a_plain_local_marker_stays_local() {
        for marker in [
            r#"{"t":"odm-list","v":1}"#,
            r#"{"t":"odm-list","v":2,"local_done":true}"#,
            r#"{"t":"odm-list"}"#,
        ] {
            assert_eq!(decode_continuation_token(marker), Ok(ListThroughCursor::Local(marker.to_string())));
        }
        assert_eq!(
            decode_continuation_token("photos/2024/01.jpg"),
            Ok(ListThroughCursor::Local("photos/2024/01.jpg".to_string()))
        );
        assert_eq!(
            decode_continuation_token("{not json"),
            Ok(ListThroughCursor::Local("{not json".to_string()))
        );
        assert_eq!(
            decode_continuation_token("{\"t\":\"other\"}"),
            Ok(ListThroughCursor::Local("{\"t\":\"other\"}".to_string()))
        );
    }

    #[test]
    fn source_list_plan_intersects_the_filter_prefix() {
        assert_eq!(source_list_plan("", None, None), SourceListPlan::Page { prefix: String::new() });
        assert_eq!(
            source_list_plan("photos/2024/", Some("photos/"), None),
            SourceListPlan::Page {
                prefix: "photos/2024/".to_string()
            }
        );
        assert_eq!(
            source_list_plan("photos/", Some("photos/2024/"), None),
            SourceListPlan::Page {
                prefix: "photos/2024/".to_string()
            }
        );
        assert_eq!(source_list_plan("videos/", Some("photos/"), None), SourceListPlan::Skip);
        assert_eq!(
            source_list_plan("", Some("photos/2024/"), Some("/")),
            SourceListPlan::Folded {
                probe_prefix: "photos/2024/".to_string(),
                common_prefix: "photos/".to_string(),
            }
        );
        assert_eq!(
            source_list_plan("pho", Some("photos"), Some("/")),
            SourceListPlan::Page {
                prefix: "photos".to_string()
            },
            "a filter prefix that adds no delimiter keeps the source's own roll-up"
        );
    }

    #[test]
    fn rate_limiter_spends_its_burst_then_paces_and_refuses() {
        let limiter = SourceListRateLimiter::new(10);
        let start = Instant::now();
        for _ in 0..10 {
            assert_eq!(limiter.reserve_at(start, Duration::from_secs(1)), Some(Duration::ZERO));
        }
        let paced = limiter.reserve_at(start, Duration::from_secs(1)).expect("within the budget");
        assert!(paced > Duration::ZERO && paced <= Duration::from_millis(101), "{paced:?}");
        assert_eq!(limiter.reserve_at(start, Duration::ZERO), None, "a zero budget refuses");
        // A full second of refill restores the whole burst.
        assert_eq!(limiter.reserve_at(start + Duration::from_secs(5), Duration::ZERO), Some(Duration::ZERO));
    }

    fn key_set() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::btree_set(
            proptest::sample::select(vec!["a", "a/", "a/1", "a/2", "a/b/1", "b", "b/1", "c", "c/1", "c/2", "d", "d/e/f"])
                .prop_map(str::to_string),
            0..=12,
        )
        .prop_map(|set: BTreeSet<String>| set.into_iter().collect())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            rng_seed: proptest::test_runner::RngSeed::Fixed(0xec5706),
            ..ProptestConfig::with_cases(256)
        })]

        /// Full pagination of a merged listing equals the sorted, deduplicated
        /// union of both sides, with every shared key served by local, and no
        /// page longer than `max_keys`.
        #[test]
        fn merged_pagination_equals_the_deduplicated_union(
            local in key_set(),
            source in key_set(),
            max_keys in 1usize..=5,
            with_delimiter in any::<bool>(),
            prefix in proptest::sample::select(vec!["", "a", "a/", "c/"]),
        ) {
            let delimiter = with_delimiter.then_some("/");
            let (emitted, sizes) = walk(&local, &source, prefix, delimiter, max_keys);
            let got: Vec<ListEntryKey> = emitted.iter().map(|(entry, _)| entry.clone()).collect();
            prop_assert_eq!(got, expected(&local, &source, prefix, delimiter));
            prop_assert!(sizes.iter().all(|size| *size <= max_keys), "{:?}", sizes);
            for (entry, side) in &emitted {
                if !entry.is_prefix && local.iter().any(|key| key == &entry.name) {
                    prop_assert_eq!(*side, MergeSide::Local, "local must win for {}", entry.name);
                }
            }
        }
    }
}
