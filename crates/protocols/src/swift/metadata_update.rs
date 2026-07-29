// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Account and container metadata updates
//!
//! Swift account and container POSTs are *additive*: an item the request does
//! not mention keeps its stored value, and removal is explicit — either
//! `X-Remove-{Account,Container}-Meta-{name}`, or the item sent with an empty
//! value. Object POST is the one that replaces the whole set; `swift::object`
//! handles that separately and must keep doing so.
//!
//! Getting this wrong is not a cosmetic divergence. Account metadata holds the
//! TempURL signing key, so a POST that set an unrelated item while dropping
//! the rest would invalidate every outstanding TempURL and FormPost signature
//! for the account.
//!
//! This module turns a request's headers into the items to write and the items
//! to drop, and applies that to the bucket tag set the metadata is persisted
//! in.

use super::{MAX_METADATA_COUNT, MAX_METADATA_VALUE_SIZE, SwiftError, SwiftResult};
use axum::http::HeaderMap;
use s3s::dto::{Tag, Tagging};
use std::collections::{BTreeMap, BTreeSet};

/// Request header prefix carrying an account metadata item.
const ACCOUNT_META_HEADER_PREFIX: &str = "x-account-meta-";
/// Request header prefix removing an account metadata item.
const ACCOUNT_META_REMOVE_HEADER_PREFIX: &str = "x-remove-account-meta-";
/// Request header prefix carrying a container metadata item.
const CONTAINER_META_HEADER_PREFIX: &str = "x-container-meta-";
/// Request header prefix removing a container metadata item.
const CONTAINER_META_REMOVE_HEADER_PREFIX: &str = "x-remove-container-meta-";

/// Bucket-tag namespace holding account metadata items.
pub(crate) const ACCOUNT_META_TAG_PREFIX: &str = "swift-account-meta-";
/// Bucket-tag namespace holding container metadata items.
///
/// Deliberately narrower than the `swift-` tags around it: the container ACL
/// (`swift-acl-*`) and versioning (`swift-versions-location`) tags share this
/// tag set and must survive a metadata POST.
pub(crate) const CONTAINER_META_TAG_PREFIX: &str = "swift-meta-";

/// The metadata changes carried by one account or container POST.
///
/// Item names are held lowercased. Swift metadata names are case-insensitive,
/// HTTP header names arrive lowercased anyway, and a removal has to match the
/// name a previous POST stored — so normalizing once here is what makes
/// `X-Remove-Container-Meta-Color` find a stored `color`.
///
/// Ordered rather than hashed so the persisted tag set — and therefore the
/// serialized XML — comes out in a stable order for a given update.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataUpdate {
    /// Items to write, by name.
    items: BTreeMap<String, String>,
    /// Names to drop.
    removals: BTreeSet<String>,
}

impl MetadataUpdate {
    /// Write `name` = `value`.
    pub fn set(mut self, name: &str, value: &str) -> Self {
        let name = name.to_lowercase();
        self.removals.remove(&name);
        self.items.insert(name, value.to_string());
        self
    }

    /// Drop `name`, if it is stored.
    pub fn remove(mut self, name: &str) -> Self {
        let name = name.to_lowercase();
        self.items.remove(&name);
        self.removals.insert(name);
        self
    }

    /// Parse the metadata headers of an account POST.
    pub(crate) fn from_account_headers(headers: &HeaderMap) -> Self {
        Self::from_headers(headers, ACCOUNT_META_HEADER_PREFIX, ACCOUNT_META_REMOVE_HEADER_PREFIX)
    }

    /// Parse the metadata headers of a container POST.
    pub(crate) fn from_container_headers(headers: &HeaderMap) -> Self {
        Self::from_headers(headers, CONTAINER_META_HEADER_PREFIX, CONTAINER_META_REMOVE_HEADER_PREFIX)
    }

    /// Parse metadata headers under `item_prefix`, and removals under
    /// `remove_prefix`. Both prefixes are lowercase, matching how `http`
    /// normalizes header names.
    ///
    /// An item sent with an empty value is a removal — the deletion path
    /// Swift clients use when they do not send a dedicated remove header.
    /// A name carried by both header forms is removed: the explicit removal
    /// wins, as it does in Swift, where a remove header is rewritten into an
    /// empty-valued item header.
    fn from_headers(headers: &HeaderMap, item_prefix: &str, remove_prefix: &str) -> Self {
        let mut update = Self::default();

        for (name, value) in headers {
            let Some(item) = name.as_str().strip_prefix(item_prefix) else {
                continue;
            };
            // A value that is not valid UTF-8 cannot be stored as a tag.
            // Skipping it matches how the rest of the Swift handlers treat
            // unreadable header values.
            let Ok(value) = value.to_str() else {
                continue;
            };

            update = if value.is_empty() {
                update.remove(item)
            } else {
                update.set(item, value)
            };
        }

        for name in headers.keys() {
            if let Some(item) = name.as_str().strip_prefix(remove_prefix) {
                update = update.remove(item);
            }
        }

        update
    }

    /// Whether this update changes anything.
    ///
    /// An ACL-only or versioning-only container POST produces an empty update:
    /// it names no metadata item, so it must leave stored metadata alone.
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty() && self.removals.is_empty()
    }

    /// Reject oversized values before anything is persisted.
    ///
    /// The item *count* is not checked here — an additive POST has to be
    /// measured against the merged result, which only [`Self::apply_to_tags`]
    /// can see.
    pub(crate) fn validate(&self) -> SwiftResult<()> {
        for (name, value) in &self.items {
            if value.len() > MAX_METADATA_VALUE_SIZE {
                return Err(SwiftError::BadRequest(format!(
                    "Metadata value for '{}' too large: {} bytes (max: {} bytes)",
                    name,
                    value.len(),
                    MAX_METADATA_VALUE_SIZE
                )));
            }
        }

        Ok(())
    }

    /// Merge this update into the persisted tag set.
    ///
    /// `prefix` is the tag namespace holding the items. Tags outside it — the
    /// container ACL and versioning tags, plus any S3 tags the bucket carries
    /// — are untouched, and so are the namespaced items this update does not
    /// name.
    ///
    /// The item-count cap is checked against the merged result rather than the
    /// request: because POSTs are additive, a client could otherwise walk past
    /// it one header at a time. This runs inside the bucket metadata write
    /// guard, so the count it checks is the one about to be persisted.
    pub(crate) fn apply_to_tags(&self, current: Option<&Tagging>, prefix: &str) -> SwiftResult<Tagging> {
        let mut tagging = current.cloned().unwrap_or_else(|| Tagging { tag_set: vec![] });

        tagging.tag_set.retain(|tag| match item_name(tag, prefix) {
            Some(name) => !self.items.contains_key(name) && !self.removals.contains(name),
            None => true,
        });

        let merged = tagging.tag_set.iter().filter(|tag| item_name(tag, prefix).is_some()).count() + self.items.len();
        if merged > MAX_METADATA_COUNT {
            return Err(SwiftError::BadRequest(format!(
                "Too many metadata headers: {} (max: {})",
                merged, MAX_METADATA_COUNT
            )));
        }

        for (name, value) in &self.items {
            tagging.tag_set.push(Tag {
                key: Some(format!("{}{}", prefix, name)),
                value: Some(value.clone()),
            });
        }

        Ok(tagging)
    }
}

/// The metadata item name a tag carries, or `None` if the tag does not belong
/// to this namespace.
fn item_name<'a>(tag: &'a Tag, prefix: &str) -> Option<&'a str> {
    tag.key.as_deref()?.strip_prefix(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("test header name should parse"),
                HeaderValue::from_str(value).expect("test header value should parse"),
            );
        }
        map
    }

    fn tags(pairs: &[(&str, &str)]) -> Tagging {
        Tagging {
            tag_set: pairs
                .iter()
                .map(|(key, value)| Tag {
                    key: Some((*key).to_string()),
                    value: Some((*value).to_string()),
                })
                .collect(),
        }
    }

    fn tag_value<'a>(tagging: &'a Tagging, key: &str) -> Option<&'a str> {
        tagging
            .tag_set
            .iter()
            .find(|tag| tag.key.as_deref() == Some(key))
            .and_then(|tag| tag.value.as_deref())
    }

    #[test]
    fn from_headers_collects_items_and_ignores_unrelated_headers() {
        let update = MetadataUpdate::from_container_headers(&headers(&[
            ("x-container-meta-color", "blue"),
            ("x-container-read", ".r:*"),
            ("content-type", "text/plain"),
        ]));

        assert_eq!(update, MetadataUpdate::default().set("color", "blue"));
    }

    #[test]
    fn from_headers_lowercases_item_names() {
        // `http` normalizes header names on the way in, so the mixed case a
        // client sends is already gone by the time a handler sees it; the
        // lowercasing here is what makes a directly built update match.
        let update = MetadataUpdate::from_container_headers(&headers(&[("X-Container-Meta-Color", "Blue")]));

        assert_eq!(update, MetadataUpdate::default().set("COLOR", "Blue"));
    }

    #[test]
    fn empty_value_is_a_removal() {
        let update = MetadataUpdate::from_container_headers(&headers(&[("x-container-meta-color", "")]));

        assert_eq!(update, MetadataUpdate::default().remove("color"));
    }

    #[test]
    fn remove_header_drops_the_item() {
        let update = MetadataUpdate::from_account_headers(&headers(&[("x-remove-account-meta-temp-url-key", "x")]));

        assert_eq!(update, MetadataUpdate::default().remove("temp-url-key"));
    }

    #[test]
    fn remove_header_wins_over_a_value_for_the_same_item() {
        let update = MetadataUpdate::from_container_headers(&headers(&[
            ("x-container-meta-color", "blue"),
            ("x-remove-container-meta-color", "x"),
        ]));

        assert_eq!(update, MetadataUpdate::default().remove("color"));
    }

    #[test]
    fn a_removal_header_is_not_mistaken_for_an_item() {
        // "x-remove-container-meta-color" must not also parse as the item
        // "remove-container-meta-color" or similar.
        let update = MetadataUpdate::from_container_headers(&headers(&[("x-remove-container-meta-color", "x")]));

        assert!(update.items.is_empty(), "a removal header must not set an item");
    }

    #[test]
    fn a_request_with_no_metadata_headers_is_empty() {
        assert!(MetadataUpdate::from_container_headers(&headers(&[("x-container-read", ".r:*")])).is_empty());
        assert!(MetadataUpdate::default().is_empty());
        assert!(!MetadataUpdate::default().remove("color").is_empty());
    }

    #[test]
    fn apply_preserves_items_the_update_does_not_name() {
        let current = tags(&[
            ("swift-meta-color", "blue"),
            ("swift-meta-season", "summer"),
            ("swift-acl-read", ".r:*"),
            ("swift-versions-location", "archive"),
            ("unrelated-s3-tag", "keep"),
        ]);

        let merged = MetadataUpdate::default()
            .set("mood", "calm")
            .apply_to_tags(Some(&current), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert_eq!(tag_value(&merged, "swift-meta-color"), Some("blue"));
        assert_eq!(tag_value(&merged, "swift-meta-season"), Some("summer"));
        assert_eq!(tag_value(&merged, "swift-meta-mood"), Some("calm"));
        assert_eq!(tag_value(&merged, "swift-acl-read"), Some(".r:*"));
        assert_eq!(tag_value(&merged, "swift-versions-location"), Some("archive"));
        assert_eq!(tag_value(&merged, "unrelated-s3-tag"), Some("keep"));
    }

    #[test]
    fn apply_overwrites_a_named_item_exactly_once() {
        let current = tags(&[("swift-meta-color", "blue")]);

        let merged = MetadataUpdate::default()
            .set("color", "red")
            .apply_to_tags(Some(&current), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert_eq!(merged.tag_set.len(), 1, "overwriting must not duplicate the tag");
        assert_eq!(tag_value(&merged, "swift-meta-color"), Some("red"));
    }

    #[test]
    fn apply_drops_only_the_removed_item() {
        let current = tags(&[
            ("swift-meta-color", "blue"),
            ("swift-meta-season", "summer"),
            ("swift-acl-read", ".r:*"),
        ]);

        let merged = MetadataUpdate::default()
            .remove("color")
            .apply_to_tags(Some(&current), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert_eq!(tag_value(&merged, "swift-meta-color"), None);
        assert_eq!(tag_value(&merged, "swift-meta-season"), Some("summer"));
        assert_eq!(tag_value(&merged, "swift-acl-read"), Some(".r:*"));
    }

    #[test]
    fn apply_to_an_untagged_bucket_starts_from_nothing() {
        let merged = MetadataUpdate::default()
            .set("color", "blue")
            .apply_to_tags(None, ACCOUNT_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert_eq!(tag_value(&merged, "swift-account-meta-color"), Some("blue"));
    }

    #[test]
    fn apply_caps_the_merged_item_count_not_the_request() {
        let current = Tagging {
            tag_set: (0..MAX_METADATA_COUNT)
                .map(|i| Tag {
                    key: Some(format!("{}item{}", CONTAINER_META_TAG_PREFIX, i)),
                    value: Some("v".to_string()),
                })
                .collect(),
        };

        // One more item than the container already stores: rejected, even
        // though the request itself carries a single header.
        let err = MetadataUpdate::default()
            .set("overflow", "v")
            .apply_to_tags(Some(&current), CONTAINER_META_TAG_PREFIX)
            .expect_err("exceeding the item cap must be rejected");
        assert!(matches!(err, SwiftError::BadRequest(_)), "expected BadRequest, got {err:?}");

        // Overwriting an item already counted stays at the cap.
        MetadataUpdate::default()
            .set("item0", "v2")
            .apply_to_tags(Some(&current), CONTAINER_META_TAG_PREFIX)
            .expect("overwriting an existing item must not trip the cap");
    }

    #[test]
    fn validate_rejects_oversized_values() {
        let err = MetadataUpdate::default()
            .set("color", &"b".repeat(MAX_METADATA_VALUE_SIZE + 1))
            .validate()
            .expect_err("an oversized value must be rejected");
        assert!(matches!(err, SwiftError::BadRequest(_)), "expected BadRequest, got {err:?}");

        MetadataUpdate::default()
            .set("color", &"b".repeat(MAX_METADATA_VALUE_SIZE))
            .validate()
            .expect("a value at the limit must be accepted");
    }
}
