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

//! ListObjects for engines: the query, the page, and the S3 delimiter
//! rules shared by every engine.

use crate::engine::EngineObjectInfo;
use std::collections::BTreeMap;

/// One ListObjects page request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListQuery<'a> {
    pub prefix: &'a str,
    /// With a delimiter, keys that contain it after `prefix` are listed once
    /// as their common prefix instead of individually.
    pub delimiter: Option<&'a str>,
    /// List only names (keys or common prefixes) after this one.
    pub start_after: Option<&'a str>,
    /// Most objects and common prefixes, together, in one page.
    pub max_keys: usize,
}

impl ListQuery<'_> {
    /// The name `key` is listed under: its common prefix when the delimiter
    /// occurs after the query prefix (the prefix plus everything up to and
    /// including the first delimiter), else the key itself. `None` when the
    /// key is outside the prefix.
    pub fn name_of<'k>(&self, key: &'k str) -> Option<ListName<'k>> {
        let rest = key.strip_prefix(self.prefix)?;
        let common = self
            .delimiter
            .filter(|delimiter| !delimiter.is_empty())
            .and_then(|delimiter| rest.find(delimiter).map(|at| self.prefix.len() + at + delimiter.len()));
        Some(match common {
            Some(end) => ListName::Prefix(&key[..end]),
            None => ListName::Key(key),
        })
    }

    /// Whether a listed name comes after the query's start.
    pub fn is_after_start(&self, name: &str) -> bool {
        self.start_after.is_none_or(|after| name > after)
    }
}

/// What a key is listed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListName<'k> {
    Key(&'k str),
    Prefix(&'k str),
}

impl<'k> ListName<'k> {
    pub fn as_str(&self) -> &'k str {
        match self {
            Self::Key(name) | Self::Prefix(name) => name,
        }
    }
}

/// One ListObjects page: objects and common prefixes, each in key order,
/// at most the query's `max_keys` together.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListPage {
    pub objects: Vec<EngineObjectInfo>,
    pub common_prefixes: Vec<String>,
    /// Set when more names follow: the last name of this page, to pass as
    /// the next query's `start_after`.
    pub next_start_after: Option<String>,
}

/// Collects the first `max_keys` names of a listing from any number of
/// sources (drives, nodes) in any order, without duplicates.
#[derive(Debug)]
pub struct ListPageBuilder {
    max_keys: usize,
    names: BTreeMap<String, Option<EngineObjectInfo>>,
    truncated: bool,
}

impl ListPageBuilder {
    pub fn new(max_keys: usize) -> Self {
        Self {
            max_keys,
            names: BTreeMap::new(),
            truncated: false,
        }
    }

    pub fn push_object(&mut self, info: EngineObjectInfo) {
        self.push(info.key.clone(), Some(info));
    }

    pub fn push_prefix(&mut self, prefix: String) {
        self.push(prefix, None);
    }

    /// A source had more names than it returned.
    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn push(&mut self, name: String, object: Option<EngineObjectInfo>) {
        if self.max_keys == 0 {
            self.truncated = true;
            return;
        }
        self.names.entry(name).or_insert(object);
        if self.names.len() > self.max_keys {
            self.names.pop_last();
            self.truncated = true;
        }
    }

    pub fn finish(self) -> ListPage {
        let next_start_after = if self.truncated {
            self.names.last_key_value().map(|(name, _)| name.clone())
        } else {
            None
        };
        let mut page = ListPage {
            next_start_after,
            ..ListPage::default()
        };
        for (name, object) in self.names {
            match object {
                Some(info) => page.objects.push(info),
                None => page.common_prefixes.push(name),
            }
        }
        page
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(key: &str) -> EngineObjectInfo {
        EngineObjectInfo {
            key: key.to_owned(),
            size: 1,
            etag: "e".to_owned(),
            last_modified_unix_secs: 0,
        }
    }

    #[test]
    fn keys_roll_up_to_the_first_delimiter_after_the_prefix() {
        let query = ListQuery {
            prefix: "hicache/",
            delimiter: Some("/"),
            ..ListQuery::default()
        };
        assert_eq!(query.name_of("hicache/kimi/page"), Some(ListName::Prefix("hicache/kimi/")));
        assert_eq!(query.name_of("hicache/page"), Some(ListName::Key("hicache/page")));
        assert_eq!(query.name_of("hicache/dir/"), Some(ListName::Prefix("hicache/dir/")));
        assert_eq!(query.name_of("other/page"), None);

        let flat = ListQuery {
            prefix: "hicache/",
            ..ListQuery::default()
        };
        assert_eq!(flat.name_of("hicache/kimi/page"), Some(ListName::Key("hicache/kimi/page")));
        let empty_delimiter = ListQuery {
            delimiter: Some(""),
            ..ListQuery::default()
        };
        assert_eq!(empty_delimiter.name_of("a/b"), Some(ListName::Key("a/b")));
        let multi = ListQuery {
            delimiter: Some("::"),
            ..ListQuery::default()
        };
        assert_eq!(multi.name_of("a::b::c"), Some(ListName::Prefix("a::")));
    }

    #[test]
    fn a_prefix_given_as_start_is_skipped_whole() {
        let query = ListQuery {
            delimiter: Some("/"),
            start_after: Some("a/"),
            ..ListQuery::default()
        };
        let name = query.name_of("a/x").expect("in prefix");
        assert!(!query.is_after_start(name.as_str()));
        assert!(query.is_after_start(query.name_of("a0").expect("in prefix").as_str()));
    }

    #[test]
    fn pages_merge_sources_in_order_without_duplicates() {
        let mut page = ListPageBuilder::new(3);
        page.push_prefix("b/".to_owned());
        page.push_object(info("c"));
        page.push_prefix("b/".to_owned());
        page.push_object(info("a"));
        page.push_object(info("d"));
        let page = page.finish();
        assert_eq!(page.objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(), ["a", "c"]);
        assert_eq!(page.common_prefixes, ["b/"]);
        assert_eq!(page.next_start_after.as_deref(), Some("c"));

        let mut complete = ListPageBuilder::new(3);
        complete.push_object(info("a"));
        assert_eq!(complete.finish().next_start_after, None);

        let mut more_elsewhere = ListPageBuilder::new(3);
        more_elsewhere.push_object(info("a"));
        more_elsewhere.mark_truncated();
        assert_eq!(more_elsewhere.finish().next_start_after.as_deref(), Some("a"));
    }
}
