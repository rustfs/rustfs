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

use crate::key::ObjectDataCacheKey;
use crate::starshard_index::StarshardIdentityIndex;

/// Result of inserting a cache key into the identity index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDataCacheIndexInsertResult {
    /// The key was inserted into the identity set.
    Inserted,
    /// The key was already tracked for the identity.
    Duplicate,
    /// The identity exceeded its configured key budget and was conservatively cleared.
    Overflow {
        /// Keys removed while clearing the identity.
        cleared_keys: Vec<ObjectDataCacheKey>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectDataCacheKeySet {
    keys: Vec<ObjectDataCacheKey>,
}

impl ObjectDataCacheKeySet {
    pub(crate) fn insert(&mut self, key: ObjectDataCacheKey, max_keys: usize) -> ObjectDataCacheIndexInsertResult {
        if self.keys.iter().any(|existing| existing == &key) {
            return ObjectDataCacheIndexInsertResult::Duplicate;
        }

        if self.keys.len() >= max_keys {
            let cleared_keys = self.drain();
            return ObjectDataCacheIndexInsertResult::Overflow { cleared_keys };
        }

        self.keys.push(key);
        ObjectDataCacheIndexInsertResult::Inserted
    }

    pub(crate) fn remove_key(&mut self, key: &ObjectDataCacheKey) -> bool {
        let original_len = self.keys.len();
        self.keys.retain(|existing| existing != key);
        original_len != self.keys.len()
    }

    pub(crate) fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&ObjectDataCacheKey) -> bool,
    {
        self.keys.retain(|key| keep(key));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn cloned(&self) -> Vec<ObjectDataCacheKey> {
        self.keys.clone()
    }

    pub(crate) fn drain(&mut self) -> Vec<ObjectDataCacheKey> {
        std::mem::take(&mut self.keys)
    }
}

/// Public identity-index façade used by the cache backend.
pub type ObjectDataCacheIdentityIndex = StarshardIdentityIndex;

#[cfg(test)]
mod tests {
    use super::{ObjectDataCacheIndexInsertResult, ObjectDataCacheKeySet};
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};

    fn make_key(id: &str) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", Some(id), "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[test]
    fn key_set_deduplicates_existing_key() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let first = set.insert(key.clone(), 4);
        let second = set.insert(key, 4);

        assert_eq!(first, ObjectDataCacheIndexInsertResult::Inserted);
        assert_eq!(second, ObjectDataCacheIndexInsertResult::Duplicate);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn key_set_overflow_clears_existing_keys() {
        let mut set = ObjectDataCacheKeySet::default();
        let key_a = make_key("v1");
        let key_b = make_key("v2");

        let _ = set.insert(key_a.clone(), 1);
        let result = set.insert(key_b, 1);

        assert!(matches!(
            result,
            ObjectDataCacheIndexInsertResult::Overflow { cleared_keys } if cleared_keys == vec![key_a]
        ));
        assert!(set.is_empty());
    }
}
