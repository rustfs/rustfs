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

use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    ptr,
    sync::{Arc, Mutex},
};

use arc_swap::{ArcSwap, Guard};
use rustfs_policy::{
    auth::UserIdentity,
    policy::{Args, PolicyDoc},
};
use time::OffsetDateTime;
use tracing::warn;

use crate::store::{GroupInfo, MappedPolicy};

/// Immutable IAM cache snapshot published atomically by [`Cache`].
///
/// Readers should load one `CacheState` and read all related maps from it. Writers
/// must go through `Cache`/`LockedCache` so multi-map updates publish as one state.
#[derive(Clone)]
pub struct CacheState {
    pub policy_docs: Arc<CacheEntity<PolicyDoc>>,
    pub users: Arc<CacheEntity<UserIdentity>>,
    pub user_policies: Arc<CacheEntity<MappedPolicy>>,
    pub sts_accounts: Arc<CacheEntity<UserIdentity>>,
    pub sts_policies: Arc<CacheEntity<MappedPolicy>>,
    pub groups: Arc<CacheEntity<GroupInfo>>,
    pub user_group_memberships: Arc<CacheEntity<HashSet<String>>>,
    pub group_policies: Arc<CacheEntity<MappedPolicy>>,
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            policy_docs: Arc::new(CacheEntity::default()),
            users: Arc::new(CacheEntity::default()),
            user_policies: Arc::new(CacheEntity::default()),
            sts_accounts: Arc::new(CacheEntity::default()),
            sts_policies: Arc::new(CacheEntity::default()),
            groups: Arc::new(CacheEntity::default()),
            user_group_memberships: Arc::new(CacheEntity::default()),
            group_policies: Arc::new(CacheEntity::default()),
        }
    }
}

pub struct Cache {
    state: ArcSwap<CacheState>,
    write_lock: Mutex<()>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            state: ArcSwap::new(Arc::new(CacheState::default())),
            write_lock: Mutex::new(()),
        }
    }
}

pub type CacheSnapshot = Guard<Arc<CacheState>>;

impl Cache {
    pub fn snapshot(&self) -> CacheSnapshot {
        self.state.load()
    }

    pub(crate) fn with_write_lock<R>(&self, f: impl FnOnce(&mut LockedCache) -> R) -> R {
        let _guard = self.write_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.state.load_full();
        let mut locked = LockedCache {
            state: CacheState::clone(&current),
            current_ptr: Arc::as_ptr(&current),
            dirty: false,
        };
        let ret = f(&mut locked);
        if locked.dirty {
            self.state.store(Arc::new(locked.state));
        }
        ret
    }

    pub fn build_user_group_memberships(&self) {
        self.with_write_lock(|cache| cache.build_user_group_memberships());
    }

    pub fn add_or_update_policy_doc(&self, key: &str, value: &PolicyDoc, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_policy_doc(key, value, t));
    }

    pub fn add_or_update_user(&self, key: &str, value: &UserIdentity, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_user(key, value, t));
    }

    pub fn add_or_update_user_policy(&self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_user_policy(key, value, t));
    }

    pub fn add_or_update_sts_account(&self, key: &str, value: &UserIdentity, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_sts_account(key, value, t));
    }

    pub fn add_or_update_sts_policy(&self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_sts_policy(key, value, t));
    }

    pub fn add_or_update_group(&self, key: &str, value: &GroupInfo, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_group(key, value, t));
    }

    pub fn add_or_update_user_group_membership(&self, key: &str, value: &HashSet<String>, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_user_group_membership(key, value, t));
    }

    pub fn add_or_update_group_policy(&self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.add_or_update_group_policy(key, value, t));
    }

    pub fn delete_policy_doc(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_policy_doc(key, t));
    }

    pub fn delete_user(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_user(key, t));
    }

    pub fn delete_user_policy(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_user_policy(key, t));
    }

    pub fn delete_sts_account(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_sts_account(key, t));
    }

    pub fn delete_sts_policy(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_sts_policy(key, t));
    }

    pub fn delete_group(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_group(key, t));
    }

    pub fn delete_group_policy(&self, key: &str, t: OffsetDateTime) {
        self.with_write_lock(|cache| cache.delete_group_policy(key, t));
    }
}

pub(crate) struct LockedCache {
    state: CacheState,
    current_ptr: *const CacheState,
    dirty: bool,
}

impl LockedCache {
    pub(crate) fn state(&self) -> &CacheState {
        &self.state
    }

    pub(crate) fn matches_snapshot(&self, snapshot: &CacheSnapshot) -> bool {
        ptr::eq(self.current_ptr, Arc::as_ptr(snapshot))
    }

    fn exec<T: Clone>(target: &mut Arc<CacheEntity<T>>, t: OffsetDateTime, mut op: impl FnMut(&mut CacheEntity<T>)) -> bool {
        if target.load_time >= t {
            return false;
        }

        let mut new = CacheEntity::clone(target);
        op(&mut new);
        *target = Arc::new(new);
        true
    }

    fn replaced<T>(value: CacheEntity<T>) -> Arc<CacheEntity<T>> {
        Arc::new(value.update_load_time())
    }

    pub(crate) fn replace_policy_docs(&mut self, value: CacheEntity<PolicyDoc>) {
        self.state.policy_docs = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_users(&mut self, value: CacheEntity<UserIdentity>) {
        self.state.users = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_user_policies(&mut self, value: CacheEntity<MappedPolicy>) {
        self.state.user_policies = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_sts_accounts(&mut self, value: CacheEntity<UserIdentity>) {
        self.state.sts_accounts = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_sts_policies(&mut self, value: CacheEntity<MappedPolicy>) {
        self.state.sts_policies = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_groups(&mut self, value: CacheEntity<GroupInfo>) {
        self.state.groups = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_group_policies(&mut self, value: CacheEntity<MappedPolicy>) {
        self.state.group_policies = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn replace_user_group_memberships(&mut self, value: CacheEntity<HashSet<String>>) {
        self.state.user_group_memberships = Self::replaced(value);
        self.dirty = true;
    }

    pub(crate) fn add_or_update_policy_doc(&mut self, key: &str, value: &PolicyDoc, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.policy_docs, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_user(&mut self, key: &str, value: &UserIdentity, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.users, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_user_policy(&mut self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.user_policies, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_sts_account(&mut self, key: &str, value: &UserIdentity, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.sts_accounts, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_sts_policy(&mut self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.sts_policies, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_group(&mut self, key: &str, value: &GroupInfo, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.groups, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_user_group_membership(&mut self, key: &str, value: &HashSet<String>, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.user_group_memberships, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn add_or_update_group_policy(&mut self, key: &str, value: &MappedPolicy, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.group_policies, t, |map| {
            map.insert(key.to_string(), value.clone());
        });
    }

    pub(crate) fn delete_policy_doc(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.policy_docs, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_user(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.users, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_user_policy(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.user_policies, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_sts_account(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.sts_accounts, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_sts_policy(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.sts_policies, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_group(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.groups, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn delete_group_policy(&mut self, key: &str, t: OffsetDateTime) {
        self.dirty |= Self::exec(&mut self.state.group_policies, t, |map| {
            map.remove(key);
        });
    }

    pub(crate) fn build_user_group_memberships(&mut self) {
        let groups = Arc::clone(&self.state.groups);
        let mut user_group_memberships = HashMap::new();
        for (group_name, group) in groups.iter() {
            for user_name in &group.members {
                user_group_memberships
                    .entry(user_name.clone())
                    .or_insert_with(HashSet::new)
                    .insert(group_name.clone());
            }
        }
        self.replace_user_group_memberships(CacheEntity::new(user_group_memberships));
    }
}

impl CacheInner {
    #[inline]
    pub fn get_user(&self, user_name: &str) -> Option<&UserIdentity> {
        self.snapshot
            .users
            .get(user_name)
            .or_else(|| self.snapshot.sts_accounts.get(user_name))
    }

    // todo
    pub fn is_allowed_sts(&self, _args: &Args, _parent: &str) -> bool {
        warn!("policy cache STS check path is not implemented");
        false
    }

    // todo
    pub fn is_allowed_service_account(&self, _args: &Args, _parent: &str) -> bool {
        warn!("policy cache service account check path is not implemented");
        false
    }

    pub fn is_allowed(&self, _args: Args) -> bool {
        warn!("policy cache is_allowed check path is currently denied by default");
        false
    }

    pub fn policy_db_get(&self, _name: &str, _groups: &[String]) -> Vec<String> {
        warn!("policy cache policy_db_get is not implemented, returning empty policy set");
        vec![]
    }
}

#[derive(Clone)]
pub struct CacheEntity<T> {
    map: HashMap<String, T>,
    /// The time of the reload
    load_time: OffsetDateTime,
}

impl<T> Deref for CacheEntity<T> {
    type Target = HashMap<String, T>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<T> DerefMut for CacheEntity<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<T> CacheEntity<T> {
    pub fn new(map: HashMap<String, T>) -> Self {
        Self {
            map,
            load_time: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl<T> Default for CacheEntity<T> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            load_time: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl<T> CacheEntity<T> {
    pub fn update_load_time(mut self) -> Self {
        self.load_time = OffsetDateTime::now_utc();
        self
    }
}

pub struct CacheInner {
    snapshot: CacheSnapshot,
}

impl From<&Cache> for CacheInner {
    fn from(value: &Cache) -> Self {
        Self {
            snapshot: value.snapshot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future::join_all;
    use rustfs_policy::auth::UserIdentity;
    use time::OffsetDateTime;

    use crate::cache::Cache;
    use crate::store::MappedPolicy;

    #[tokio::test]
    async fn test_cache_entity_add() {
        let owner = Arc::new(Cache::default());

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let owner = Arc::clone(&owner);
            f.push(async move {
                let user = UserIdentity {
                    version: index as i64,
                    ..Default::default()
                };
                owner.add_or_update_user(&key, &user, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache = owner.snapshot();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache.users.get(&key).map(|user| user.version), Some(index as i64));
        }
    }

    #[tokio::test]
    async fn test_cache_entity_update() {
        let owner = Arc::new(Cache::default());

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let owner = Arc::clone(&owner);
            f.push(async move {
                let user = UserIdentity {
                    version: index as i64,
                    ..Default::default()
                };
                owner.add_or_update_user(&key, &user, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = owner.snapshot();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.users.get(&key).map(|user| user.version), Some(index as i64));
        }

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let owner = Arc::clone(&owner);
            f.push(async move {
                let user = UserIdentity {
                    version: (index * 1000) as i64,
                    ..Default::default()
                };
                owner.add_or_update_user(&key, &user, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = owner.snapshot();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.users.get(&key).map(|user| user.version), Some((index * 1000) as i64));
        }
    }

    #[tokio::test]
    async fn test_cache_entity_delete() {
        let owner = Arc::new(Cache::default());

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let owner = Arc::clone(&owner);
            f.push(async move {
                let user = UserIdentity {
                    version: index as i64,
                    ..Default::default()
                };
                owner.add_or_update_user(&key, &user, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = owner.snapshot();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.users.get(&key).map(|user| user.version), Some(index as i64));
        }
        drop(cache_load);

        let mut f = vec![];

        for key in (0..100).map(|x| x.to_string()) {
            let owner = Arc::clone(&owner);
            f.push(async move {
                owner.delete_user(&key, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = owner.snapshot();
        assert!(cache_load.users.is_empty());
    }

    #[tokio::test]
    async fn test_cache_snapshot_reads_one_published_state() {
        let cache = Cache::default();
        let before = cache.snapshot();
        let user = UserIdentity {
            version: 7,
            ..Default::default()
        };
        let policy = MappedPolicy::new("readwrite");

        cache.with_write_lock(|cache| {
            let now = OffsetDateTime::now_utc();
            cache.add_or_update_user("snapshot-user", &user, now);
            cache.add_or_update_user_policy("snapshot-user", &policy, now);
        });

        assert!(!before.users.contains_key("snapshot-user"));
        assert!(!before.user_policies.contains_key("snapshot-user"));

        let after = cache.snapshot();
        assert!(after.users.contains_key("snapshot-user"));
        assert!(after.user_policies.contains_key("snapshot-user"));
    }
}
