use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    ptr,
    sync::Arc,
};

use arc_swap::{ArcSwap, AsRaw, Guard};
use log::warn;
use time::OffsetDateTime;

use crate::{
    auth::UserIdentity,
    policy::{Args, MappedPolicy, Policy, PolicyDoc},
    Error,
};

pub struct Cache {
    pub policy_docs: ArcSwap<CacheEntity<PolicyDoc>>,
    pub users: ArcSwap<CacheEntity<UserIdentity>>,
    pub user_policies: ArcSwap<CacheEntity<MappedPolicy>>,
    pub sts_accounts: ArcSwap<CacheEntity<UserIdentity>>,
    pub sts_policies: ArcSwap<CacheEntity<MappedPolicy>>,
    pub groups: ArcSwap<CacheEntity<String>>,
    pub user_group_memeberships: ArcSwap<CacheEntity<HashSet<String>>>,
    pub group_policies: ArcSwap<CacheEntity<MappedPolicy>>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            policy_docs: ArcSwap::new(Arc::new(CacheEntity::default())),
            users: ArcSwap::new(Arc::new(CacheEntity::default())),
            user_policies: ArcSwap::new(Arc::new(CacheEntity::default())),
            sts_accounts: ArcSwap::new(Arc::new(CacheEntity::default())),
            sts_policies: ArcSwap::new(Arc::new(CacheEntity::default())),
            groups: ArcSwap::new(Arc::new(CacheEntity::default())),
            user_group_memeberships: ArcSwap::new(Arc::new(CacheEntity::default())),
            group_policies: ArcSwap::new(Arc::new(CacheEntity::default())),
        }
    }
}

impl Cache {
    pub fn ptr_eq<Base, A, B>(a: A, b: B) -> bool
    where
        A: AsRaw<Base>,
        B: AsRaw<Base>,
    {
        let a = a.as_raw();
        let b = b.as_raw();
        ptr::eq(a, b)
    }

    fn exec<T: Clone>(target: &ArcSwap<CacheEntity<T>>, t: OffsetDateTime, mut op: impl FnMut(&mut CacheEntity<T>)) {
        let mut cur = target.load();
        loop {
            // 当前的更新时间晚于执行时间，说明后台任务加载完毕，不需要执行当前操作。
            if cur.load_time >= t {
                return;
            }

            let mut new = CacheEntity::clone(&cur);
            op(&mut new);

            // 使用cas原子替换内容
            let prev = target.compare_and_swap(&*cur, Arc::new(new));
            let swapped = Self::ptr_eq(&*cur, &*prev);
            if swapped {
                return;
            } else {
                cur = prev;
            }
        }
    }

    pub fn add_or_update<T: Clone>(target: &ArcSwap<CacheEntity<T>>, key: &str, value: &T, t: OffsetDateTime) {
        Self::exec(target, t, |map: &mut CacheEntity<T>| {
            map.insert(key.to_string(), value.clone());
        })
    }

    pub fn delete<T: Clone>(target: &ArcSwap<CacheEntity<T>>, key: &str, t: OffsetDateTime) {
        Self::exec(target, t, |map: &mut CacheEntity<T>| {
            map.remove(key);
        })
    }
}

impl CacheInner {
    #[inline]
    fn get_user<'a>(&self, user_name: &'a str) -> Option<&UserIdentity> {
        self.users.get(user_name).or_else(|| self.sts_accounts.get(user_name))
    }

    fn get_policy(&self, name: &str, groups: &[String]) -> crate::Result<Vec<Policy>> {
        todo!()
    }

    /// 如果是临时用户，返回Ok(Some(partent_name)))
    /// 如果不是临时用户，返回Ok(None)
    fn is_temp_user<'a>(&self, user_name: &'a str) -> crate::Result<Option<&str>> {
        let user = self
            .get_user(user_name)
            .ok_or_else(|| Error::NoSuchUser(user_name.to_owned()))?;

        if user.credentials.is_temp() {
            Ok(Some(&user.credentials.parent_user))
        } else {
            Ok(None)
        }
    }

    /// 如果是临时用户，返回Ok(Some(partent_name)))
    /// 如果不是临时用户，返回Ok(None)
    fn is_service_account<'a>(&self, user_name: &'a str) -> crate::Result<Option<&str>> {
        let user = self
            .get_user(user_name)
            .ok_or_else(|| Error::NoSuchUser(user_name.to_owned()))?;

        if user.credentials.is_service_account() {
            Ok(Some(&user.credentials.parent_user))
        } else {
            Ok(None)
        }
    }

    // todo
    pub fn is_allowed_sts(&self, args: &Args, parent: &str) -> bool {
        warn!("unimplement is_allowed_sts");
        false
    }

    // todo
    pub fn is_allowed_service_account(&self, args: &Args, parent: &str) -> bool {
        warn!("unimplement is_allowed_sts");
        false
    }

    pub fn is_allowed(&self, args: Args) -> bool {
        todo!()
    }

    pub fn policy_db_get(&self, name: &str, groups: &[String]) -> Vec<String> {
        todo!()
    }
}

#[derive(Clone)]
pub struct CacheEntity<T> {
    map: HashMap<String, T>,
    /// 重新加载的时间
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

pub type G<T> = Guard<Arc<CacheEntity<T>>>;

pub struct CacheInner {
    pub policy_docs: G<PolicyDoc>,
    pub users: G<UserIdentity>,
    pub user_policies: G<MappedPolicy>,
    pub sts_accounts: G<UserIdentity>,
    pub sts_policies: G<MappedPolicy>,
    pub groups: G<String>,
    pub user_group_memeberships: G<HashSet<String>>,
    pub group_policies: G<MappedPolicy>,
}

impl From<&Cache> for CacheInner {
    fn from(value: &Cache) -> Self {
        Self {
            policy_docs: value.policy_docs.load(),
            users: value.users.load(),
            user_policies: value.user_policies.load(),
            sts_accounts: value.sts_accounts.load(),
            sts_policies: value.sts_policies.load(),
            groups: value.groups.load(),
            user_group_memeberships: value.user_group_memeberships.load(),
            group_policies: value.group_policies.load(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use futures::future::join_all;
    use time::OffsetDateTime;

    use super::CacheEntity;
    use crate::cache::Cache;

    #[tokio::test]
    async fn test_cache_entity_add() {
        let cache = ArcSwap::new(Arc::new(CacheEntity::<usize>::default()));

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let c = &cache;
            f.push(async move {
                Cache::add_or_update(&c, &key, &index, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache = cache.load();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache.get(&key), Some(&index));
        }
    }

    #[tokio::test]
    async fn test_cache_entity_update() {
        let cache = ArcSwap::new(Arc::new(CacheEntity::<usize>::default()));

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let c = &cache;
            f.push(async move {
                Cache::add_or_update(&c, &key, &index, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = cache.load();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.get(&key), Some(&index));
        }

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let c = &cache;
            f.push(async move {
                Cache::add_or_update(&c, &key, &(index * 1000), OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = cache.load();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.get(&key), Some(&(index * 1000)));
        }
    }

    #[tokio::test]
    async fn test_cache_entity_delete() {
        let cache = ArcSwap::new(Arc::new(CacheEntity::<usize>::default()));

        let mut f = vec![];

        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            let c = &cache;
            f.push(async move {
                Cache::add_or_update(&c, &key, &index, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = cache.load();
        for (index, key) in (0..100).map(|x| x.to_string()).enumerate() {
            assert_eq!(cache_load.get(&key), Some(&index));
        }

        let mut f = vec![];

        for key in (0..100).map(|x| x.to_string()) {
            let c = &cache;
            f.push(async move {
                Cache::delete(&c, &key, OffsetDateTime::now_utc());
            });
        }
        join_all(f).await;

        let cache_load = cache.load();
        assert!(cache_load.is_empty());
    }
}
