use std::{collections::HashMap, path::Path, sync::Arc};

use ecstore::{
    config::error::is_not_found,
    store::{ECStore, ListPathOptions},
    store_api::{HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader},
    utils::path::dir,
};
use futures::future::try_join_all;
use log::{debug, warn};
use serde::{de::DeserializeOwned, Serialize};
use tracing::error;

use super::Store;
use crate::{
    auth::UserIdentity,
    cache::{Cache, CacheEntity},
    policy::{utils::split_path, MappedPolicy, PolicyDoc, UserType},
    Error,
};

#[derive(Clone)]
pub struct ObjectStore {
    object_api: Arc<ECStore>,
}

impl ObjectStore {
    const BUCKET_NAME: &str = ".rustfs.sys";

    pub fn new(object_api: Arc<ECStore>) -> Self {
        Self { object_api }
    }

    async fn list_iam_config_items(&self, prefix: &str, items: &[&str]) -> crate::Result<Vec<String>> {
        debug!("list iam config items, prefix: {prefix}");

        // todo, 实现walk，使用walk
        let mut futures = Vec::with_capacity(items.len());

        for item in items {
            let prefix = format!("{}{}", prefix, item);
            futures.push(async move {
                let items = self
                    .object_api
                    .list_path(
                        &ListPathOptions {
                            bucket: Self::BUCKET_NAME.into(),
                            prefix: prefix.clone(),
                            ..Default::default()
                        },
                        "",
                    )
                    .await;

                match items {
                    Ok(items) => Result::<_, crate::Error>::Ok(items.objects),
                    Err(e) if is_not_found(&e) => Result::<_, crate::Error>::Ok(vec![]),
                    Err(e) => Err(Error::StringError(format!("list {prefix} failed, err: {e:?}"))),
                }
            });
        }

        Ok(try_join_all(futures)
            .await?
            .into_iter()
            .flat_map(|x| x.into_iter())
            .map(|x| x.name)
            .collect())
    }

    async fn load_policy(&self, name: &str) -> crate::Result<PolicyDoc> {
        let (mut policy, object) = self
            .load_iam_config::<PolicyDoc>(&format!("config/iam/policies/{name}/policy.json"))
            .await?;

        if policy.version == 0 {
            policy.create_date = object.mod_time;
            policy.update_date = object.mod_time;
        }

        Ok(policy)
    }

    async fn load_user_identity(&self, user_type: UserType, name: &str) -> crate::Result<Option<UserIdentity>> {
        let (mut user, _) = self
            .load_iam_config::<UserIdentity>(&format!(
                "config/iam/{base}{name}/identity.json",
                base = user_type.prefix(),
                name = name
            ))
            .await?;

        if user.credentials.is_expired() {
            return Ok(None);
        }

        if user.credentials.access_key.is_empty() {
            user.credentials.access_key = name.to_owned();
        }

        // todo, 校验session token

        Ok(Some(user))
    }
}

#[async_trait::async_trait]
impl Store for ObjectStore {
    async fn load_iam_config<Item>(&self, path: impl AsRef<str> + Send) -> crate::Result<(Item, ObjectInfo)>
    where
        Item: DeserializeOwned,
    {
        debug!("load iam config, path: {}", path.as_ref());
        let mut reader = self
            .object_api
            .get_object_reader(
                Self::BUCKET_NAME,
                path.as_ref(),
                HTTPRangeSpec::nil(),
                Default::default(),
                &Default::default(),
            )
            .await
            .map_err(crate::Error::EcstoreError)?;

        let data = reader.read_all().await.map_err(crate::Error::EcstoreError)?;
        // let data = crypto::decrypt_data(&[], &data)?;

        Ok((
            serde_json::from_slice(&data).map_err(|e| crate::Error::StringError(e.to_string()))?,
            reader.object_info,
        ))
    }

    #[tracing::instrument(level = "debug", skip(self, item, path))]
    async fn save_iam_config<Item: Serialize + Send>(&self, item: Item, path: impl AsRef<str> + Send) -> crate::Result<()> {
        let data = serde_json::to_vec(&item).map_err(|e| crate::Error::StringError(e.to_string()))?;
        // let data = crypto::encrypt_data(&[], &data)?;

        self.object_api
            .put_object(
                Self::BUCKET_NAME,
                path.as_ref(),
                &mut PutObjReader::from_vec(data),
                &ObjectOptions {
                    max_parity: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(crate::Error::EcstoreError)?;

        Ok(())
    }

    async fn load_policy_docs(&self) -> crate::Result<HashMap<String, PolicyDoc>> {
        let paths = self.list_iam_config_items("config/iam/", &["policies/"]).await?;

        let mut result = Self::get_default_policyes();
        for path in paths {
            let name = Path::new(&path).iter().rev().nth(0).unwrap();

            let (mut policy_doc, object_info) = self
                .load_iam_config::<PolicyDoc>(format!("config/iam/policies/{}/policy.json", name.to_str().unwrap()))
                .await?;

            if policy_doc.version == 0 {
                policy_doc.create_date = object_info.mod_time;
                policy_doc.update_date = object_info.mod_time;
            }

            result.insert(name.to_str().unwrap().to_owned(), policy_doc);
        }

        Ok(result)
    }

    async fn load_users(&self, user_type: UserType) -> crate::Result<HashMap<String, UserIdentity>> {
        let paths = self.list_iam_config_items("config/iam/", &[user_type.prefix()]).await?;

        let mut result = HashMap::new();
        for path in paths {
            let name = Path::new(&path).iter().rev().nth(0).unwrap();

            let (mut user_identity, _) = self
                .load_iam_config::<UserIdentity>(format!("config/iam/users/{}/identity.json", name.to_str().unwrap()))
                .await?;

            if user_identity.credentials.is_expired() {
                return Err(Error::NoSuchUser(name.to_str().unwrap().to_owned()));
            }

            if user_identity.credentials.access_key.is_empty() {
                user_identity.credentials.access_key = name.to_str().unwrap().to_owned();
            }

            // todo 解析 sts

            result.insert(name.to_str().unwrap().to_owned(), user_identity);
        }

        Ok(result)
    }

    /// load all and make a new cache.
    async fn load_all(&self, cache: &Cache) -> crate::Result<()> {
        let items = self
            .list_iam_config_items(
                "config/iam/",
                &[
                    "policydb/",
                    "policies/",
                    "groups/",
                    "policydb/users/",
                    "policydb/groups/",
                    "service-accounts/",
                    "policydb/sts-users/",
                    "sts/",
                ],
            )
            .await?;
        debug!("all iam items: {items:?}");

        let (policy_docs, users, user_policies, sts_policies, sts_accounts) = (
            Arc::new(tokio::sync::Mutex::new(CacheEntity::new(Self::get_default_policyes()))),
            Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
            Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
            Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
            Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
        );

        // 一次读取32个元素
        let iter = items
            .iter()
            .map(|item| item.trim_start_matches("config/iam/"))
            .map(|item| split_path(item, item.starts_with("policydb/")))
            .filter_map(|(list_key, trimmed_item)| {
                debug!("list_key: {list_key}, trimmed_item: {trimmed_item}");

                if list_key == "format.json" {
                    return None;
                }

                let (policy_docs, users, user_policies, sts_policies, sts_accounts) = (
                    policy_docs.clone(),
                    users.clone(),
                    user_policies.clone(),
                    sts_policies.clone(),
                    sts_accounts.clone(),
                );

                Some(async move {
                    match list_key {
                        "policies/" => {
                            let trimmed_item = dir(trimmed_item);
                            let name = trimmed_item.trim_end_matches('/');
                            let policy_doc = self.load_policy(name).await?;
                            policy_docs.lock().await.insert(name.to_owned(), policy_doc);
                        }
                        "users/" => {
                            let name = dir(trimmed_item);
                            if let Some(user) = self.load_user_identity(UserType::Reg, &name).await? {
                                users.lock().await.insert(name.to_owned(), user);
                            };
                        }
                        "groups/" => {}
                        "policydb/users/" | "policydb/groups/" => {
                            let name = trimmed_item.strip_suffix(".json").unwrap_or(trimmed_item);
                            let mapped_policy = self
                                .load_mapped_policy(UserType::Reg, name, list_key == "policydb/groups/")
                                .await?;
                            if !mapped_policy.policies.is_empty() {
                                user_policies.lock().await.insert(name.to_owned(), mapped_policy);
                            }
                        }
                        "service-accounts/" => {
                            let trimmed_item = dir(trimmed_item);
                            let name = trimmed_item.trim_end_matches('/');
                            let Some(user) = self.load_user_identity(UserType::Svc, name).await? else {
                                return Ok(());
                            };

                            let parent = user.credentials.parent_user.clone();

                            {
                                users.lock().await.insert(name.to_owned(), user);
                            }

                            if users.lock().await.get(&parent).is_some() {
                                return Ok(());
                            }

                            match self.load_mapped_policy(UserType::Sts, parent.as_str(), false).await {
                                Ok(m) => sts_policies.lock().await.insert(name.to_owned(), m),
                                Err(Error::EcstoreError(e)) if is_not_found(&e) => return Ok(()),
                                Err(e) => return Err(e),
                            };
                        }
                        "sts/" => {
                            let name = dir(trimmed_item);
                            if let Some(user) = self.load_user_identity(UserType::Sts, &name).await? {
                                warn!("sts_accounts insert {}, user {:?}", name, &user.credentials.access_key);
                                sts_accounts.lock().await.insert(name.to_owned(), user);
                            };
                        }
                        "policydb/sts-users/" => {
                            let name = trimmed_item.strip_suffix(".json").unwrap_or(trimmed_item);
                            let mapped_policy = self.load_mapped_policy(UserType::Sts, name, false).await?;
                            if !mapped_policy.policies.is_empty() {
                                sts_policies.lock().await.insert(name.to_owned(), mapped_policy);
                            }
                        }
                        _ => {}
                    }

                    crate::Result::Ok(())
                })
            });

        let mut all_futures = Vec::with_capacity(32);

        for f in iter {
            all_futures.push(f);

            if all_futures.len() == 32 {
                try_join_all(all_futures).await?;
                all_futures = Vec::with_capacity(32);
            }
        }

        if !all_futures.is_empty() {
            try_join_all(all_futures).await?;
        }

        if let Some(x) = Arc::into_inner(users) {
            cache.users.store(Arc::new(x.into_inner().update_load_time()))
        }

        if let Some(x) = Arc::into_inner(policy_docs) {
            cache.policy_docs.store(Arc::new(x.into_inner().update_load_time()))
        }
        if let Some(x) = Arc::into_inner(user_policies) {
            cache.user_policies.store(Arc::new(x.into_inner().update_load_time()))
        }
        if let Some(x) = Arc::into_inner(sts_policies) {
            cache.sts_policies.store(Arc::new(x.into_inner().update_load_time()))
        }
        if let Some(x) = Arc::into_inner(sts_accounts) {
            cache.sts_accounts.store(Arc::new(x.into_inner().update_load_time()))
        }

        Ok(())
    }
    async fn load_mapped_policy(&self, user_type: UserType, name: &str, _is_group: bool) -> crate::Result<MappedPolicy> {
        let (p, _) = self
            .load_iam_config::<MappedPolicy>(&format!("{base}{name}.json", base = user_type.prefix(), name = name))
            .await?;

        Ok(p)
    }
}
