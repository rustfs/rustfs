pub mod object;

use std::collections::HashMap;

use ecstore::store_api::ObjectInfo;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    auth::UserIdentity,
    cache::Cache,
    policy::{MappedPolicy, PolicyDoc, UserType, DEFAULT_POLICIES},
};

#[async_trait::async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    async fn load_iam_config<Item>(&self, path: impl AsRef<str> + Send) -> crate::Result<(Item, ObjectInfo)>
    where
        Item: DeserializeOwned;

    async fn save_iam_config<Item: Serialize + Send>(&self, item: Item, path: impl AsRef<str> + Send) -> crate::Result<()>;
    async fn delete_iam_config(&self, path: impl AsRef<str> + Send) -> crate::Result<()>;

    async fn load_all(&self, cache: &Cache) -> crate::Result<()>;

    fn get_default_policyes() -> HashMap<String, PolicyDoc> {
        let default_policies = DEFAULT_POLICIES;
        default_policies
            .iter()
            .map(|(n, p)| {
                (
                    n.to_string(),
                    PolicyDoc {
                        version: 1,
                        policy: p.clone(),
                        ..Default::default()
                    },
                )
            })
            .collect()
    }

    async fn load_users(&self, user_type: UserType) -> crate::Result<HashMap<String, UserIdentity>>;

    async fn load_policy_docs(&self) -> crate::Result<HashMap<String, PolicyDoc>>;
    async fn load_mapped_policy(&self, user_type: UserType, name: &str, is_group: bool) -> crate::Result<MappedPolicy>;
}
