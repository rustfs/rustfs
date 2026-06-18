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

use std::sync::Arc;

pub(crate) const IAM_CONFIG_ROOT_PREFIX: &str = rustfs_ecstore::config::RUSTFS_CONFIG_PREFIX;

pub(crate) type IamEcstoreError = rustfs_ecstore::error::Error;
pub(crate) type IamConfigObjectInfo = rustfs_ecstore::store_api::ObjectInfo;
pub(crate) type IamConfigObjectOptions = rustfs_ecstore::store_api::ObjectOptions;
pub(crate) type IamStorageError = rustfs_ecstore::error::StorageError;
pub(crate) type IamStorageResult<T> = rustfs_ecstore::error::Result<T>;
pub(crate) type IamStore = rustfs_ecstore::store::ECStore;

pub(crate) async fn read_iam_config_no_lock(api: Arc<IamStore>, file: &str) -> IamStorageResult<Vec<u8>> {
    rustfs_ecstore::config::com::read_config_no_lock(api, file).await
}

pub(crate) async fn read_iam_config_with_metadata(
    api: Arc<IamStore>,
    file: &str,
    opts: &IamConfigObjectOptions,
) -> IamStorageResult<(Vec<u8>, IamConfigObjectInfo)> {
    rustfs_ecstore::config::com::read_config_with_metadata(api, file, opts).await
}

pub(crate) async fn save_iam_config(api: Arc<IamStore>, file: &str, data: Vec<u8>) -> IamStorageResult<()> {
    rustfs_ecstore::config::com::save_config(api, file, data).await
}

pub(crate) async fn save_iam_config_with_opts(
    api: Arc<IamStore>,
    file: &str,
    data: Vec<u8>,
    opts: &IamConfigObjectOptions,
) -> IamStorageResult<()> {
    rustfs_ecstore::config::com::save_config_with_opts(api, file, data, opts).await
}

pub(crate) async fn delete_iam_config(api: Arc<IamStore>, file: &str) -> IamStorageResult<()> {
    rustfs_ecstore::config::com::delete_config(api, file).await
}

pub(crate) fn classify_iam_system_path_failure_reason(err: &IamEcstoreError) -> &'static str {
    rustfs_ecstore::error::classify_system_path_failure_reason(err)
}

pub(crate) async fn is_iam_first_cluster_node_local() -> bool {
    rustfs_ecstore::global::is_first_cluster_node_local().await
}

pub(crate) struct IamNotificationPeerErr {
    pub(crate) err: Option<IamEcstoreError>,
}

impl From<rustfs_ecstore::notification_sys::NotificationPeerErr> for IamNotificationPeerErr {
    fn from(value: rustfs_ecstore::notification_sys::NotificationPeerErr) -> Self {
        Self { err: value.err }
    }
}

pub(crate) async fn notify_iam_delete_policy(policy_name: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .delete_policy(policy_name)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_policy(policy_name: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .load_policy(policy_name)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_delete_user(access_key: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .delete_user(access_key)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_user(access_key: &str, temp: bool) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .load_user(access_key, temp)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_service_account(access_key: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .load_service_account(access_key)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_delete_service_account(access_key: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .delete_service_account(access_key)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_group(group: &str) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys.load_group(group).await.into_iter().map(Into::into).collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_policy_mapping(
    user_or_group: &str,
    user_type: u64,
    is_group: bool,
) -> Vec<IamNotificationPeerErr> {
    match rustfs_ecstore::notification_sys::get_global_notification_sys() {
        Some(notification_sys) => notification_sys
            .load_policy_mapping(user_or_group, user_type, is_group)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}
