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

pub(crate) use rustfs_ecstore::api::config::RUSTFS_CONFIG_PREFIX as IAM_CONFIG_ROOT_PREFIX;
use rustfs_ecstore::api::config::com::{
    delete_config as ecstore_delete_config, read_config_no_lock as ecstore_read_config_no_lock,
    read_config_with_metadata as ecstore_read_config_with_metadata, save_config as ecstore_save_config,
    save_config_with_opts as ecstore_save_config_with_opts,
};
use rustfs_ecstore::api::error::{
    Error as EcstoreErrorType, Result as EcstoreResultType, StorageError as EcstoreStorageError,
    classify_system_path_failure_reason as ecstore_classify_system_path_failure_reason,
};
use rustfs_ecstore::api::notification::{
    NotificationPeerErr as EcstoreNotificationPeerErr, NotificationSys as EcstoreNotificationSys, get_global_notification_sys,
};
use rustfs_ecstore::api::runtime::first_cluster_node_is_local as ecstore_first_cluster_node_is_local;
use rustfs_ecstore::api::storage::ECStore as EcstoreStore;
use rustfs_storage_api as storage_contracts;

pub(crate) type IamEcstoreError = EcstoreErrorType;
pub(crate) type IamStorageError = EcstoreStorageError;
pub(crate) type IamStorageResult<T> = EcstoreResultType<T>;
pub(crate) type IamStore = EcstoreStore;
pub(crate) type IamConfigObjectInfo = <IamStore as storage_contracts::ObjectOperations>::ObjectInfo;
pub(crate) type IamConfigObjectOptions = <IamStore as storage_contracts::ObjectOperations>::ObjectOptions;
pub(crate) type IamNotificationSys = EcstoreNotificationSys;
pub(crate) type IamEcstoreNotificationPeerErr = EcstoreNotificationPeerErr;

pub(crate) async fn read_iam_config_no_lock(api: Arc<IamStore>, file: &str) -> IamStorageResult<Vec<u8>> {
    ecstore_read_config_no_lock(api, file).await
}

pub(crate) async fn read_iam_config_with_metadata(
    api: Arc<IamStore>,
    file: &str,
    opts: &IamConfigObjectOptions,
) -> IamStorageResult<(Vec<u8>, IamConfigObjectInfo)> {
    ecstore_read_config_with_metadata(api, file, opts).await
}

pub(crate) async fn save_iam_config(api: Arc<IamStore>, file: &str, data: Vec<u8>) -> IamStorageResult<()> {
    ecstore_save_config(api, file, data).await
}

pub(crate) async fn save_iam_config_with_opts(
    api: Arc<IamStore>,
    file: &str,
    data: Vec<u8>,
    opts: &IamConfigObjectOptions,
) -> IamStorageResult<()> {
    ecstore_save_config_with_opts(api, file, data, opts).await
}

pub(crate) async fn delete_iam_config(api: Arc<IamStore>, file: &str) -> IamStorageResult<()> {
    ecstore_delete_config(api, file).await
}

pub(crate) fn classify_iam_system_path_failure_reason(err: &IamEcstoreError) -> &'static str {
    ecstore_classify_system_path_failure_reason(err)
}

pub(crate) async fn is_iam_first_cluster_node_local() -> bool {
    ecstore_first_cluster_node_is_local().await
}

pub(crate) fn notification_sys() -> Option<&'static IamNotificationSys> {
    get_global_notification_sys()
}

pub(crate) mod crate_boundary {
    pub(crate) use super::{
        IAM_CONFIG_ROOT_PREFIX, IamEcstoreError, IamEcstoreNotificationPeerErr, IamStorageError, IamStore,
        classify_iam_system_path_failure_reason, delete_iam_config, is_iam_first_cluster_node_local, read_iam_config_no_lock,
        read_iam_config_with_metadata, save_iam_config, save_iam_config_with_opts,
    };
}

pub(crate) mod object_store {
    pub(crate) use super::storage_contracts::{HTTPPreconditions, ListOperations, ObjectInfoOrErr, ObjectOperations};
}

pub(crate) mod runtime {
    pub(crate) use super::{IamNotificationSys, notification_sys};
}
