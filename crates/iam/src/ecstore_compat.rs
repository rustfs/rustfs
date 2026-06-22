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

pub(crate) use rustfs_ecstore::api::config::RUSTFS_CONFIG_PREFIX as ECSTORE_RUSTFS_CONFIG_PREFIX;
pub(crate) use rustfs_ecstore::api::config::com::{
    delete_config as ecstore_delete_config, read_config_no_lock as ecstore_read_config_no_lock,
    read_config_with_metadata as ecstore_read_config_with_metadata, save_config as ecstore_save_config,
    save_config_with_opts as ecstore_save_config_with_opts,
};
pub(crate) use rustfs_ecstore::api::error::{
    Error as EcstoreErrorType, Result as EcstoreResultType, StorageError as EcstoreStorageError,
    classify_system_path_failure_reason as ecstore_classify_system_path_failure_reason,
};
pub(crate) use rustfs_ecstore::api::global::is_first_cluster_node_local as ecstore_is_first_cluster_node_local;
pub(crate) use rustfs_ecstore::api::notification::{
    NotificationPeerErr as EcstoreNotificationPeerErr, get_global_notification_sys as ecstore_get_global_notification_sys,
};
pub(crate) use rustfs_ecstore::api::storage::ECStore as EcstoreStore;
