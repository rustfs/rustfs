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

use super::replication_error_boundary::Result;
use super::replication_storage_boundary::{HTTPPreconditions, ObjectInfo, ObjectOptions, ReplicationObjectIO};
use crate::config::{com, storageclass};
use std::sync::Arc;

pub(crate) struct ReplicationConfigStore;

impl ReplicationConfigStore {
    pub(crate) const RRS: &'static str = storageclass::RRS;
    pub(crate) const STANDARD: &'static str = storageclass::STANDARD;

    pub(crate) async fn read<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
    where
        S: ReplicationObjectIO,
    {
        com::read_config(api, file).await
    }

    pub(crate) async fn read_limited<S>(api: Arc<S>, file: &str, max_bytes: usize) -> Result<Vec<u8>>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_limited(api, file, max_bytes).await
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
    pub(crate) async fn read_no_lock<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_no_lock(api, file).await
    }

    pub(crate) async fn read_no_lock_with_metadata<S>(api: Arc<S>, file: &str) -> Result<(Vec<u8>, ObjectInfo)>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_with_metadata(
            api,
            file,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    }

    pub(crate) async fn read_no_lock_with_metadata_preserve_empty<S>(api: Arc<S>, file: &str) -> Result<(Vec<u8>, ObjectInfo)>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_no_lock_preserve_empty_with_metadata(api, file).await
    }

    pub(crate) async fn save<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config(api, file, data).await
    }

    pub(crate) async fn save_no_lock<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config_no_lock(api, file, data).await
    }

    pub(crate) async fn save_conditional<S>(
        api: Arc<S>,
        file: &str,
        data: Vec<u8>,
        http_preconditions: HTTPPreconditions,
    ) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config_with_opts_quiet(
            api,
            file,
            data,
            &ObjectOptions {
                max_parity: true,
                http_preconditions: Some(http_preconditions),
                ..Default::default()
            },
        )
        .await
    }

    pub(crate) async fn save_conditional_no_lock<S>(
        api: Arc<S>,
        file: &str,
        data: Vec<u8>,
        http_preconditions: HTTPPreconditions,
    ) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config_with_opts_quiet(
            api,
            file,
            data,
            &ObjectOptions {
                max_parity: true,
                no_lock: true,
                http_preconditions: Some(http_preconditions),
                ..Default::default()
            },
        )
        .await
    }
}
