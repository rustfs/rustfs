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

use super::replication_error_boundary::{Error, Result};
use super::replication_storage_boundary::{HTTPPreconditions, ObjectOptions, ReplicationObjectIO};
use crate::config::{com, storageclass};
use rustfs_lock::distributed_lock::LockLostSignal;
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

    pub(crate) async fn read_preserve_empty<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_preserve_empty(api, file).await
    }

    pub(crate) async fn read_no_lock<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
    where
        S: ReplicationObjectIO,
    {
        com::read_config_no_lock(api, file).await
    }

    pub(crate) async fn save<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config(api, file, data).await
    }

    /// Renamed from `read_no_lock` when main's plain `read_no_lock` landed
    /// (#5641): both names existed with different return types.
    pub(crate) async fn read_no_lock_with_etag<S>(api: Arc<S>, file: &str) -> Result<(Vec<u8>, String)>
    where
        S: ReplicationObjectIO,
    {
        let (data, object_info) = com::read_config_no_lock_preserve_empty_with_metadata(api, file).await?;
        let etag = object_info
            .etag
            .filter(|etag| !etag.trim().is_empty())
            .ok_or_else(|| Error::other("persisted MRF file has no ETag"))?;
        Ok((data, etag))
    }

    pub(crate) async fn save_conditional<S>(
        api: Arc<S>,
        file: &str,
        data: Vec<u8>,
        expected_etag: Option<String>,
        lock_lost_signals: Vec<Arc<LockLostSignal>>,
    ) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        let mut opts = ObjectOptions {
            max_parity: true,
            http_preconditions: Some(match expected_etag {
                Some(etag) => HTTPPreconditions {
                    if_match: Some(etag),
                    ..Default::default()
                },
                None => HTTPPreconditions {
                    if_none_match: Some("*".to_string()),
                    ..Default::default()
                },
            }),
            ..Default::default()
        };
        for signal in lock_lost_signals {
            opts.add_namespace_lock_lost_signal(signal);
        }
        com::save_config_with_opts(api, file, data, &opts).await
    }

    pub(crate) async fn save_no_lock<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        com::save_config_no_lock(api, file, data).await
    }
}
