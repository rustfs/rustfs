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

use super::replication_storage_boundary::ReplicationObjectIO;
use crate::config::com;
use crate::error::Result;
use std::sync::Arc;

pub(crate) async fn read<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
where
    S: ReplicationObjectIO,
{
    com::read_config(api, file).await
}

pub(crate) async fn save<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
where
    S: ReplicationObjectIO,
{
    com::save_config(api, file, data).await
}
