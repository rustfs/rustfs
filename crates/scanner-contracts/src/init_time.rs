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

use chrono::{DateTime, Utc};
use std::sync::LazyLock;
use tokio::sync::RwLock;

/// Global initialization time of the RustFS node.
pub static GLOBAL_INIT_TIME: LazyLock<RwLock<Option<DateTime<Utc>>>> = LazyLock::new(|| RwLock::new(None));

/// Set the global RustFS initialization time to the current UTC time.
pub async fn set_global_init_time_now() {
    let now = Utc::now();
    *GLOBAL_INIT_TIME.write().await = Some(now);
}

/// Get the global RustFS initialization time.
///
/// # Returns
/// * `Option<DateTime<Utc>>` - The initialization time if set.
pub async fn get_global_init_time() -> Option<DateTime<Utc>> {
    *GLOBAL_INIT_TIME.read().await
}
