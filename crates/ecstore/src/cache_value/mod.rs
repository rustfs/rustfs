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

use lazy_static::lazy_static;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub mod metacache_set;

lazy_static! {
    pub static ref LIST_PATH_RAW_CANCEL_TOKEN: Arc<CancellationToken> = Arc::new(CancellationToken::new());
}
