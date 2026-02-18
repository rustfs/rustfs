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

use rustfs_ecstore::error::Result;
use std::path::Path;
use surrealkv::{Tree, TreeBuilder};

pub async fn new_kv_store(path: impl AsRef<Path>) -> Result<Tree> {
    let tree = TreeBuilder::new()
        .with_path(path.as_ref().to_path_buf())
        .build()
        .map_err(|e| rustfs_ecstore::error::Error::other(e.to_string()))?;
    Ok(tree)
}
