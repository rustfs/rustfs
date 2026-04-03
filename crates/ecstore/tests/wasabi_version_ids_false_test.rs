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

//! Separate integration test binary so `LazyLock` reads `RUSTFS_WASABI_VERSION_IDS=false`.

use rustfs_ecstore::wasabi_version_ids_enabled;
use temp_env::with_var;

#[test]
fn wasabi_version_ids_respects_false_env() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("false"), || {
        assert!(!wasabi_version_ids_enabled());
    });
}
