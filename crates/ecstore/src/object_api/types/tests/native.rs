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

//! Tests of what only a default build compiles.

use super::*;

/// Only a build that reads the sealed layout may reason about its framing.
#[test]
fn get_actual_size_keeps_the_stored_size_of_a_minio_sealed_object_without_the_minio_read_path() {
    let info = ObjectInfo {
        size: 4128,
        user_defined: minio_sealed_object_metadata(false),
        ..Default::default()
    };

    assert_eq!(info.get_actual_size().unwrap(), 4128);
    assert_eq!(info.encryption_original_size().unwrap(), None);
}
