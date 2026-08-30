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

use s3s::header::{
    X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_RESTORE,
    X_AMZ_SERVER_SIDE_ENCRYPTION,
};

#[test]
fn persisted_metadata_keys_are_byte_stable() {
    // These HTTP-header constants are also persisted xl.meta map keys. A drift can make an
    // existing WORM retention appear absent or make a restored object's data directory reclaimable.
    assert_eq!(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str(), "x-amz-object-lock-legal-hold");
    assert_eq!(X_AMZ_OBJECT_LOCK_MODE.as_str(), "x-amz-object-lock-mode");
    assert_eq!(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str(), "x-amz-object-lock-retain-until-date");
    assert_eq!(X_AMZ_RESTORE.as_str(), "x-amz-restore");
    assert_eq!(X_AMZ_SERVER_SIDE_ENCRYPTION.as_str(), "x-amz-server-side-encryption");
}
