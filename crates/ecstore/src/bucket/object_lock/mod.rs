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

pub mod objectlock;
pub mod objectlock_sys;
pub mod types;

/// Whether a bucket Object Lock configuration has locking enabled. The
/// serving-layer `ObjectLockConfiguration` DTO implements this in
/// the bucket-metadata module, which owns the persisted configuration type
/// during the s3s ratchet migration (rustfs/backlog#1842).
pub trait ObjectLockApi {
    fn enabled(&self) -> bool;
}

/// Whether a legal-hold status value is one of the two valid wire values.
/// Implemented for the serving-layer DTO in the bucket-metadata module.
pub trait ObjectLockStatusExt {
    fn valid(&self) -> bool;
}
