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

//! Data-key providers: where a data key is wrapped and unwrapped.

pub(crate) mod kms_backed;
pub(crate) mod local;
#[cfg(feature = "m-compatible")]
pub(crate) mod minio;
pub(crate) mod provider;

/// Decrypt a config object that another server stored KMS-encrypted on this
/// drive set, under the local master key and bound to `object_path`.
///
/// `None` when this build reads no such format, when no local master key is
/// configured, or when `blob` does not decrypt under it for that path.
pub fn decrypt_kms_sealed_config(blob: &[u8], object_path: &str) -> Option<Vec<u8>> {
    #[cfg(feature = "m-compatible")]
    {
        minio::decrypt_minio_kms_config(blob, object_path)
    }
    #[cfg(not(feature = "m-compatible"))]
    {
        let _ = (blob, object_path);
        None
    }
}
