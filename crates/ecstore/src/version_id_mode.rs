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

//! Process-wide toggle for Wasabi-compatible version ids vs legacy UUID behavior.

use crate::error::{Result, StorageError};
use http::HeaderMap;
use rand::RngExt;
use rustfs_filemeta::S3VersionId;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

static WASABI_VERSION_IDS_ENABLED: LazyLock<bool> =
    LazyLock::new(|| rustfs_utils::get_env_bool(rustfs_config::ENV_WASABI_VERSION_IDS, true));

/// HTTP header used by replication/sync to pin the S3 `VersionId` on PUT (Wasabi compatibility).
///
/// HTTP header names are case-insensitive; this is the canonical lowercase form for lookups.
pub const WASABI_SET_VERSION_ID_HEADER: &str = "x-wasabi-set-version-id";

/// Returns whether Wasabi-compatible version id mode is enabled for this process.
///
/// The value is read once from [`rustfs_config::ENV_WASABI_VERSION_IDS`] (default `true`).
#[inline]
pub fn wasabi_version_ids_enabled() -> bool {
    *WASABI_VERSION_IDS_ENABLED
}

/// Writes Wasabi `createVersionId` time prefix into `out[..22]`: 21 zero-padded decimal digits (`%021d`)
/// then ASCII `-`, matching Go `fmt.Sprintf("%021d-", nanos)`.
#[inline]
fn write_wasabi_version_id_time_prefix(out: &mut [u8; 32], nanos: u64) {
    let mut n = nanos;
    for i in (0..21).rev() {
        out[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out[21] = b'-';
}

/// Mints a new S3 version id for a **new versioned** object when the server assigns the id.
///
/// - [`wasabi_version_ids_enabled`] **off**: random UUID (legacy RustFS).
/// - **on**: Wasabi `createVersionId` shape (`%021d-` nanoseconds + 10 chars from Wasabi alphabet).
#[inline]
pub fn mint_new_object_version_id() -> S3VersionId {
    if !wasabi_version_ids_enabled() {
        return S3VersionId::Uuid(Uuid::new_v4());
    }
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let mut a = [0u8; 32];
    write_wasabi_version_id_time_prefix(&mut a, nanos);
    let alphabet = rustfs_filemeta::WASABI_VERSION_ID_RANDOM_ALPHABET;
    let mut rng = rand::rng();
    for j in 0..10 {
        let i = rng.random_range(0..alphabet.len());
        a[22 + j] = alphabet[i];
    }
    debug_assert!(std::str::from_utf8(&a).is_ok());
    debug_assert_eq!(a.len(), 32);
    S3VersionId::WasabiAscii(a)
}

/// When Wasabi version-id mode is **off**, rejects any non-empty `X-Wasabi-Set-Version-Id` header.
///
/// When mode is **on**, this is a no-op (strict validation of the value is implemented elsewhere).
pub fn ensure_wasabi_set_version_id_header_allowed(headers: &HeaderMap, bucket: &str, object: &str) -> Result<()> {
    if wasabi_version_ids_enabled() {
        return Ok(());
    }
    let Some(raw) = headers.get(WASABI_SET_VERSION_ID_HEADER) else {
        return Ok(());
    };
    let s = raw.to_str().map_err(|_| {
        StorageError::InvalidArgument(
            bucket.to_owned(),
            object.to_owned(),
            "X-Wasabi-Set-Version-Id: invalid header value encoding".to_owned(),
        )
    })?;
    if s.trim().is_empty() {
        return Ok(());
    }
    Err(StorageError::InvalidArgument(
        bucket.to_owned(),
        object.to_owned(),
        "X-Wasabi-Set-Version-Id is not supported when RUSTFS_WASABI_VERSION_IDS is false".to_owned(),
    ))
}

#[cfg(test)]
mod wasabi_version_id_prefix_tests {
    use super::write_wasabi_version_id_time_prefix;

    #[test]
    fn time_prefix_matches_go_style_zero_padded_21_digits() {
        for nanos in [0u64, 1, 42, 1_234_567_890_123_456_789, u64::MAX] {
            let mut buf = [0u8; 32];
            write_wasabi_version_id_time_prefix(&mut buf, nanos);
            let expected = format!("{nanos:021}-");
            assert_eq!(&buf[..22], expected.as_bytes(), "nanos={nanos}");
        }
    }
}
