// Copyright 2026 RustFS Team
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

use crate::disk::{
    RUSTFS_META_BUCKET,
    error::{DiskError, Result},
};
use uuid::Uuid;

const NATIVE_STREAM_PREFIX: &str = "native-preparations/";

/// Recognize only the reserved per-preparation shard path used by the native
/// stream writer. Malformed paths under that namespace must not fall through to
/// the general disk writer.
pub(crate) fn is_native_stream_path(volume: &str, path: &str) -> Result<bool> {
    if volume != RUSTFS_META_BUCKET || !path.starts_with(NATIVE_STREAM_PREFIX) {
        return Ok(false);
    }

    let mut components = path.split('/');
    if components.next() != Some("native-preparations") {
        return Err(DiskError::InvalidPath);
    }
    let preparation = components
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or(DiskError::InvalidPath)?;
    let data_dir = components
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or(DiskError::InvalidPath)?;
    let part_name = components.next().ok_or(DiskError::InvalidPath)?;
    let part = part_name
        .strip_prefix("part.")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .ok_or(DiskError::InvalidPath)?;

    if components.next().is_some() || path != format!("native-preparations/{preparation}/{data_dir}/part.{part}") {
        return Err(DiskError::InvalidPath);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_stream_path_requires_canonical_identity_and_part() {
        let preparation = Uuid::from_u128(1);
        let data_dir = Uuid::from_u128(2);
        let valid = format!("native-preparations/{preparation}/{data_dir}/part.1");
        assert!(is_native_stream_path(RUSTFS_META_BUCKET, &valid).expect("valid path"));
        assert!(!is_native_stream_path("bucket", &valid).expect("other volume"));
        assert!(!is_native_stream_path(RUSTFS_META_BUCKET, "ordinary/object").expect("ordinary path"));
        for invalid in [
            "native-preparations/00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000002/part.1".to_owned(),
            format!("native-preparations/{preparation}/{data_dir}/part.0"),
            format!("native-preparations/{preparation}/{data_dir}/part.01"),
            format!("native-preparations/{preparation}/{data_dir}/part.1/extra"),
            format!("native-preparations/{preparation}/../part.1"),
        ] {
            assert!(matches!(is_native_stream_path(RUSTFS_META_BUCKET, &invalid), Err(DiskError::InvalidPath)));
        }
    }
}
